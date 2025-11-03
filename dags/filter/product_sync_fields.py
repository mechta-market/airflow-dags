import logging
from datetime import datetime, timedelta
from typing import List, Any
import time

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from elasticsearch import helpers

from helpers.utils import (
    elastic_conn,
    request_to_onec_proxy,
    put_to_s3,
    get_from_s3,
    SHOP_DEFAULT_DB_NAME,
)

# Constants
DAG_ID = "product_sync_fields"
default_args = {
    "owner": "Ilya",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

INDEX_NAME = "product_v2"
INDEX_NAME_MEASUREMENT_UNITS = "dict_measurement_units_v1"
INDEX_NAME_VAT_RATE = "dict_vat_rate_v1"

S3_FILE_NAME_PRODUCT_MAPPINGS = f"{DAG_ID}/product_mappings.json"
S3_FILE_NAME_MEASUREMENT_UNITS = f"{DAG_ID}/product_measurement_units.json"
S3_FILE_NAME_VAT_RATE = f"{DAG_ID}/product_vat_rate.json"

BATCH_SIZE = 250


def fetch_measurement_units(**context) -> None:
    logger = logging.getLogger(__name__)
    task_instance = context["ti"]

    try:
        logger.info("Starting measurement units fetch")

        response = request_to_onec_proxy(body={
            "method": "GET",
            "path": "/getbaseinfo/product_measurement_unit_classifier",
            "node": {"name": SHOP_DEFAULT_DB_NAME},
            "source": "mdm"
        })

        data = response.get("data", [])

        if not data:
            logger.warning("No measurement units data received from 1C API")
            return

        valid_data = [item for item in data if item.get("id") is not None]
        if len(valid_data) != len(data):
            logger.warning(f"Filtered out {len(data) - len(valid_data)} items without ID")

        put_to_s3(data=valid_data, s3_key=S3_FILE_NAME_MEASUREMENT_UNITS)
        task_instance.xcom_push(key="measurement_units_count", value=len(valid_data))

        logger.info(f"Successfully fetched {len(valid_data)} measurement units")

    except Exception as e:
        logger.error(f"Failed to fetch measurement units: {str(e)}")
        raise


def fetch_vat_rate(**context) -> None:
    logger = logging.getLogger(__name__)
    task_instance = context["ti"]

    try:
        logger.info("Starting VAT rate fetch")

        response = request_to_onec_proxy(body={
            "method": "GET",
            "path": "/getbaseinfo/vat_rate",
            "node": {"name": SHOP_DEFAULT_DB_NAME},
            "source": "mdm"
        })

        data = response.get("data", [])

        if not data:
            logger.warning("No VAT rate data received from 1C API")
            return

        vat_dict = {}
        for item in data:
            item_id = item.get("id")
            rate = item.get("rate")
            if item_id is not None and rate is not None:
                vat_dict[item_id] = rate
            else:
                logger.warning(f"Skipping VAT item with missing id or rate: {item}")

        put_to_s3(data=data, s3_key=S3_FILE_NAME_VAT_RATE)
        task_instance.xcom_push(key="vat_rate_mapping", value=vat_dict)
        task_instance.xcom_push(key="vat_rate_count", value=len(vat_dict))

        logger.info(f"Successfully fetched {len(vat_dict)} VAT rates")

    except Exception as e:
        logger.error(f"Failed to fetch VAT rates: {str(e)}")
        raise


def fetch_products_and_create_mappings(**context) -> None:
    logger = logging.getLogger(__name__)
    task_instance = context["ti"]

    try:
        logger.info("Starting products fetch and mapping")

        vat_dict = task_instance.xcom_pull(
            task_ids="fetch_vat_rate_task",
            key="vat_rate_mapping"
        ) or {}

        response = request_to_onec_proxy(body={
            "method": "POST",
            "path": "/getbaseinfo/product",
            "node": {"name": SHOP_DEFAULT_DB_NAME},
            "source": "mdm"
        })

        products = response.get("data", [])

        if not products:
            logger.warning("No product data received from 1C API")
            return

        product_mappings = []
        skipped_products = 0

        for product in products:
            product_id = product.get("id")
            if not product_id:
                skipped_products += 1
                continue

            mapping = {
                "id": product_id,
                "okei_code": product.get("okei_code", ""),
                "vat_rate": vat_dict.get(product.get("vat_rate", ""), ""),
            }
            product_mappings.append(mapping)

        if skipped_products > 0:
            logger.warning(f"Skipped {skipped_products} products without ID")

        put_to_s3(data=product_mappings, s3_key=S3_FILE_NAME_PRODUCT_MAPPINGS)
        task_instance.xcom_push(key="products_count", value=len(product_mappings))

        logger.info(f"Successfully created mappings for {len(product_mappings)} products")

    except Exception as e:
        logger.error(f"Failed to fetch products and create mappings: {str(e)}")
        raise


def check_existing_documents(client: Any, index_name: str, document_ids: List[str]) -> set:
    logger = logging.getLogger(__name__)

    existing_ids = set()
    batch_size = 500

    for i in range(0, len(document_ids), batch_size):
        batch_ids = document_ids[i:i + batch_size]

        try:
            response = client.mget(
                index=index_name,
                body={"ids": batch_ids},
                _source=False
            )

            for doc in response.get("docs", []):
                if doc.get("found", False):
                    existing_ids.add(doc["_id"])

            logger.info(
                f"Checked batch {i // batch_size + 1}/{(len(document_ids) - 1) // batch_size + 1}: {len(existing_ids)} existing so far")

        except Exception as e:
            logger.error(f"Failed to check document existence for batch: {str(e)}")

    logger.info(f"Found {len(existing_ids)} existing documents out of {len(document_ids)} total")
    return existing_ids


def upsert_to_es(**context) -> None:
    """Upsert dictionaries (measurement units and VAT rates) - allow creating new documents."""
    logger = logging.getLogger(__name__)
    task_instance = context["ti"]

    try:
        logger.info("Starting dictionary upsert process (allow create new documents)")

        client = elastic_conn(Variable.get("elastic_scheme"))

        if not hasattr(client, "bulk"):
            raise RuntimeError("elastic_conn did not return a valid Elasticsearch client")

        client.info()
        logger.info("Elasticsearch connection established")

        results = {}

        logger.info("Processing measurement units (allow create new)...")
        try:
            measurement_units_data = get_from_s3(s3_key=S3_FILE_NAME_MEASUREMENT_UNITS) or []

            if measurement_units_data:
                actions = []
                for item in measurement_units_data:
                    if item.get("id"):
                        actions.append({
                            "_op_type": "update",
                            "_index": INDEX_NAME_MEASUREMENT_UNITS,
                            "_id": item.get("id"),
                            "doc": item,
                            "doc_as_upsert": True,
                            "retry_on_conflict": 3,
                        })

                if actions:
                    success, errors = helpers.bulk(
                        client,
                        actions,
                        refresh=False,
                        stats_only=False,
                        request_timeout=60
                    )
                    results["measurement_units"] = {
                        "total": len(actions),
                        "success": success,
                        "errors": errors
                    }
                    logger.info(f"Measurement units: {success} documents processed")
                else:
                    results["measurement_units"] = {"total": 0, "success": 0, "errors": []}
                    logger.info("No measurement units to process")
            else:
                results["measurement_units"] = {"total": 0, "success": 0, "errors": []}
                logger.info("No measurement units data found")

        except Exception as e:
            logger.error(f"Failed to process measurement units: {str(e)}")
            results["measurement_units"] = {"total": 0, "success": 0, "errors": [str(e)]}

        logger.info("Processing VAT rates (allow create new)...")
        try:
            vat_data = get_from_s3(s3_key=S3_FILE_NAME_VAT_RATE) or []

            if vat_data:
                actions = []
                for item in vat_data:
                    if item.get("id"):
                        actions.append({
                            "_op_type": "update",
                            "_index": INDEX_NAME_VAT_RATE,
                            "_id": item.get("id"),
                            "doc": item,
                            "doc_as_upsert": True,
                            "retry_on_conflict": 3,
                        })

                if actions:
                    success, errors = helpers.bulk(
                        client,
                        actions,
                        refresh=False,
                        stats_only=False,
                        request_timeout=60
                    )
                    results["vat_rates"] = {
                        "total": len(actions),
                        "success": success,
                        "errors": errors
                    }
                    logger.info(f"VAT rates: {success} documents processed")
                else:
                    results["vat_rates"] = {"total": 0, "success": 0, "errors": []}
                    logger.info("No VAT rates to process")
            else:
                results["vat_rates"] = {"total": 0, "success": 0, "errors": []}
                logger.info("No VAT rates data found")

        except Exception as e:
            logger.error(f"Failed to process VAT rates: {str(e)}")
            results["vat_rates"] = {"total": 0, "success": 0, "errors": [str(e)]}

        client.indices.refresh(index=INDEX_NAME_MEASUREMENT_UNITS)
        client.indices.refresh(index=INDEX_NAME_VAT_RATE)

        total_success = (
                results["measurement_units"]["success"] +
                results["vat_rates"]["success"]
        )
        total_errors = (
                len(results["measurement_units"]["errors"]) +
                len(results["vat_rates"]["errors"])
        )

        logger.info(f"Dictionary upsert completed: {total_success} successful, {total_errors} errors")

        task_instance.xcom_push(key="dictionary_upsert_results", value=results)

    except Exception as e:
        logger.error(f"Dictionary upsert process failed: {str(e)}")
        raise


def update_in_es(**context) -> None:
    """Update ONLY existing products in product_v2 index - do NOT create new documents."""
    logger = logging.getLogger(__name__)
    task_instance = context["ti"]

    try:
        logger.info("Starting product update process (existing documents only)")

        client = elastic_conn(Variable.get("elastic_scheme"))

        if not hasattr(client, "bulk"):
            raise RuntimeError("elastic_conn did not return a valid Elasticsearch client")

        client.info()
        logger.info("Elasticsearch connection established")

        product_mappings = get_from_s3(s3_key=S3_FILE_NAME_PRODUCT_MAPPINGS) or []

        if not product_mappings:
            logger.warning("No product mappings found to update")
            task_instance.xcom_push(key="product_update_results", value={
                "total": 0,
                "success": 0,
                "errors": [],
                "existing_count": 0
            })
            return

        mapping_ids = [item.get("id") for item in product_mappings if item.get("id")]
        logger.info(f"Checking existence for {len(mapping_ids)} product IDs")

        existing_ids = check_existing_documents(client, INDEX_NAME, mapping_ids)

        if not existing_ids:
            logger.warning("No existing products found to update")
            task_instance.xcom_push(key="product_update_results", value={
                "total": len(mapping_ids),
                "success": 0,
                "errors": [],
                "existing_count": 0
            })
            return

        actions = []
        for mapping in product_mappings:
            product_id = mapping.get("id")
            if product_id and product_id in existing_ids:
                actions.append({
                    "_op_type": "update",
                    "_index": INDEX_NAME,
                    "_id": product_id,
                    "doc": {
                        "okei_code": mapping.get("okei_code", ""),
                        "vat_rate": mapping.get("vat_rate", ""),
                    },
                    "doc_as_upsert": False,
                    "retry_on_conflict": 3,
                })

        logger.info(f"Preparing to update {len(actions)} existing products")

        total_success = 0
        all_errors = []

        for i in range(0, len(actions), BATCH_SIZE):
            batch = actions[i:i + BATCH_SIZE]

            try:
                success, errors = helpers.bulk(
                    client,
                    batch,
                    refresh=False,
                    stats_only=False,
                    request_timeout=60
                )

                total_success += success
                logger.info(f"Product batch {i // BATCH_SIZE + 1}: {success} documents updated")

                if errors:
                    logger.error(f"Product batch {i // BATCH_SIZE + 1} had {len(errors)} errors")
                    all_errors.extend(errors)

            except Exception as bulk_error:
                logger.error(f"Bulk operation failed for product batch {i // BATCH_SIZE + 1}: {str(bulk_error)}")
                all_errors.append({"batch": i // BATCH_SIZE + 1, "error": str(bulk_error)})

            time.sleep(0.1)

        client.indices.refresh(index=INDEX_NAME)

        results = {
            "total": len(mapping_ids),
            "success": total_success,
            "errors": all_errors,
            "existing_count": len(existing_ids)
        }

        logger.info(
            f"Product update completed: {total_success}/{len(existing_ids)} existing products updated, {len(all_errors)} errors")

        task_instance.xcom_push(key="product_update_results", value=results)

    except Exception as e:
        logger.error(f"Product update process failed: {str(e)}")
        raise


with DAG(
        dag_id=DAG_ID,
        default_args=default_args,
        schedule="*/30 * * * *",
        start_date=datetime(2025, 10, 31),
        catchup=False,
        max_active_runs=1,
        tags=["1c", "elasticsearch", "product", "sync", "measurement_units", "vat_rate"],
        doc_md=__doc__,
) as dag:
    fetch_units = PythonOperator(
        task_id="fetch_measurement_units_task",
        python_callable=fetch_measurement_units,
    )

    fetch_vat = PythonOperator(
        task_id="fetch_vat_rate_task",
        python_callable=fetch_vat_rate,
    )

    fetch_products = PythonOperator(
        task_id="fetch_products_task",
        python_callable=fetch_products_and_create_mappings,
    )

    sync_dictionaries = PythonOperator(
        task_id="sync_dictionaries_task",
        python_callable=upsert_to_es,
    )

    update_products = PythonOperator(
        task_id="update_existing_products_task",
        python_callable=update_in_es,
    )

    [fetch_units, fetch_vat] >> fetch_products >> [sync_dictionaries, update_products]