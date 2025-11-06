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
    ZERO_UUID,
)

# Constants
default_args = {
    "owner": "Ilya",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

DAG_ID = "product_shop_fields"

INDEX_NAME = "product_v2"
INDEX_NAME_VAT_RATE = "product_vat"

S3_FILE_NAME = f"{DAG_ID}/product_mappings.json"

BATCH_SIZE = 250


def fetch_products_and_create_mappings() -> None:
    try:
        logging.info("Starting products fetch and mapping")
        client = elastic_conn(Variable.get("elastic_scheme"))

        vat_dict = fetch_vat_rates(client)

        if not vat_dict:
            logging.error("No VAT rates fetched - this is required data")
            raise Exception("Cannot proceed without VAT rates data")

        response = request_to_onec_proxy(body={
            "method": "POST",
            "path": "/getbaseinfo/product",
            "node": {"name": SHOP_DEFAULT_DB_NAME}
        })

        if not response.get("success", False):
            logging.error(f"Failed to fetch measurement units, success: {response.get('success', False)}")
            raise

        products = response.get("data", [])

        if not products:
            logging.error("No product data received from 1C API")
            raise

        product_mappings = []
        skipped_products = 0

        for product in products:
            product_id = product.get("id")
            if not product_id or product_id == ZERO_UUID:
                skipped_products += 1
                continue

            product_vat_id = product.get("vat_rate")
            vat_rate_value = None
            if product_vat_id and product_vat_id != ZERO_UUID:
                vat_rate_value = vat_dict.get(str(product_vat_id), None)

            mapping = {
                "id": product_id,
                "okei_code": product.get("okei_code", ""),
                "vat_rate": vat_rate_value,
            }
            product_mappings.append(mapping)

        if skipped_products > 0:
            logging.warning(f"Skipped {skipped_products} products without ID")

        put_to_s3(data=product_mappings, s3_key=S3_FILE_NAME)

        logging.info(f"Successfully created mappings for {len(product_mappings)} products")
        logging.info(
            f"VAT rate mapping coverage: {len([m for m in product_mappings if m['vat_rate']])}/{len(product_mappings)} products have VAT rates")

    except Exception as e:
        logging.error(f"Failed to fetch products and create mappings: {str(e)}")
        raise


def fetch_vat_rates(client) -> dict:
    vat_dict = {}
    scroll_id = None

    try:
        if not client.indices.exists(index=INDEX_NAME_VAT_RATE):
            logging.error(f"Index {INDEX_NAME_VAT_RATE} does not exist")
            raise Exception(f"Required index {INDEX_NAME_VAT_RATE} not found")

        query = {"query": {"match_all": {}}, "size": 100}
        response = client.search(index=INDEX_NAME_VAT_RATE, body=query, scroll="2m")
        scroll_id = response.get("_scroll_id")
        hits = response.get("hits", {}).get("hits", [])

        if not hits:
            logging.warning(f"Index {INDEX_NAME_VAT_RATE} exists but contains no VAT rates")
            raise Exception("No VAT rates found in index")

        while hits:
            for hit in hits:
                vat_id = hit.get("_id")
                vat_src = hit.get("_source", {})
                if vat_src:
                    vat_dict[str(vat_id)] = vat_src

            response = client.scroll(scroll_id=scroll_id, scroll="2m")
            scroll_id = response.get("_scroll_id")
            hits = response.get("hits", {}).get("hits", [])

        logging.info(f"Loaded {len(vat_dict)} VAT rates from Elasticsearch")

    except Exception as e:
        logging.error(f"Failed to fetch VAT rates from Elasticsearch: {str(e)}")
        raise

    finally:
        if scroll_id:
            try:
                client.clear_scroll(scroll_id=scroll_id)
            except Exception as e:
                logging.debug(f"Scroll cleanup failed: {str(e)}")

    return vat_dict

def check_existing_documents(client: Any, index_name: str, document_ids: List[str]) -> set:
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

            if (i // batch_size) % 10 == 0:
                logging.info(
                    f"Checked batch {i // batch_size + 1}/{(len(document_ids) - 1) // batch_size + 1}: {len(existing_ids)} existing so far")


        except Exception as e:
            logging.error(f"Failed to check document existence for batch: {str(e)}")

    logging.info(f"Found {len(existing_ids)} existing documents out of {len(document_ids)} total")
    return existing_ids

def update_in_es_callable(**context) -> None:
    try:
        logging.info("Starting product update process (existing documents only)")

        client = elastic_conn(Variable.get("elastic_scheme"))

        try:
            client.info()
        except Exception as e:
            logging.error("Elasticsearch not available: %s", e)
            raise
        logging.info("Elasticsearch connection established")

        product_mappings = get_from_s3(s3_key=S3_FILE_NAME) or []

        if not product_mappings:
            logging.warning("No product mappings found to update")
            return

        mapping_ids = [str(item.get("id")) for item in product_mappings if item.get("id")]
        logging.info(f"Checking existence for {len(mapping_ids)} product IDs")

        existing_ids = check_existing_documents(client, INDEX_NAME, mapping_ids)

        if not existing_ids:
            logging.warning("No existing products found to update")
            return

        actions = []
        for mapping in product_mappings:
            product_id = mapping.get("id")
            if not product_id:
                continue
            pid = str(product_id)
            if pid in existing_ids:
                doc = {}
                okei = mapping.get("okei_code")
                if okei is not None:
                    doc["okei_code"] = okei
                vat_val = mapping.get("vat_rate")
                if vat_val:
                    doc["vat_rate"] = vat_val

                if not doc:
                    continue

                actions.append({
                    "_op_type": "update",
                    "_index": INDEX_NAME,
                    "_id": pid,
                    "doc": doc,
                    "doc_as_upsert": False,
                    "retry_on_conflict": 3,
                })

        logging.info(f"Preparing to update {len(actions)} existing products")

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
                if (i // BATCH_SIZE) % 10 == 0:
                    logging.info(f"Product batch {i // BATCH_SIZE + 1}: {success} documents updated")

                if errors:
                    logging.error(f"Product batch {i // BATCH_SIZE + 1} had {len(errors)} errors")
                    all_errors.extend(errors)

            except Exception as bulk_error:
                logging.error(f"Bulk operation failed for product batch {i // BATCH_SIZE + 1}: {str(bulk_error)}")
                all_errors.append({"batch": i // BATCH_SIZE + 1, "error": str(bulk_error)})

            time.sleep(0.1)

        client.indices.refresh(index=INDEX_NAME)

        logging.info(
            f"Product update completed: {total_success}/{len(existing_ids)} existing products updated, {len(all_errors)} errors")

    except Exception as e:
        logging.error(f"Product update process failed: {str(e)}")
        raise


with DAG(
        dag_id=DAG_ID,
        default_args=default_args,
        schedule="40 * * * *",
        start_date=datetime(2025, 10, 31),
        catchup=False,
        max_active_runs=1,
        tags=["1c", "elasticsearch", "product", "measurement_unit", "okei_code", "vat", "shop"],
) as dag:

    fetch_products = PythonOperator(
        task_id="fetch_products_task",
        python_callable=fetch_products_and_create_mappings,
    )

    update_products = PythonOperator(
        task_id="update_existing_products_task",
        python_callable=update_in_es_callable,
    )

    fetch_products >> update_products