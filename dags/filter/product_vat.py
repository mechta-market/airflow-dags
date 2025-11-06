import logging
from datetime import datetime, timedelta
from typing import List, Any
import time

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from elasticsearch import helpers, NotFoundError

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

DAG_ID = "product_vat"
INDEX_NAME = "product_vat"
S3_FILE_NAME = f"{DAG_ID}/product_vat.json"

BATCH_SIZE = 250


def fetch_data_callable(**context) -> None:
    try:
        logging.info("Starting VAT fetch")

        response = request_to_onec_proxy(body={
            "method": "GET",
            "path": "/getbaseinfo/vat_rate",
            "node": {"name": SHOP_DEFAULT_DB_NAME}
        })

        if not response.get("success", False):
            logging.error(
                f"error: {response.get('error_code')}; desc: {response.get('desc')}"
            )
            logging.error(f"Failed to fetch measurement units, success: {response.get('success', False)}, response: {str(response)[:2000]}")
            raise

        data = response.get("data", [])

        if not data:
            logging.error("No VAT data received from 1C API")
            raise

        valid_data = [item for item in data if item.get("id") is not None and item.get("id") != ZERO_UUID]
        if len(valid_data) != len(data):
            logging.warning(f"Filtered out {len(data) - len(valid_data)} items without ID")

        put_to_s3(data=valid_data, s3_key=S3_FILE_NAME)

        logging.info(f"Successfully fetched {len(valid_data)} VAT")

    except Exception as e:
        logging.error(f"Failed to fetch VAT: {str(e)}")
        raise

def delete_previous_data_callable(**context):
    items = get_from_s3(s3_key=S3_FILE_NAME)

    if not items:
        logging.error("No incoming VAT file found on S3; nothing to delete.")
        raise

    incoming_ids = {str(item["id"]) for item in items if item.get("id") is not None}

    client = elastic_conn(Variable.get("elastic_scheme"))

    try:
        if not client.indices.exists(index=INDEX_NAME):
            logging.info(f"Index {INDEX_NAME} does not exist — skipping delete step.")
            raise
    except Exception as e:
        logging.error(f"Failed checking index existence for {INDEX_NAME}: {e}")
        raise

    query = {"_source": False, "query": {"match_all": {}}}
    scroll_id = None
    existing_ids = set()

    try:
        resp = client.search(index=INDEX_NAME, body=query, size=5000, scroll="2m")
        scroll_id = resp.get("_scroll_id")
        hits = resp.get("hits", {}).get("hits", [])
        for h in hits:
            existing_ids.add(h["_id"])

        while hits:
            resp = client.scroll(scroll_id=scroll_id, scroll="2m")
            scroll_id = resp.get("_scroll_id")
            hits = resp.get("hits", {}).get("hits", [])
            for h in hits:
                existing_ids.add(h["_id"])

    except NotFoundError:
        logging.info(f"Index {INDEX_NAME} not found during search; nothing to delete.")
        raise
    except Exception as e:
        logging.error(f"failed to fetch ids from Elasticsearch: {e}")
        raise
    finally:
        try:
            if scroll_id:
                client.clear_scroll(scroll_id=scroll_id)
        except Exception:
            logging.debug("clear_scroll failed or was unnecessary")

    ids_to_delete = existing_ids - incoming_ids
    if not ids_to_delete:
        logging.info("No stale documents to delete")
        return

    actions = [
        {"_op_type": "delete", "_index": INDEX_NAME, "_id": doc_id}
        for doc_id in ids_to_delete
    ]

    try:
        success, errors = helpers.bulk(
            client,
            actions,
            refresh="wait_for",
            stats_only=False,
            raise_on_error=False,
        )
        logging.info(f"delete success, deleted document count={success}")
        if errors:
            logging.error(f"error during bulk delete: {errors}")
    except Exception as bulk_error:
        logging.error(f"bulk delete failed, error: {bulk_error}")
        raise

    logging.info(f"Deleted {len(ids_to_delete)} stale documents from {INDEX_NAME}")

def upsert_to_es_callable(**context) -> None:
    try:
        logging.info("Starting VATs upsert")

        client = elastic_conn(Variable.get("elastic_scheme"))

        try:
            client.info()
        except Exception as e:
            logging.error("Elasticsearch not available: %s", e)
            raise

        logging.info("Elasticsearch connection established")

        results = {}

        data = get_from_s3(s3_key=S3_FILE_NAME) or []

        if data:
            actions = []
            for item in data:
                if item.get("id"):
                    actions.append({
                        "_op_type": "update",
                        "_index": INDEX_NAME,
                        "_id": str(item.get("id")),
                        "doc": item,
                        "doc_as_upsert": True,
                        "retry_on_conflict": 3,
                    })

            if actions:
                try:
                    success, errors = helpers.bulk(
                        client,
                        actions,
                        refresh=False,
                        stats_only=False,
                        request_timeout=60,
                        raise_on_error=False,
                    )
                except Exception as bulk_exc:
                    logging.error(f"Bulk upsert raised exception: {bulk_exc}")
                    success = 0
                    errors = [{"error": str(bulk_exc)}]

                results["vat"] = {
                    "total": len(actions),
                    "success": success,
                    "errors": errors
                }
                logging.info(f"VAT: {success} documents processed")
                if errors:
                    logging.error(f"Upsert errors sample: {errors[:3]}")
            else:
                results["vat"] = {"total": 0, "success": 0, "errors": []}
                logging.info("No VAT to process")
        else:
            logging.error("No VAT data found")
            raise

        # refresh destination index
        try:
            client.indices.refresh(index=INDEX_NAME)
        except Exception as e:
            logging.warning(f"Failed to refresh index {INDEX_NAME}: {e}")

        logging.info(
            "Dictionary upsert completed: %d successful, %d errors",
            results["vat"]["success"],
            len(results["vat"]["errors"])
        )

    except Exception as e:
        logging.error(f"Dictionary upsert process failed: {str(e)}")
        raise



with DAG(
        dag_id=DAG_ID,
        default_args=default_args,
        schedule="35 * * * *",
        start_date=datetime(2025, 10, 31),
        catchup=False,
        max_active_runs=1,
        tags=["1c", "elasticsearch", "product", "vat", "shop"],
) as dag:

    fetch_data = PythonOperator(
        task_id="fetch_data_task",
        python_callable=fetch_data_callable,
    )

    delete_previous_data = PythonOperator(
        task_id="delete_previous_data_task",
        python_callable=delete_previous_data_callable,
    )

    upsert_to_es = PythonOperator(
        task_id="upsert_to_es_task",
        python_callable=upsert_to_es_callable,
    )

    fetch_data >> delete_previous_data >> upsert_to_es