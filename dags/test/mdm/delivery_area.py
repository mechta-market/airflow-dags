import logging
from typing import Any
from datetime import datetime

from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from elasticsearch import helpers

from helpers.utils import (
    elastic_conn,
    request_to_onec_proxy,
    normalize_zero_uuid_fields,
    get_from_s3,
    put_to_s3,
    ZERO_UUID,
    SHOP_DEFAULT_DB_NAME
)

DAG_ID = "delivery_area"
default_args = {
    "owner": "Olzhas",
    "depends_on_past": False,
}
INDEX_NAME = "delivery_area"

NORMALIZE_FIELDS = ["parent_id", "organization"]

S3_FILE_NAME = f"{DAG_ID}/delivery_area.json"


def fetch_data_callable() -> None:
    response = request_to_onec_proxy(body={
        "method": "GET",
        "path": "/getbaseinfo/delivery_areas",
        "node": {
            "name": "AstInetShop" # SHOP_DEFAULT_DB_NAME
        }
    })
    if not response.get("success", False):
        logging.error(
            f"error: {response.get('error_code')}; desc: {response.get('desc')}"
        )
        return

    logging.info(f"data count={len(response.get("data", []))}")

    put_to_s3(data=response.get("data"), s3_key=S3_FILE_NAME)


def normalize_data_callable() -> None:
    items = get_from_s3(s3_key=S3_FILE_NAME)

    if not items:
        return

    normalized = []
    for item in items:
        if item.get("id") == ZERO_UUID:
            continue
        normalized.append(normalize_zero_uuid_fields(item, NORMALIZE_FIELDS))

    put_to_s3(data=normalized, s3_key=S3_FILE_NAME)

def delete_different_data_callable():
    extracted_data = get_from_s3(s3_key=S3_FILE_NAME)
    if not extracted_data:
        raise ValueError(f"no data found in {S3_FILE_NAME}")
    
    extracted_ids = {item["id"] for item in extracted_data if item.get("id")}

    client = elastic_conn(Variable.get("elastic_scheme"))

    existing_ids_query = {
        "_source": False,
        "fields": ["_id"],
        "query": {"match_all": {}},
    }

    scroll_id = Any
    try:
        response = client.search(
            index=INDEX_NAME, body=existing_ids_query, size=5000, scroll="2m"
        )
        scroll_id = response["_scroll_id"]
        existing_ids = {hit["_id"] for hit in response["hits"]["hits"]}
        while len(response["hits"]["hits"]) > 0:
            response = client.scroll(scroll_id=scroll_id, scroll="2m")
            existing_ids.update(hit["_id"] for hit in response["hits"]["hits"])
    except Exception as e:
        logging.error(f"failed to fetch ids from Elasticsearch: {e}")
        raise
    finally:
        if scroll_id:
            client.clear_scroll(scroll_id=scroll_id)

    ids_to_delete = existing_ids - extracted_ids
    logging.info(f"ids to delete count={len(ids_to_delete)}")

    delete_actions = [
        {
            "_op_type": "delete",
            "_index": INDEX_NAME,
            "_id": id,
        }
        for id in ids_to_delete
    ]

    if delete_actions:
        try:
            success, errors = helpers.bulk(
                client, delete_actions, refresh="wait_for", stats_only=False
            )
            logging.info(f"delete success, deleted document count={success}")
            if errors:
                logging.error(f"error during bulk delete: {errors}")
        except Exception as bulk_error:
            logging.error(f"bulk delete failed, error: {bulk_error}")
            raise

def upsert_to_es_callable():
    source_data = get_from_s3(s3_key=S3_FILE_NAME)
    logging.info(f"source_data count={len(source_data)}")

    if not source_data:
        return

    es_client = elastic_conn(Variable.get("elastic_scheme"))

    actions = [
        {
            "_op_type": "update",
            "_index": INDEX_NAME,
            "_id": record.get("id"),
            "doc": record,
            "doc_as_upsert": True,
            "retry_on_conflict": 3,
        }
        for record in source_data
        if record.get("id")
    ]
    logging.info(f"actions count={len(actions)}")

    if actions:
        try:
            success, errors = helpers.bulk(
                es_client, actions, refresh="wait_for", stats_only=False
            )
            logging.info(f"update success, updated documents count={success}")
            if errors:
                logging.error(f"error during bulk update: {errors}")
        except Exception as bulk_error:
            logging.error(f"bulk update failed, error: {bulk_error}")
            raise
    else:
        logging.info("no records to update")


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=datetime(2025, 10, 10),
    schedule="50 * * * *",
    max_active_runs=1,
    catchup=False,
    tags=["1c", "elasticsearch"],
) as dag:

    fetch_data = PythonOperator(
        task_id="fetch_data_task",
        python_callable=fetch_data_callable,
    )

    normalize_data = PythonOperator(
        task_id="normalize_data_task",
        python_callable=normalize_data_callable,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    delete_different_data = PythonOperator(
        task_id="delete_different_data_task",
        python_callable=delete_different_data_callable,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    upsert_to_es = PythonOperator(
        task_id="upsert_to_es_task",
        python_callable=upsert_to_es_callable,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    fetch_data >> normalize_data >> delete_different_data >> upsert_to_es
