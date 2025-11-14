import logging
from typing import Any
from datetime import datetime

from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.python import PythonOperator

from elasticsearch import helpers
from elasticsearch.helpers import BulkIndexError

from helpers.utils import (
    elastic_conn,
    request_to_nsi_api,
    put_to_s3,
    get_from_s3,
)

DAG_ID = "product_category"
default_args = {
    "owner": "Olzhas",
    "depends_on_past": False,
}

DICTIONARY_NAME = "product_category"
INDEX_NAME = DICTIONARY_NAME

S3_FILE_NAME = f"{DAG_ID}/product_category.json"


def fetch_data_callable():
    resp = request_to_nsi_api(host=Variable.get("nsi_host"), endpoint=DICTIONARY_NAME)

    data = resp.get("results", [])

    if not data:  # len(data) < 1
        raise ValueError("source provided no data")

    put_to_s3(data=data, s3_key=S3_FILE_NAME)


def delete_different_data_callable() -> None:
    items = get_from_s3(s3_key=S3_FILE_NAME)
    if not items:
        raise ValueError(f"no data found in {S3_FILE_NAME}")

    extracted_ids = {item.get("id") for item in items}

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

    ids_to_delete = existing_ids.difference(extracted_ids)
    logging.info(f"product ids to delete count={len(ids_to_delete)}")

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
    items = get_from_s3(s3_key=S3_FILE_NAME)

    if not items:
        return

    client = elastic_conn(Variable.get("elastic_scheme"))

    actions = [
        {
            "_op_type": "update",
            "_index": DICTIONARY_NAME,
            "_id": item.get("id"),
            "doc": item,
            "doc_as_upsert": True,
        }
        for item in items
        if item.get("id")
    ]
    logging.info(f"actions count={len(actions)}")

    try:
        success, errors = helpers.bulk(
            client, actions, refresh="wait_for", stats_only=False
        )
        logging.info(f"successfully updated documents={success}")
        if errors:
            logging.error(f"errors encountered: {errors}")
    except BulkIndexError as bulk_error:
        logging.error(f"bulk update failed: {bulk_error}")


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule="45 * * * *",
    start_date=datetime(2025, 5, 14),
    catchup=False,
    tags=["nsi", "elasticsearch"],
) as dag:

    fetch_data = PythonOperator(
        task_id="fetch_data_task",
        python_callable=fetch_data_callable,
    )

    delete_different_data = PythonOperator(
        task_id="delete_different_data_task",
        python_callable=delete_different_data_callable,
    )

    upsert_to_es = PythonOperator(
        task_id="upsert_to_es_task",
        python_callable=upsert_to_es_callable,
    )

    fetch_data >> delete_different_data >> upsert_to_es
