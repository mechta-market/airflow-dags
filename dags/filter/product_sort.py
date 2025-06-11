import os
import json
import logging
from datetime import datetime
from helpers.utils import request_to_site_api

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook

from elasticsearch import helpers
from elasticsearch.helpers import BulkIndexError

DAG_ID = "product_sort"

INDEX_NAME = "product_v2"
DATA_FILE_PATH = f"/tmp/{DAG_ID}.product_sort_site.json"


def fetch_data_callable(**context):
    response = request_to_site_api(
        host=Variable.get("site_api_host"), endpoint="v2/airflow/product/sort"
    )
    with open(DATA_FILE_PATH, "w", encoding="utf-8") as f:
        json.dump(response.get("products"), f, ensure_ascii=False)

    logging.info(f"Data saved to {DATA_FILE_PATH}")
    context["ti"].xcom_push(key="data_file_path", value=DATA_FILE_PATH)


def upsert_to_es_callable(**context):
    file_path = context["ti"].xcom_pull(
        key="data_file_path", task_ids="fetch_data_task"
    )

    if not file_path or not os.path.exists(file_path):
        logging.info("Data file not found.")
        return

    with open(file_path, "r", encoding="utf-8") as f:
        items = json.load(f)

    if not items:
        return

    hosts = ["http://mdm.default:9200"]
    es_hook = ElasticsearchPythonHook(hosts=hosts)
    client = es_hook.get_conn
    actions = [
        {
            "_op_type": "update",
            "_index": INDEX_NAME,
            "_id": item.get("id"),
            "doc": item,
        }
        for item in items
        if item.get("id")
    ]
    logging.info(f"ACTIONS COUNT {len(actions)}.")

    try:
        success, errors = helpers.bulk(
            client, 
            actions, 
            refresh="wait_for", 
            stats_only=False,
            raise_on_error=False, 
            raise_on_exception=False
        )
        logging.info(f"Successfully updated {success} documents.")
        if errors:
            logging.error(f"Errors encountered: {errors}")
    except BulkIndexError as bulk_error:
        logging.error(f"Bulk update failed: {bulk_error}")


default_args = {
    "owner": "Sultan",
    "depends_on_past": False,
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval="0 0 * * *",
    start_date=datetime(2025, 5, 27),
    catchup=False,
    tags=["elasticsearch", "site"],
) as dag:

    fetch_data = PythonOperator(
        task_id="fetch_data_task",
        python_callable=fetch_data_callable,
        provide_context=True,
    )

    upsert_to_es = PythonOperator(
        task_id="upsert_to_es_task",
        python_callable=upsert_to_es_callable,
        provide_context=True,
    )

    fetch_data >> upsert_to_es
