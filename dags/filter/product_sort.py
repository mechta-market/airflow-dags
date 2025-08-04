import logging
from datetime import datetime

from airflow.sdk import DAG, Variable
from airflow.operators.python import PythonOperator
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook

from elasticsearch import helpers
from elasticsearch.helpers import BulkIndexError

from helpers.utils import request_to_site_api, put_to_s3, get_from_s3

DAG_ID = "product_sort"

INDEX_NAME = "product_v2"

S3_FILE_NAME = f"{DAG_ID}/product_sort.json"


def fetch_data_callable():
    response = request_to_site_api(
        host=Variable.get("site_api_host"), endpoint="v2/airflow/product/sort"
    )
    put_to_s3(data=response.get("products"), s3_key=S3_FILE_NAME)


def upsert_to_es_callable():
    items = get_from_s3(s3_key=S3_FILE_NAME)

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
    logging.info(f"actions count={len(actions)}")

    try:
        success, errors = helpers.bulk(
            client,
            actions,
            refresh="wait_for",
            stats_only=False,
            raise_on_error=False,
            raise_on_exception=False,
        )
        logging.info(f"successfully updated documents count={success}")
        if errors:
            logging.error(f"errors encountered: {errors}")
    except BulkIndexError as bulk_error:
        logging.error(f"bulk update failed: {bulk_error}")


default_args = {
    "owner": "Sultan",
    "depends_on_past": False,
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule="0 0 * * *",
    start_date=datetime(2025, 5, 27),
    catchup=False,
    tags=["elasticsearch", "site", "product"],
) as dag:

    fetch_data = PythonOperator(
        task_id="fetch_data_task",
        python_callable=fetch_data_callable,
    )

    upsert_to_es = PythonOperator(
        task_id="upsert_to_es_task",
        python_callable=upsert_to_es_callable,
    )

    fetch_data >> upsert_to_es
