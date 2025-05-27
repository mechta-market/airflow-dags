import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook
from helpers.utils import request_to_site_api

INDEX_NAME = "product_v1"

def fetch_data_callable(**context):
    response = request_to_site_api(host=Variable.get("site_api_host"), endpoint="v2/airflow/product/action")
    if not response.get("success", False):
        logging.error(
            f"Error: {response.get('error_code')}; Desc: {response.get('desc')}"
        )
        return

    context["ti"].xcom_push(key="fetched_data", value=response.get("products"))

def upsert_to_es_callable(**context):
    items = context["ti"].xcom_pull(key="fetched_data", task_ids="fetch_data_task")
    if not items:
        return
    hosts = ["http://mdm.default:9200"]
    es_hook = ElasticsearchPythonHook(hosts=hosts)
    client = es_hook.get_conn
    for item in items:
        doc_id = item.get("id")
        if not doc_id:
            continue
        client.update(
            index=INDEX_NAME,
            id=doc_id,
            body={"doc": {"actions": item.get("actions")}, "doc_as_upsert": True},
        )

default_args = {
    "owner": "Sultan",
    "depends_on_past": False,
}

with DAG(
    dag_id=f"product_actions_etl",
    default_args=default_args,
    schedule_interval="*/60 * * * *",
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