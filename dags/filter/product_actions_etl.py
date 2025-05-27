import os
import json
from datetime import datetime
from helpers.utils import request_to_site_api

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook

INDEX_NAME = "product_v1"
DATA_FILE_PATH = "/tmp/product_action_site.json"


def fetch_data_callable(**context):
    response = request_to_site_api(
        host=Variable.get("site_api_host"), endpoint="v2/airflow/product/action"
    )
    with open(DATA_FILE_PATH, "w", encoding="utf-8") as f:
        json.dump(response, f, ensure_ascii=False)

    print(f"Data saved to {DATA_FILE_PATH}")
    context["ti"].xcom_push(key="data_file_path", value=DATA_FILE_PATH)


def upsert_to_es_callable(**context):
    file_path = context["ti"].xcom_pull(
        key="data_file_path", task_ids="fetch_data_task"
    )

    if not file_path or not os.path.exists(file_path):
        print("Data file not found.")
        return

    with open(file_path, "r", encoding="utf-8") as f:
        items = json.load(f)

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
