import json
import requests
from typing import Any
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

# from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook

DICTIONARY_NAME = "city"
INDEX_NAME = f"{DICTIONARY_NAME}_1c"


def Request_to_1C() -> Any:
    host = Variable.get("1c_gw_host")
    url = f"{host}/send/by_db_name/AstOffice/getbaseinfo/{DICTIONARY_NAME}"

    resp = requests.post(url, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    results = payload.get("results", [])
    return results


def fetch_data_callable(**context):
    """Получаем данные из 1c и сохраняем в XCom."""
    results = Request_to_1C()
    print(results)
    context["ti"].xcom_push(key="fetched_data", value=results)


def upsert_to_es_callable(**context):
    """Загружаем данные в Elasticsearch."""
    items = context["ti"].xcom_pull(key="fetched_data", task_ids="fetch_data_task")
    print(f"ITEMS: {items}")
    if not items:
        return

    # hosts = ["http://mdm.default:9200"]
    # es_hook = ElasticsearchPythonHook(
    #     hosts=hosts,
    # )
    # client = es_hook.get_conn

    # for item in items:
    #     doc_id = item.get("id")
    #     if not doc_id:
    #         continue
    #     client.update(
    #         index=INDEX_NAME,
    #         id=doc_id,
    #         body={"doc": item, "doc_as_upsert": True},
    #     )


default_args = {
    "owner": "Amir",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=f"{DICTIONARY_NAME}_1c",
    default_args=default_args,
    schedule_interval="*/10 * * * *",
    start_date=datetime(2025, 5, 14),
    catchup=False,
    tags=["1c", "elasticsearch"],
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
