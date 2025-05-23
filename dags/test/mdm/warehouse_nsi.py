import json
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook

DICTIONARY_NAME = "warehouse"
INDEX_NAME = f"{DICTIONARY_NAME}_nsi"


def get_token() -> str:
    access_token = Variable.get("access_token")
    return f"Bearer {access_token}"


def get_elasticsearch_password() -> str:
    el_password = Variable.get("elasticsearch_password")
    return el_password


def fetch_data_callable(**context):
    """Получаем данные из NSI и сохраняем в XCom."""
    url = f"https://api.mdev.kz/nsi/{DICTIONARY_NAME}"
    headers = {
        "Authorization": get_token(),
    }
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    results = payload.get("results", [])
    context["ti"].xcom_push(key="fetched_data", value=results)


def upsert_to_es_callable(**context):
    """Загружаем данные в Elasticsearch."""
    items = context["ti"].xcom_pull(key="fetched_data", task_ids="fetch_data_task")
    if not items:
        return

    hosts = ["https://mdm.zeon.mdev.kz"]
    es_hook = ElasticsearchPythonHook(
        hosts=hosts,
        es_conn_args={"basic_auth": ("mdm", get_elasticsearch_password())},
    )
    client = es_hook.get_conn

    for item in items:
        doc_id = item.get("id")
        if not doc_id:
            continue
        client.update(
            index=INDEX_NAME,
            id=doc_id,
            body={"doc": item, "doc_as_upsert": True},
        )


default_args = {
    "owner": "Amir",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=f"{DICTIONARY_NAME}_nsi",
    default_args=default_args,
    schedule_interval="*/10 * * * *",
    start_date=datetime(2025, 5, 14),
    catchup=False,
    tags=["nsi", "elasticsearch"],
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
