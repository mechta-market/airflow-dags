from datetime import datetime
from helpers.utils import elastic_conn, request_to_nsi_api

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


DICTIONARY_NAME = "product_category"
INDEX_NAME = f"{DICTIONARY_NAME}_nsi"


def fetch_data_callable(**context):
    resp = request_to_nsi_api(host="http://nsi.default", endpoint={DICTIONARY_NAME})
    context["ti"].xcom_push(key="fetched_data", value=resp.get("results", []))


def upsert_to_es_callable(**context):
    """Загружаем данные в Elasticsearch."""
    items = context["ti"].xcom_pull(key="fetched_data", task_ids="fetch_data_task")
    if not items:
        return

    client = elastic_conn()

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
}

with DAG(
    dag_id=f"{DICTIONARY_NAME}_nsi",
    default_args=default_args,
    schedule_interval="45 */60 * * *",
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
