from datetime import datetime
from helpers.utils import (
    elastic_conn,
    request_to_nsi_api,
    put_to_s3,
    get_from_s3,
)

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


DAG_ID = "product_category"
DICTIONARY_NAME = "product_category"
S3_FILE_NAME = f"{DAG_ID}/product_category.json"


def fetch_data_callable():
    resp = request_to_nsi_api(host=Variable.get("nsi_host"), endpoint=DICTIONARY_NAME)
    put_to_s3(data=resp.get("results", []), s3_key=S3_FILE_NAME)


def upsert_to_es_callable():
    """Загружаем данные в Elasticsearch."""
    items = get_from_s3(s3_key=S3_FILE_NAME)

    if not items:
        return

    client = elastic_conn(Variable.get("elastic_scheme"))

    for item in items:
        doc_id = item.get("id")
        if not doc_id:
            continue
        client.update(
            index=DICTIONARY_NAME,
            id=doc_id,
            body={"doc": item, "doc_as_upsert": True},
        )


default_args = {
    "owner": "Amir",
    "depends_on_past": False,
}

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

    upsert_to_es = PythonOperator(
        task_id="upsert_to_es_task",
        python_callable=upsert_to_es_callable,
    )

    fetch_data >> upsert_to_es
