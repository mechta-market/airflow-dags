import json
import logging
from datetime import datetime
from helpers.utils import (
    elastic_conn,
    request_to_1c,
    normalize_zero_uuid_fields,
    ZERO_UUID,
)

from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator


DAG_ID = "organisation"
DICTIONARY_NAME = "organisation"
NORMALIZE_FIELDS = []
BUCKET_NAME = "airflow"


def fetch_data_callable(**context) -> None:
    """Получаем данные из 1c и сохраняем в XCom."""
    response = request_to_1c(host=Variable.get("1c_gw_host"), dic_name=DICTIONARY_NAME)
    if not response.get("success", False):
        logging.error(
            f"Error: {response.get('error_code')}; Desc: {response.get('desc')}"
        )
        return

    data_bytes = json.dumps(response.get("data"), ensure_ascii=False).encode("utf-8")

    s3_key = "1с-data/subdivision.json"

    # Загружаем в S3
    s3 = S3Hook(aws_conn_id="s3")
    client = s3.get_conn()
    logging.info(client.list_buckets())
    s3.load_bytes(
        bytes_data=data_bytes, key=s3_key, bucket_name=BUCKET_NAME, replace=True
    )

    logging.info(f"Data saved to s3://{BUCKET_NAME}/{s3_key}")
    context["ti"].xcom_push(key=f"s3_key_{DAG_ID}", value=s3_key)


def normalize_data_callable(**context) -> None:
    """Нормализация данных перед загрузкой в Elasticsearch."""
    s3_key = context["ti"].xcom_pull(key=f"s3_key_{DAG_ID}", task_ids="fetch_data_task")
    if not s3_key:
        logging.error("No S3 key found in XCom.")
        return

    s3 = S3Hook(aws_conn_id="s3")

    # Загружаем из S3
    file_obj = s3.get_key(key=s3_key, bucket_name=BUCKET_NAME)
    file_content = file_obj.get()["Body"].read()
    items = json.loads(file_content)

    if not items:
        return

    normalized = []
    for item in items:
        if item.get("id") == ZERO_UUID:
            continue
        normalized.append(normalize_zero_uuid_fields(item, NORMALIZE_FIELDS))

    context["ti"].xcom_push(key=f"normalized_data_{DAG_ID}", value=normalized)


def upsert_to_es_callable(**context):
    """Загружаем данные в Elasticsearch."""
    items = context["ti"].xcom_pull(
        key=f"normalized_data_{DAG_ID}", task_ids="normalize_data_task"
    )
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
    schedule_interval="15 * * * *",
    start_date=datetime(2025, 5, 14),
    catchup=False,
    tags=["1c", "elasticsearch"],
) as dag:

    fetch_data = PythonOperator(
        task_id="fetch_data_task",
        python_callable=fetch_data_callable,
        provide_context=True,
    )

    normalize_data = PythonOperator(
        task_id="normalize_data_task",
        python_callable=normalize_data_callable,
        provide_context=True,
    )

    upsert_to_es = PythonOperator(
        task_id="upsert_to_es_task",
        python_callable=upsert_to_es_callable,
        provide_context=True,
    )

    fetch_data >> normalize_data >> upsert_to_es
