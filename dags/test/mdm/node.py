import logging
from datetime import datetime
from helpers.utils import (
    elastic_conn,
    request_to_1c,
    normalize_zero_uuid_fields,
    put_to_s3,
    get_from_s3,
    ZERO_UUID,
)

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator

DAG_ID = "node"
DICTIONARY_NAME = "node"
NORMALIZE_FIELDS = ["parent_id"]
S3_FILE_NAME = f"{DAG_ID}/node.json"


def fetch_data_callable() -> None:
    """Получаем данные из 1c и сохраняем в XCom."""
    response = request_to_1c(host=Variable.get("1c_gw_host"), dic_name=DICTIONARY_NAME)
    if not response.get("success", False):
        logging.error(
            f"Error: {response.get('error_code')}; Desc: {response.get('desc')}"
        )
        return

    put_to_s3(data=response.get("data"), s3_key=S3_FILE_NAME)


def normalize_data_callable() -> None:
    """Нормализация данных перед загрузкой в Elasticsearch."""
    items = get_from_s3(s3_key=S3_FILE_NAME)

    if not items:
        return

    normalized = []
    for item in items:
        if item.get("id") == ZERO_UUID:
            continue
        # Нормализуем поля
        normalized_item = normalize_zero_uuid_fields(item, NORMALIZE_FIELDS)

        # Очистка и фильтрация hosts, если это список
        if "hosts" in normalized_item and isinstance(normalized_item["hosts"], list):
            filtered_hosts = [
                host.strip() for host in normalized_item["hosts"] if host.strip()
            ]
            normalized_item["hosts"] = filtered_hosts

        normalized.append(normalized_item)

    put_to_s3(data=normalized, s3_key=S3_FILE_NAME)


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
    schedule_interval="10 * * * *",
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
