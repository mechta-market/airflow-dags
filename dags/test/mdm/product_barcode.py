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
from airflow.operators.python import PythonOperator


from elasticsearch import helpers
from elasticsearch.helpers import BulkIndexError


DAG_ID = "product_barcode"
DICTIONARY_NAME = "product_barcode"
S3_FILE_NAME = f"{DAG_ID}/product_barcode.json"

NORMALIZE_FIELDS = []


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
        if item.get("product_id") == ZERO_UUID or not item.get("value"):
            continue

        normalized.append(normalize_zero_uuid_fields(item, NORMALIZE_FIELDS))

    put_to_s3(data=normalized, s3_key=S3_FILE_NAME)


def upsert_to_es_callable():
    """Загружаем данные в Elasticsearch."""
    items = get_from_s3(s3_key=S3_FILE_NAME)

    if not items:
        return

    client = elastic_conn(Variable.get("elastic_scheme"))

    actions = [
        {
            "_op_type": "update",
            "_index": DICTIONARY_NAME,
            "_id": item.get("product_id"),
            "doc": item,
            "doc_as_upsert": True,
        }
        for item in items
        if item.get("product_id")
    ]
    logging.info(f"ACTIONS COUNT {len(actions)}.")

    try:
        success, errors = helpers.bulk(
            client, actions, refresh="wait_for", stats_only=False
        )
        logging.info(f"Successfully updated {success} documents.")
        if errors:
            logging.error(f"Errors encountered: {errors}")
    except BulkIndexError as bulk_error:
        logging.error(f"Bulk update failed: {bulk_error}")


default_args = {
    "owner": "Amir",
    "depends_on_past": False,
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule="20 * * * *",
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
