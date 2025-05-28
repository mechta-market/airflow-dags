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
from airflow.operators.python_operator import PythonOperator


DICTIONARY_NAME = "subdivision"
NORMALIZE_FIELDS = ["node_id", "city_id"]


def fetch_data_callable(**context) -> None:
    """Получаем данные из 1c и сохраняем в XCom."""
    response = request_to_1c(host=Variable.get("1c_gw_host"), dic_name=DICTIONARY_NAME)
    if not response.get("success", False):
        logging.error(
            f"Error: {response.get('error_code')}; Desc: {response.get('desc')}"
        )
        return

    context["ti"].xcom_push(key="fetched_data", value=response.get("data"))


def normalize_data_callable(**context) -> None:
    """Нормализация данных перед загрузкой в Elasticsearch."""
    items = context["ti"].xcom_pull(key="fetched_data", task_ids="fetch_data_task")
    if not items:
        return

    normalized = []
    for item in items:
        if item.get("id") == ZERO_UUID:
            continue
        sell_warehouse_ids = []
        for sell_warehouse_id in item.get("sell_warehouse_ids"):
            if sell_warehouse_id == ZERO_UUID:
                continue
            sell_warehouse_ids.append(sell_warehouse_id)
        item["sell_warehouse_ids"] = sell_warehouse_ids
        normalized.append(normalize_zero_uuid_fields(item, NORMALIZE_FIELDS))

    context["ti"].xcom_push(key="normalized_data", value=normalized)


def upsert_to_es_callable(**context):
    """Загружаем данные в Elasticsearch."""
    items = context["ti"].xcom_pull(
        key="normalized_data", task_ids="normalize_data_task"
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
    dag_id=f"{DICTIONARY_NAME}",
    default_args=default_args,
    schedule_interval="35 * * * *",
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
