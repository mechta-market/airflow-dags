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
from airflow.operators.python_operator import PythonOperator

from elasticsearch import helpers
from elasticsearch.helpers import BulkIndexError

DAG_ID = "city"
DICTIONARY_NAME = "city"
WAREHOUSE_INDEX_NAME = "warehouse"
SUBDIVISION_INDEX_NAME = "subdivision"
NORMALIZE_FIELDS = ["cb_subdivision_id", "i_shop_subdivision_id", "organisation_id"]
S3_FILE_NAME = f"{DAG_ID}/city.json"


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
        normalized.append(normalize_zero_uuid_fields(item, NORMALIZE_FIELDS))

    put_to_s3(data=normalized, s3_key=S3_FILE_NAME)


def fetch_data_from_subdivision_callable():
    items = get_from_s3(s3_key=S3_FILE_NAME)

    if not items:
        return

    client = elastic_conn(Variable.get("elastic_scheme"))

    query = {"query": {"match_all": {}}}

    response = client.search(
        index=SUBDIVISION_INDEX_NAME, body=query, size=100, scroll="2m"
    )

    subdivision_map = {
        hit["_id"]: node_id
        for hit in response["hits"]["hits"]
        if (node_id := hit["_source"].get("node_id"))
    }

    for item in items:
        subdivision_id = item.get("cb_subdivision_id")
        if subdivision_id in subdivision_map:
            item["cb_node_id"] = subdivision_map[subdivision_id]

    put_to_s3(data=items, s3_key=S3_FILE_NAME)


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


def upsert_city_ids_in_warehouse_callable():
    """Загружаем данные в Elasticsearch."""
    items = get_from_s3(s3_key=S3_FILE_NAME)

    client = elastic_conn(Variable.get("elastic_scheme"))

    warehouse_city_ids = {}
    for item in items:
        warehouse_ids = item.get("warehouse_ids")
        for warehouse_id in warehouse_ids:
            if not warehouse_id:
                logging.info(f"warehouse_id is empty: {warehouse_id}")
                continue
            city_id = item.get("id")
            if not city_id:
                logging.info(f"city_id is empty: {city_id}")
                continue
            warehouse_city_ids.setdefault(warehouse_id, set()).add(city_id)

    actions = []
    for warehouse_id, city_ids in warehouse_city_ids.items():
        if not warehouse_id or not city_ids:
            logging.info(
                f"warehouse_id or city_ids is empty: wid: {warehouse_id}, cids: {city_ids}"
            )
            continue

        actions.append(
            {
                "_op_type": "update",
                "_index": WAREHOUSE_INDEX_NAME,
                "_id": warehouse_id,
                "doc": {"city_ids": list(city_ids)},
            }
        )

    logging.info(f"ACTIONS: {actions}.")
    logging.info(f"ACTIONS COUNT {len(actions)}.")
    if actions:
        try:
            success, errors = helpers.bulk(
                client,
                actions,
                refresh="wait_for",
                stats_only=False,
                raise_on_error=False,
                raise_on_exception=False,
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
    schedule_interval="0 * * * *",
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

    fetch_data_from_subdivision = PythonOperator(
        task_id="fetch_data_from_subdivision_task",
        python_callable=fetch_data_from_subdivision_callable,
        provide_context=True,
    )

    upsert_to_es = PythonOperator(
        task_id="upsert_to_es_task",
        python_callable=upsert_to_es_callable,
        provide_context=True,
    )

    upsert_city_ids_in_warehouse = PythonOperator(
        task_id="upsert_city_ids_in_warehouse_task",
        python_callable=upsert_city_ids_in_warehouse_callable,
        provide_context=True,
    )

    (
        fetch_data
        >> normalize_data
        >> fetch_data_from_subdivision
        >> [upsert_to_es, upsert_city_ids_in_warehouse]
    )
