import logging
from datetime import datetime
from helpers.utils import request_to_1c, normalize_zero_uuid_fields, ZERO_UUID

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook

from elasticsearch import helpers
from elasticsearch.helpers import BulkIndexError

DICTIONARY_NAME = "city"
INDEX_NAME = f"{DICTIONARY_NAME}_1c"
WAREHOUSE_INDEX_NAME = "warehouse_1c"
SUBDIVISION_INDEX_NAME = "subdivision_1c"
NORMALIZE_FIELDS = ["cb_subdivision_id", "i_shop_subdivision_id", "organisation_id"]


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
        normalized.append(normalize_zero_uuid_fields(item, NORMALIZE_FIELDS))

    context["ti"].xcom_push(key="normalized_data", value=normalized)


def fetch_data_from_subdivision_callable(**context):
    items = context["ti"].xcom_pull(
        key="normalized_data", task_ids="normalize_data_task"
    )
    if not items:
        return

    hosts = ["http://mdm.default:9200"]
    es_hook = ElasticsearchPythonHook(
        hosts=hosts,
    )
    client = es_hook.get_conn

    query = {"query": {"match_all": {}}}

    response = client.search(index=SUBDIVISION_INDEX_NAME, body=query, scroll="2m")

    subdivision_map = {
        hit["_id"]: hit["_source"].get("node_id") for hit in response["hits"]["hits"]
    }
    logging.info(f"subdivision_map: {subdivision_map}")

    for item in items:
        subdivision_id = item.get("cb_subdivision_id")
        if subdivision_id in subdivision_map:
            item["cb_node_id"] = subdivision_map[subdivision_id]

    logging.info(items)
    context["ti"].xcom_push(key="fetched_data_from_subdivision", value=items)


def upsert_to_es_callable(**context):
    """Загружаем данные в Elasticsearch."""
    items = context["ti"].xcom_pull(
        key="fetched_data_from_subdivision", task_ids="fetch_data_from_subdivision_task"
    )
    if not items:
        return

    hosts = ["http://mdm.default:9200"]
    es_hook = ElasticsearchPythonHook(
        hosts=hosts,
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


def upsert_city_ids_in_warehouse_callable(**context):
    """Загружаем данные в Elasticsearch."""
    items = context["ti"].xcom_pull(
        key="fetched_data_from_subdivision", task_ids="fetch_data_from_subdivision_task"
    )
    logging.info(f"Pulled items for upsert_city_ids_in_warehouse: {items}")
    if not items:
        logging.info("No items found, exiting.")
        return

    hosts = ["http://mdm.default:9200"]
    es_hook = ElasticsearchPythonHook(
        hosts=hosts,
    )
    client = es_hook.get_conn

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
                f"warehouse_id or city_ids is empty: wid: {warehouse_id}, cids: {city_id}"
            )
            continue

        actions.append(
            {
                "_op_type": "update",
                "_index": WAREHOUSE_INDEX_NAME,
                "_id": warehouse_id,
                "doc": {"city_ids": list(city_ids)},
                "doc_as_upsert": True,  # optional: creates if not exists
            }
        )

    if actions:
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
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=f"{DICTIONARY_NAME}_1c",
    default_args=default_args,
    schedule_interval="*/60 * * * *",
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
