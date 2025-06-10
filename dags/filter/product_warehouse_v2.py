import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any, Dict, List
import requests

from elasticsearch import helpers
from elasticsearch.helpers import BulkIndexError

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from filter.utils import clean_tmp_file, load_data_from_tmp_file, save_data_to_tmp_file
from helpers.utils import elastic_conn

# DAG parameters

DAG_ID = "product_warehouse_v2"
default_args = {
    "owner": "Olzhas",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Constants

INDEX_NAME = "product_v2"

# Configurations

logging.basicConfig(level=logging.INFO)

# Functions


class DocumentWarehouse:
    def __init__(
        self, id: str, classification: str, city_ids: List[str], real_value: int
    ):
        self.id = id
        self.classification = classification
        self.city_ids = city_ids
        self.real_value = real_value

    def to_dict(self):
        return {
            "id": self.id,
            "classification": self.classification,
            "city_id": self.city_ids,
            "real_value": self.real_value,
        }


# Tasks


def get_product_ids_callable(**context):
    client = elastic_conn(Variable.get("elastic_scheme"))

    existing_ids_query = {
        "_source": False,
        "fields": ["_id"],
        "query": {"match_all": {}},
    }

    try:
        response = client.search(
            index=INDEX_NAME, body=existing_ids_query, size=10000, scroll="2m"
        )
        scroll_id = response["_scroll_id"]
        existing_ids = {hit["_id"] for hit in response["hits"]["hits"]}
        while len(response["hits"]["hits"]) > 0:
            response = client.scroll(scroll_id=scroll_id, scroll="2m")
            existing_ids.update(hit["_id"] for hit in response["hits"]["hits"])
    except Exception as e:
        logging.error(f"failed to fetch ids from Elasticsearch: {e}")
        raise
    finally:
        if scroll_id:
            client.clear_scroll(scroll_id=scroll_id)

    product_ids = list(existing_ids)

    save_data_to_tmp_file(
        context=context,
        xcom_key="product_ids_file_path",
        data=product_ids,
        file_path=f"/tmp/{DAG_ID}.product_ids.json",
    )
    logging.info(f"extracted product_ids count: {len(product_ids)}")


def get_warehouse_callable(**context):
    BASE_URL = Variable.get("nsi_host")

    warehouses: List[dict] = []
    page = 0
    page_size = 1000

    def fetch_page(page: int) -> List[str]:
        response = requests.get(
            f"{BASE_URL}/warehouse",
            params={
                "list_params.page": page,
                "list_params.page_size": page_size,
            },
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
        return data.get("results", [])

    while True:
        try:
            results = fetch_page(page=page)
            if not results:
                break

            warehouses.extend([item for item in results if "id" in item])
            page += 1
        except Exception as e:
            logging.error(f"error during fetching warehouses: {e}")
            break

    warehouses_dict: Dict[str, dict] = {w["id"]: w for w in warehouses if w.get("id")}

    save_data_to_tmp_file(
        context=context,
        xcom_key="warehouses_file_path",
        data=warehouses_dict,
        file_path=f"/tmp/{DAG_ID}.warehouses.json",
    )
    logging.info(f"extracted warehouses count: {len(warehouses_dict)}")


def get_city_warehouse_callable(**context):
    BASE_URL = Variable.get("nsi_host")

    city_warehouses: List[dict] = []
    page = 0
    page_size = 1000

    def fetch_page(page: int) -> List[str]:
        response = requests.get(
            f"{BASE_URL}/city_warehouse",
            params={
                "list_params.page": page,
                "list_params.page_size": page_size,
            },
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
        return data.get("results", [])

    while True:
        try:
            results = fetch_page(page=page)
            if not results:
                break

            city_warehouses.extend(
                [
                    item
                    for item in results
                    if "warehouse_id" in item and "city_id" in item
                ]
            )
            page += 1
        except Exception as e:
            logging.error(f"error during fetching warehouses: {e}")
            break

    warehouse_cities_dict = {}

    for item in city_warehouses:
        warehouse_id = item.get("warehouse_id")
        city_id = item.get("city_id")

        if warehouse_id and city_id:
            if warehouse_id not in warehouse_cities_dict:
                warehouse_cities_dict[warehouse_id] = []
            warehouse_cities_dict[warehouse_id].append(city_id)

    save_data_to_tmp_file(
        context=context,
        xcom_key="warehouse_cities_file_path",
        data=warehouse_cities_dict,
        file_path=f"/tmp/{DAG_ID}.warehouse_cities.json",
    )
    logging.info(f"extracted city_warehouses count: {len(warehouse_cities_dict)}")


def transform_data_callable(**context):
    product_ids = load_data_from_tmp_file(
        context=context,
        xcom_key="product_ids_file_path",
        task_id="get_product_ids_task",
    )
    warehouses_dict = load_data_from_tmp_file(
        context=context,
        xcom_key="warehouses_file_path",
        task_id="get_warehouse_task",
    )
    warehouse_cities_dict = load_data_from_tmp_file(
        context=context,
        xcom_key="warehouse_cities_file_path",
        task_id="get_city_warehouse_task",
    )

    MAX_WORKERS = 5
    BATCH_SIZE = 100
    BASE_URL = Variable.get("store_host")

    product_warehouse_dict: Dict[str, List[Dict[str, Any]]] = {}

    def process_product_warehouse(product_id: str) -> (str, List[Dict[str, Any]]):
        response = requests.get(
            f"{BASE_URL}/product_warehouse",
            params={
                "list_params.page_size": 1000,
                "product_id": product_id,
                "has_real_value": True,
            },
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()
        results = data.get("results", [])
        if not results:
            return product_id, []

        raw_list = [item for item in results if "warehouse_id" in item]
        documents = []
        for raw in raw_list:
            warehouse_id = raw.get("warehouse_id", "")
            real_value = raw.get("real_value", 0)

            if not warehouse_id or not real_value:
                continue

            doc = DocumentWarehouse(
                id=warehouse_id,
                classification=warehouses_dict.get(warehouse_id, {}).get(
                    "classification", ""
                ),
                city_ids=warehouse_cities_dict.get(warehouse_id, []),
                real_value=real_value,
            )
            documents.append(doc.to_dict())

        return product_id, documents

    for i in range(0, len(product_ids), BATCH_SIZE):
        batch = product_ids[i : i + BATCH_SIZE]
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(process_product_warehouse, pid): pid for pid in batch
            }
            for future in as_completed(futures):
                try:
                    product_id, docs = future.result()
                    product_warehouse_dict[product_id] = docs
                except Exception as e:
                    logging.error(f"failed to process product: {e}")
                    raise

    save_data_to_tmp_file(
        context=context,
        xcom_key="product_warehouses_file_path",
        data=dict(product_warehouse_dict),
        file_path=f"/tmp/{DAG_ID}.product_warehouses.json",
    )
    logging.info(f"transformed product_warehouses count: {len(product_warehouse_dict)}")


def load_data_callable(**context):
    product_warehouses_dict = load_data_from_tmp_file(
        context=context,
        xcom_key="product_warehouses_file_path",
        task_id="transform_data_task",
    )

    client = elastic_conn(Variable.get("elastic_scheme"))

    actions = []
    for product_id, warehouses in product_warehouses_dict.items():
        actions.append(
            {
                "_op_type": "update",
                "_index": INDEX_NAME,
                "_id": product_id,
                "doc": {"warehouses": warehouses},
            }
        )

    try:
        success, errors = helpers.bulk(
            client, actions, refresh="wait_for", stats_only=False
        )
        logging.info(f"Successfully updated {success} documents.")
        if errors:
            logging.error(f"Errors encountered during bulk update: {errors}")
    except BulkIndexError as bulk_error:
        logging.error(f"Bulk update failed: {bulk_error}")
        raise


def cleanup_temp_files_callable(**context):
    tmp_file_keys = [
        {"xcom_key": "product_ids_file_path", "task_id": "get_product_ids_task"},
        {"xcom_key": "warehouses_file_path", "task_id": "get_warehouse_task"},
        {
            "xcom_key": "warehouse_cities_file_path",
            "task_id": "get_city_warehouse_task",
        },
        {"xcom_key": "product_warehouses_file_path", "task_id": "transform_data_task"},
    ]

    for tmp_file in tmp_file_keys:
        file_path = context["ti"].xcom_pull(
            key=tmp_file.get("xcom_key"), task_ids=tmp_file.get("task_id")
        )
        if file_path:
            clean_tmp_file(file_path)


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="DAG to upload product_warehouses data from Store service to Elasticsearch index",
    start_date=datetime(2025, 6, 10, 0, 10),
    schedule="*/15 * * * *",
    max_active_runs=1,
    catchup=False,
    tags=["nsi", "elasticsearch", "store"],
) as dag:
    get_product_ids = PythonOperator(
        task_id="get_product_ids_task",
        python_callable=get_product_ids_callable,
        provide_context=True,
    )

    get_warehouse = PythonOperator(
        task_id="get_warehouse_task",
        python_callable=get_warehouse_callable,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    get_city_warehouse = PythonOperator(
        task_id="get_city_warehouse_task",
        python_callable=get_city_warehouse_callable,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    transform_data = PythonOperator(
        task_id="transform_data_task",
        python_callable=transform_data_callable,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    load_data = PythonOperator(
        task_id="load_data_task",
        python_callable=load_data_callable,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    cleanup_temp_files = PythonOperator(
        task_id="cleanup_temp_files_task",
        python_callable=cleanup_temp_files_callable,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        get_product_ids
        >> [get_warehouse, get_city_warehouse]
        >> transform_data
        >> load_data
        >> cleanup_temp_files
    )
