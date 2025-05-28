import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Any, Dict, List

from filter.utils import fetch_with_retry, clean_tmp_file

import requests
from elasticsearch import helpers
from elasticsearch.helpers import BulkIndexError

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook
from airflow.utils.trigger_rule import TriggerRule

# DAG parameters

DAG_ID="product_warehouse_etl"
default_args = {
    "owner": "Olzhas",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Constants

INDEX_NAME = "product_v1"

# Configurations

logging.basicConfig(level=logging.INFO)

# Functions

class DocumentWarehouse:
    def __init__(self, id: str, classification: str, city_ids: List[str], real_value: int):
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
    DATA_FILE_PATH = f"/tmp/{DAG_ID}.product_ids.json"
    
    product_ids = []

    hosts = ["http://mdm.default:9200"]
    es_hook = ElasticsearchPythonHook(hosts=hosts)
    client = es_hook.get_conn

    try:
        query = {
            "_source": False,
            "query": {"match_all": {}}
        }

        scroll = client.search(
            index=INDEX_NAME,
            body=query,
            scroll="2m",
            size=1000,
        )

        scroll_id = scroll["_scroll_id"]
        hits = scroll["hits"]["hits"]
        product_ids.extend([doc["_id"] for doc in hits])

        while hits:
            scroll = client.scroll(scroll_id=scroll_id, scroll="2m")
            hits = scroll["hits"]["hits"]
            product_ids.extend([doc["_id"] for doc in hits])

    except Exception as e:
        logging.error(f"Error fetching product IDs from Elasticsearch: {e}")
        raise

    try:
        with open(DATA_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(product_ids, f, ensure_ascii=False)
    except IOError as e:
        raise Exception(f"Task failed: couldn't save file to {DATA_FILE_PATH}") from e
    
    logging.info(f"product_ids data are saved: {len(product_ids)}")
    context["ti"].xcom_push(key="product_ids_file_path", value=DATA_FILE_PATH)


def get_warehouse_callable(**context):
    BASE_URL = "http://nsi.default"
    DATA_FILE_PATH = f"/tmp/{DAG_ID}.warehouse.json"

    warehouses = []
    page = 0
    page_size = 1000

    while True:
        try:
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
            results = data.get("results", [])
            if not results:
                break
            warehouses.extend([item for item in results if "id" in item])

            page += 1
        except Exception as e:
            logging.error(f"Error during fetching warehouses: {e}")
            break

    warehouses_dict = {
        w["id"]: w
        for w in warehouses
        if w.get("id")
    }

    try:
        with open(DATA_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(warehouses_dict, f, ensure_ascii=False)
    except IOError as e:
        raise Exception(f"Task failed: couldn't save file to {DATA_FILE_PATH}") from e
    
    logging.info(f"warehouse data are saved: {len(warehouses_dict)}")
    context["ti"].xcom_push(key="warehouses_file_path", value=DATA_FILE_PATH)


def get_city_warehouse_callable(**context):
    BASE_URL = "http://nsi.default"
    DATA_FILE_PATH = f"/tmp/{DAG_ID}.warehouse_cities.json"

    city_warehouses = []
    page = 0
    page_size = 1000

    while True:
        try:
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
            results = data.get("results", [])
            if not results:
                break
            city_warehouses.extend([item for item in results if "warehouse_id" in item])

            page += 1
        except Exception as e:
            logging.error(f"Error during fetching warehouses: {e}")
            break

    warehouse_cities_dict = defaultdict(list)

    for item in city_warehouses:
        warehouse_id = item.get("warehouse_id")
        city_id = item.get("city_id")

        if warehouse_id and city_id:
            warehouse_cities_dict[warehouse_id].append(city_id)

    try:
        with open(DATA_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(dict(warehouse_cities_dict), f, ensure_ascii=False)
    except IOError as e:
        raise Exception(f"Task failed: couldn't save file to {DATA_FILE_PATH}") from e
    
    logging.info(f"warehouse_cities data are saved: {len(warehouse_cities_dict)}")
    context["ti"].xcom_push(key="warehouse_cities_file_path", value=DATA_FILE_PATH)


def transform_data_callable(**context):
    product_ids = []
    warehouses_dict = {}
    warehouse_cities_dict = {}

    product_ids_file_path = context["ti"].xcom_pull(
        key="product_ids_file_path", task_ids="get_product_ids_task"
    )

    if not product_ids_file_path or not os.path.exists(product_ids_file_path):
        raise FileNotFoundError(f"File not found: {product_ids_file_path}")

    with open(product_ids_file_path, "r", encoding="utf-8") as f:
        product_ids = json.load(f)

    warehouses_file_path = context["ti"].xcom_pull(
        key="warehouses_file_path", task_ids="get_warehouse_task"
    )

    if not warehouses_file_path or not os.path.exists(warehouses_file_path):
        raise FileNotFoundError(f"File not found: {warehouses_file_path}")

    with open(warehouses_file_path, "r", encoding="utf-8") as f:
        warehouses_dict = json.load(f)

    warehouse_cities_file_path = context["ti"].xcom_pull(
        key="warehouse_cities_file_path", task_ids="get_city_warehouse_task"
    )

    if not warehouse_cities_file_path or not os.path.exists(warehouse_cities_file_path):
        raise FileNotFoundError(f"File not found: {warehouse_cities_file_path}")

    with open(warehouse_cities_file_path, "r", encoding="utf-8") as f:
        warehouse_cities_dict = json.load(f)

    # do

    MAX_WORKERS = 5
    BATCH_SIZE = 100
    BASE_URL = "http://store.default"

    product_warehouse_dict: Dict[str, List[Dict[str, Any]]] = {}

    def process_single_product(product_id: str) -> (str, List[Dict[str, Any]]):
        try:
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
        except requests.RequestException as e:
            logging.warning(f"Failed to fetch warehouse data for product_id={product_id}: {e}")
            return product_id, []
        
        results = data.get("results", [])
        if not results:
            return product_id, []
        
        raw_list = [item for item in results if "warehouse_id" in item]
        documents = []
        for raw in raw_list:
            warehouse_id = raw["warehouse_id"]
            doc = DocumentWarehouse(
                id=warehouse_id,
                classification=warehouses_dict.get(warehouse_id, {}).get("classification", ""),
                city_ids=warehouse_cities_dict.get(warehouse_id, []),
                real_value=raw["real_value"]
            )
            documents.append(doc.to_dict())

        return product_id, documents

    for i in range(0, len(product_ids), BATCH_SIZE):
        batch = product_ids[i:i + BATCH_SIZE]
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_single_product, pid): pid for pid in batch}
            for future in as_completed(futures):
                product_id, docs = future.result()
                if docs:
                    product_warehouse_dict[product_id] = docs

    # save

    DATA_FILE_PATH = f"/tmp/{DAG_ID}.product_warehouse.json"
    try:
        with open(DATA_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(dict(product_warehouse_dict), f, ensure_ascii=False)
    except IOError as e:
        raise Exception(f"Task failed: couldn't save file to {DATA_FILE_PATH}") from e
    
    logging.info(f"product_warehouse data are saved: {len(product_warehouse_dict)}")
    context["ti"].xcom_push(key="product_warehouse_file_path", value=DATA_FILE_PATH)


def load_data_callable(**context):
    file_path = context["ti"].xcom_pull(
        key="product_warehouse_file_path", task_ids="transform_data_task"
    )

    if not file_path or not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        product_warehouses_dict = json.load(f)
    
    hosts = ["http://mdm.default:9200"]
    es_hook = ElasticsearchPythonHook(
        hosts=hosts,
    )
    client = es_hook.get_conn

    actions = []
    for product_id, warehouses in product_warehouses_dict.items():
        actions.append({
            "_op_type": "update",
            "_index": INDEX_NAME,
            "_id": product_id,
            "doc": { 
                "warehouses": warehouses 
            },
        })

    try:
        success, errors = helpers.bulk(
            client, actions, refresh="wait_for", stats_only=False
        )
        logging.info(f"Successfully updated {success} documents.")
        if errors:
            logging.error(f"Errors encountered during bulk update: {errors}")
    except BulkIndexError as bulk_error:
        raise Exception(f"Bulk update failed: {bulk_error}") 


def cleanup_temp_files_callable(**context):
    file_path = context["ti"].xcom_pull(
        key="product_ids_file_path", task_ids="get_product_ids_task"
    )
    clean_tmp_file(file_path)

    file_path = context["ti"].xcom_pull(
        key="warehouse_file_path", task_ids="get_warehouse_task"
    )
    clean_tmp_file(file_path)

    file_path = context["ti"].xcom_pull(
        key="warehouse_cities_file_path", task_ids="get_city_warehouse_task"
    )
    clean_tmp_file(file_path)

    file_path = context["ti"].xcom_pull(
        key="product_warehouse_file_path", task_ids="transform_data_task"
    )
    clean_tmp_file(file_path)

# DAG 

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='DAG to upload product_warehouses data from Store service to Elasticsearch index',
    start_date=datetime(2025, 5, 22, 0, 5),
    schedule="*/10 * * * *",
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

    get_product_ids >> [get_warehouse, get_city_warehouse] >> transform_data >> load_data >> cleanup_temp_files
