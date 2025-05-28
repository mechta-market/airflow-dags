import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from collections import defaultdict

from filter.utils import fetch_with_retry, clean_tmp_file, REQUEST_TIMEOUT

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
    context["ti"].xcom_push(key="warehouse_file_path", value=DATA_FILE_PATH)


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
    logging.info("product_warehouse are transformed.")


def load_data_callable(**context):
    logging.info("product_warehouse are loaded.")


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


# DAG initialization.
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
