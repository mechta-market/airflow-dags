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

DAG_ID="product_price_etl"
default_args = {
    "owner": "Olzhas",
    "depends_on_past": False,
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
}

# Constants

INDEX_NAME = "product_v1"

ASTANA_CITY_ID = "cc4316f8-4333-11ea-a22d-005056b6dbd7"
ASTANA_OFFICE_SUBDIVISION_ID = "3bd9bf4f-7dd7-11e8-a213-005056b6dbd7"

# Configurations

logging.basicConfig(level=logging.INFO)

# Functions

class DocumentBasePrice:
    def __init__(self, city_id: str, price: float):
        self.city_id = city_id
        self.price = price

    def to_dict(self):
        return {
            "city_id": self.city_id,
            "price": self.price,
        }

class DocumentFinalPrice:
    def __init__(self, city_id: str, subdivision_id: str, is_i_shop: bool, price: float):
        self.city_id = city_id
        self.subdivision_id = subdivision_id
        self.is_i_shop = is_i_shop
        self.price = price

    def to_dict(self):
        return {
            "city_id": self.city_id,
            "subdivision_id": self.subdivision_id,
            "is_i_shop": self.is_i_shop,
            "price": self.price,
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
            if not hits:
                break
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

def get_city_callable(**context):
    BASE_URL = "http://nsi.default"
    DATA_FILE_PATH = f"/tmp/{DAG_ID}.city.json"

    cities = []
    page = 0
    page_size = 1000

    while True:
        try:
            response = requests.get(
                f"{BASE_URL}/city",
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
            cities.extend([item for item in results if "id" in item])

            page += 1
        except Exception as e:
            logging.error(f"Error during fetching warehouses: {e}")
            break

    cities_dict = {
        c["id"]: c
        for c in cities
        if c.get("id")
    }

    try:
        with open(DATA_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(cities_dict, f, ensure_ascii=False)
    except IOError as e:
        raise Exception(f"Task failed: couldn't save file to {DATA_FILE_PATH}") from e
    
    logging.info(f"city data are saved: {len(cities_dict)}")
    context["ti"].xcom_push(key="cities_file_path", value=DATA_FILE_PATH)

def get_subdivision_callable(**context):
    BASE_URL = "http://nsi.default"
    DATA_FILE_PATH = f"/tmp/{DAG_ID}.subdivision.json"

    subdivisions = []
    page = 0
    page_size = 1000

    while True:
        try:
            response = requests.get(
                f"{BASE_URL}/subdivision",
                params={
                    "list_params.page": page,
                    "list_params.page_size": page_size,
                    "is_shop": True,
                },
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])
            if not results:
                break
            subdivisions.extend([item for item in results if "id" in item])

            page += 1
        except Exception as e:
            logging.error(f"Error during fetching warehouses: {e}")
            break

    subdivisions_dict = {
        s["id"]: s
        for s in subdivisions
        if s.get("id")
    }

    try:
        with open(DATA_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(subdivisions_dict, f, ensure_ascii=False)
    except IOError as e:
        raise Exception(f"Task failed: couldn't save file to {DATA_FILE_PATH}") from e
    
    logging.info(f"subdivision data are saved: {len(subdivisions_dict)}")
    context["ti"].xcom_push(key="subdivisions_file_path", value=DATA_FILE_PATH)

def transform_base_price_callable(**context):
    product_ids = []
    cities_dict = {}

    product_ids_file_path = context["ti"].xcom_pull(
        key="product_ids_file_path", task_ids="get_product_ids_task"
    )

    if not product_ids_file_path or not os.path.exists(product_ids_file_path):
        raise FileNotFoundError(f"File not found: {product_ids_file_path}")

    with open(product_ids_file_path, "r", encoding="utf-8") as f:
        product_ids = json.load(f)

    cities_file_path = context["ti"].xcom_pull(
        key="cities_file_path", task_ids="get_city_task"
    )

    if not cities_file_path or not os.path.exists(cities_file_path):
        raise FileNotFoundError(f"File not found: {cities_file_path}")

    with open(cities_file_path, "r", encoding="utf-8") as f:
        cities_dict = json.load(f)

    # do

    MAX_WORKERS = 5
    BATCH_SIZE = 100
    BASE_URL = "http://price.default"

    product_base_price_dict: Dict[str, List[Dict[str, Any]]] = {}

    def process_single_product(product_id: str) -> (str, List[Dict[str, Any]]):
        base_price = {}
        spec_base_prices = []

        try:
            response = requests.get(
                f"{BASE_URL}/base_price/{product_id}",
                timeout=10,
            )
            response.raise_for_status()
            base_price = response.json()
        except requests.RequestException as e:
            logging.warning(f"Failed to fetch base_price for product_id={product_id}: {e}")
            return product_id, []

        try:
            response = requests.get(
                f"{BASE_URL}/spec_base_price",
                params={
                    "list_params.page_size": 1000,
                    "product_id": product_id,
                },
                timeout=60,
            )
            response.raise_for_status()
            data = response.json()
            spec_base_prices = data.get("results", [])
        except requests.RequestException as e:
            logging.warning(f"Failed to fetch warehouse data for product_id={product_id}: {e}")
            return product_id, []
        
        cities_set = set()
        result = []

        if base_price.get("price", 0):
            result.append(
                DocumentBasePrice(
                    city_id=ASTANA_CITY_ID,
                    price=base_price.get("price", 0),
                ).to_dict()
            )
            cities_set.add(ASTANA_CITY_ID)

        for sbp in spec_base_prices:
            if sbp.get("price", 0):
                result.append(
                    DocumentBasePrice(
                        city_id=sbp.get("city_id", ""),
                        price=sbp.get("price", 0),
                    ).to_dict()
                )
                cities_set.add(sbp.get("city_id"))

        
        if base_price.get("price", 0):
            for city_id in cities_dict.keys():
                if city_id not in cities_set:
                    result.append(
                        DocumentBasePrice(
                            city_id=city_id,
                            price=base_price.get("price", 0),
                        ).to_dict()
                    )
        return product_id, result

    product_base_price_dict = {}

    for i in range(0, len(product_ids), BATCH_SIZE):
        batch = product_ids[i:i + BATCH_SIZE]
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(process_single_product, pid): pid for pid in batch
            }
            for future in as_completed(futures):
                product_id, base_prices = future.result()
                product_base_price_dict[product_id] = base_prices

    # save

    DATA_FILE_PATH = f"/tmp/{DAG_ID}.product_base_price.json"
    try:
        with open(DATA_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(dict(product_base_price_dict), f, ensure_ascii=False)
    except IOError as e:
        raise Exception(f"Task failed: couldn't save file to {DATA_FILE_PATH}") from e
    
    logging.info(f"product_base_price data are saved: {len(product_base_price_dict)}")
    context["ti"].xcom_push(key="product_base_price_file_path", value=DATA_FILE_PATH)

def transform_final_price_callable(**context):
    product_ids = []
    subdivisions_dict = {}

    product_ids_file_path = context["ti"].xcom_pull(
        key="product_ids_file_path", task_ids="get_product_ids_task"
    )

    if not product_ids_file_path or not os.path.exists(product_ids_file_path):
        raise FileNotFoundError(f"File not found: {product_ids_file_path}")

    with open(product_ids_file_path, "r", encoding="utf-8") as f:
        product_ids = json.load(f)

    subdivisions_file_path = context["ti"].xcom_pull(
        key="subdivisions_file_path", task_ids="get_subdivision_task"
    )

    if not subdivisions_file_path or not os.path.exists(subdivisions_file_path):
        raise FileNotFoundError(f"File not found: {subdivisions_file_path}")

    with open(subdivisions_file_path, "r", encoding="utf-8") as f:
        subdivisions_dict = json.load(f)

    # !
    print(subdivisions_dict)
    print("###############")

    # do

    MAX_WORKERS = 5
    BATCH_SIZE = 100
    BASE_URL = "http://price.default"

    product_final_price_dict: Dict[str, List[Dict[str, Any]]] = {}

    def process_single_product(product_id: str) -> (str, List[Dict[str, Any]]):
        final_price = {}
        spec_final_prices = []

        try:
            response = requests.get(
                f"{BASE_URL}/final_price/{product_id}",
                timeout=10,
            )
            response.raise_for_status()
            final_price = response.json()
        except requests.RequestException as e:
            logging.warning(f"Failed to fetch final_price for product_id={product_id}: {e}")
            return product_id, []

        try:
            response = requests.get(
                f"{BASE_URL}/spec_final_price",
                params={
                    "list_params.page_size": 1000,
                    "product_id": product_id,
                },
                timeout=60,
            )
            response.raise_for_status()
            data = response.json()
            spec_final_prices = data.get("results", [])
        except requests.RequestException as e:
            logging.warning(f"Failed to fetch warehouse data for product_id={product_id}: {e}")
            return product_id, []
        
        subdivisions_set = set()
        result = []

        if final_price.get("price", 0):
            result.append(
                DocumentFinalPrice(
                    city_id=ASTANA_CITY_ID,
                    subdivision_id=ASTANA_OFFICE_SUBDIVISION_ID,
                    is_i_shop=True,
                    price=final_price.get("price", 0),
                ).to_dict()
            )
            subdivisions_set.add(ASTANA_OFFICE_SUBDIVISION_ID)

        for sfp in spec_final_prices:
            if sfp.get("price", 0) and subdivisions_dict.get(sfp.get("subdivision_id", "")):
                result.append(
                    DocumentFinalPrice(
                        city_id=sfp.get("city_id", ""),
                        subdivision_id=sfp.get("subdivision_id", ""),
                        is_i_shop=sfp.get("is_i_shop", False),
                        price=sfp.get("price", 0),
                    ).to_dict()
                )
                subdivisions_set.add(sfp.get("subdivision_id"))

        
        if final_price.get("price", 0):
            for sb_id, obj in subdivisions_dict.items():
                if "city_id" not in obj or not obj["city_id"]:
                        logging.warning(f"subdivision_id={sb_id} has missing city_id: {obj}")
                if sb_id not in subdivisions_set:
                    result.append(
                        DocumentFinalPrice(
                            city_id=obj.get("city_id", ""),
                            subdivision_id=sb_id,
                            is_i_shop=obj.get("is_i_shop", False),
                            price=final_price.get("price", 0),
                        ).to_dict()
                    )

        return product_id, result

    product_final_price_dict = {}

    for i in range(0, len(product_ids), BATCH_SIZE):
        batch = product_ids[i:i + BATCH_SIZE]
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(process_single_product, pid): pid for pid in batch
            }
            for future in as_completed(futures):
                product_id, final_prices = future.result()
                product_final_price_dict[product_id] = final_prices

    # save

    DATA_FILE_PATH = f"/tmp/{DAG_ID}.product_final_price.json"
    try:
        with open(DATA_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(dict(product_final_price_dict), f, ensure_ascii=False)
    except IOError as e:
        raise Exception(f"Task failed: couldn't save file to {DATA_FILE_PATH}") from e
    
    logging.info(f"product_final_price data are saved: {len(product_final_price_dict)}")
    context["ti"].xcom_push(key="product_final_price_file_path", value=DATA_FILE_PATH)

def load_base_price_callable(**context):
    file_path = context["ti"].xcom_pull(
        key="product_base_price_file_path", task_ids="transform_base_price_task"
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
    for product_id, base_price in product_warehouses_dict.items():
        actions.append({
            "_op_type": "update",
            "_index": INDEX_NAME,
            "_id": product_id,
            "retry_on_conflict": 3,
            "doc": { 
                "base_price": base_price 
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

def load_final_price_callable(**context):
    file_path = context["ti"].xcom_pull(
        key="product_final_price_file_path", task_ids="transform_final_price_task"
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
    for product_id, final_price in product_warehouses_dict.items():
        actions.append({
            "_op_type": "update",
            "_index": INDEX_NAME,
            "_id": product_id,
            "retry_on_conflict": 3,
            "doc": { 
                "final_price": final_price 
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
        key="cities_file_path", task_ids="get_city_task"
    )
    clean_tmp_file(file_path)

    file_path = context["ti"].xcom_pull(
        key="subdivisions_file_path", task_ids="get_subdivision_task"
    )
    clean_tmp_file(file_path)

    file_path = context["ti"].xcom_pull(
        key="product_base_price_file_path", task_ids="transform_base_price_task"
    )
    clean_tmp_file(file_path)

    file_path = context["ti"].xcom_pull(
        key="product_final_price_file_path", task_ids="transform_final_price_task"
    )
    clean_tmp_file(file_path)

# DAG 

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='DAG to upload product_price data from Price service to Elasticsearch index',
    start_date=datetime(2025, 5, 22, 0, 30),
    schedule="*/30 * * * *",
    catchup=False,
    tags=["nsi", "elasticsearch", "price"],
) as dag:
    get_product_ids = PythonOperator(
        task_id="get_product_ids_task",
        python_callable=get_product_ids_callable,
        provide_context=True,
    )

    get_city = PythonOperator(
        task_id="get_city_task",
        python_callable=get_city_callable,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    get_subdivision = PythonOperator(
        task_id="get_subdivision_task",
        python_callable=get_subdivision_callable,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    transform_base_price = PythonOperator(
        task_id="transform_base_price_task",
        python_callable=transform_base_price_callable,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    transform_final_price = PythonOperator(
        task_id="transform_final_price_task",
        python_callable=transform_final_price_callable,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    load_base_price = PythonOperator(
        task_id="load_base_price_task",
        python_callable=load_base_price_callable,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    load_final_price = PythonOperator(
        task_id="load_final_price_task",
        python_callable=load_final_price_callable,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    
    cleanup_temp_files = PythonOperator(
        task_id="cleanup_temp_files_task",
        python_callable=cleanup_temp_files_callable,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    get_product_ids >> [get_city, get_subdivision]
    get_city >> transform_base_price >> load_base_price
    get_subdivision >> transform_final_price >> load_final_price
    [load_base_price, load_final_price] >> cleanup_temp_files
