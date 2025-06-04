import json
import logging
import os
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

from filter.utils import (
    clean_tmp_file, 
    load_data_from_tmp_file, 
    save_data_to_tmp_file
)
from helpers.utils import elastic_conn


# DAG parameters

DAG_ID="product_price"
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

ERR_NO_ROWS = "err_no_rows"

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
    client = elastic_conn(Variable.get("elastic_scheme"))
    
    existing_ids_query = {
        "_source": False,
        "fields": ["_id"],
        "query": { "match_all": {} }
    }

    try:
        response = client.search(index=INDEX_NAME, body=existing_ids_query, size=10000, scroll="2m")
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

    save_data_to_tmp_file(context=context,
        xcom_key="product_ids_file_path",
        data=product_ids,
        file_path=f"/tmp/{DAG_ID}.product_ids.json",
    )
    logging.info(f"extracted product_ids count: {len(product_ids)}")


def get_city_callable(**context):
    BASE_URL = Variable.get("nsi_host")

    cities: List[dict] = []
    page = 0
    page_size = 1000

    def fetch_page(page: int) -> List[str]:
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
        return data.get("results", [])

    while True:
        try:
            results = fetch_page(page=page)
            if not results:
                break

            cities.extend([item for item in results if "id" in item])
            page += 1
        except Exception as e:
            logging.error(f"error during fetching cities: {e}")
            break

    cities_dict: Dict[str, dict] = {
        c["id"]: c
        for c in cities
        if c.get("id")
    }

    save_data_to_tmp_file(context=context,
        xcom_key="cities_file_path",
        data=cities_dict,
        file_path=f"/tmp/{DAG_ID}.city.json",
    )
    logging.info(f"extracted cities count: {len(cities_dict)}")


def get_subdivision_callable(**context):
    BASE_URL = Variable.get("nsi_host")

    subdivisions: List[dict] = []
    page = 0
    page_size = 1000

    def fetch_page(page: int) -> List[str]:
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
        return data.get("results", [])

    while True:
        try:
            results = fetch_page(page=page)
            if not results:
                break

            subdivisions.extend([item for item in results if "id" in item])
            page += 1
        except Exception as e:
            logging.error(f"error during fetching subdivisions: {e}")
            break

    subdivisions_dict: Dict[str, dict] = {
        s["id"]: s
        for s in subdivisions
        if s.get("id")
    }

    save_data_to_tmp_file(context=context,
        xcom_key="subdivisions_file_path",
        data=subdivisions_dict,
        file_path=f"/tmp/{DAG_ID}.subdivision.json",
    )
    logging.info(f"extracted subdivisions count: {len(subdivisions_dict)}")


def transform_base_price_callable(**context):
    product_ids = load_data_from_tmp_file(context=context, 
        xcom_key="product_ids_file_path",
        task_id="get_product_ids_task",
    )
    cities_dict = load_data_from_tmp_file(context=context, 
        xcom_key="cities_file_path",
        task_id="get_city_task",
    )

    MAX_WORKERS = 3
    BATCH_SIZE = 100
    BASE_URL = Variable.get("price_host")

    product_base_price_dict: Dict[str, List[Dict[str, Any]]] = {}

    def process_product_base_price(product_id: str) -> (str, List[Dict[str, Any]]):
        base_price = {}
        spec_base_prices = []

        # 1
        response = requests.get(
            f"{BASE_URL}/base_price/{product_id}",
            timeout=10,
        )
        if response.status_code == 400:
            if response.json().get("code") == ERR_NO_ROWS:
                return product_id, []
        
        response.raise_for_status()
        base_price = response.json()

        # 2
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
                executor.submit(process_product_base_price, pid): pid for pid in batch
            }
            for future in as_completed(futures):
                try:
                    product_id, base_prices = future.result()
                    product_base_price_dict[product_id] = base_prices
                except Exception as e:
                    logging.error(f"failed to process product_base_price: {e}")
                    raise

    save_data_to_tmp_file(context=context,
        xcom_key="product_base_price_file_path",
        data=dict(product_base_price_dict),
        file_path=f"/tmp/{DAG_ID}.product_base_price.json",
    )
    logging.info(f"transformed product_base_prices count: {len(product_base_price_dict)}")


def transform_final_price_callable(**context):
    product_ids = load_data_from_tmp_file(context=context, 
        xcom_key="product_ids_file_path",
        task_id="get_product_ids_task",
    )
    subdivisions_dict = load_data_from_tmp_file(context=context, 
        xcom_key="subdivisions_file_path",
        task_id="get_subdivision_task",
    )

    BASE_URL = Variable.get("price_host")
    MAX_WORKERS = 3
    BATCH_SIZE = 100

    product_final_price_dict: Dict[str, List[Dict[str, Any]]] = {}

    def process_product_final_price(product_id: str) -> (str, List[Dict[str, Any]]):
        final_price = {}
        spec_final_prices = []

        # 1
        response = requests.get(
            f"{BASE_URL}/final_price/{product_id}",
            timeout=10,
        )
        if response.status_code == 400:
            if response.json().get("code") == ERR_NO_ROWS:
                return product_id, []
        
        response.raise_for_status()
        final_price = response.json()

        # 2
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
        
        subdivisions_set = set()
        result = []

        if final_price.get("price", 0):
            result.append(
                DocumentFinalPrice(
                    subdivision_id=ASTANA_OFFICE_SUBDIVISION_ID,
                    price=final_price.get("price", 0),
                    city_id=ASTANA_CITY_ID,
                    is_i_shop=True,
                ).to_dict()
            )
            subdivisions_set.add(ASTANA_OFFICE_SUBDIVISION_ID)

        for sfp in spec_final_prices:
            if sfp.get("price", 0) and subdivisions_dict.get(sfp.get("subdivision_id", "")):
                result.append(
                    DocumentFinalPrice(
                        subdivision_id=sfp.get("subdivision_id", ""),
                        price=sfp.get("price", 0),
                        city_id=subdivisions_dict[sfp.get("subdivision_id")].get("city_id", ""),
                        is_i_shop=subdivisions_dict[sfp.get("subdivision_id")].get("is_i_shop", False),
                    ).to_dict()
                )
                subdivisions_set.add(sfp.get("subdivision_id"))

        if final_price.get("price", 0):
            for sb_id, obj in subdivisions_dict.items():
                if sb_id not in subdivisions_set:
                    result.append(
                        DocumentFinalPrice(
                            subdivision_id=sb_id,
                            price=final_price.get("price", 0),
                            city_id=obj.get("city_id", ""),
                            is_i_shop=obj.get("is_i_shop", False),
                        ).to_dict()
                    )

        return product_id, result

    product_final_price_dict = {}

    for i in range(0, len(product_ids), BATCH_SIZE):
        batch = product_ids[i:i + BATCH_SIZE]
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(process_product_final_price, pid): pid for pid in batch
            }
            for future in as_completed(futures):
                try:
                    product_id, final_prices = future.result()
                    product_final_price_dict[product_id] = final_prices
                except Exception as e:
                    logging.error(f"failed to process product_base_price: {e}")
                    raise

    save_data_to_tmp_file(context=context,
        xcom_key="product_final_price_file_path",
        data=dict(product_final_price_dict),
        file_path=f"/tmp/{DAG_ID}.product_final_price.json",
    )
    logging.info(f"transformed product_final_prices count: {len(product_final_price_dict)}")


def load_base_price_callable(**context):
    product_base_price_dict = load_data_from_tmp_file(
        context=context, 
        xcom_key="product_base_price_file_path",
        task_id="transform_base_price_task",
    )

    client = elastic_conn(Variable.get("elastic_scheme"))

    actions = []
    for product_id, base_price in product_base_price_dict.items():
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
        logging.info(f"update success, updated documents count: {success}")
        if errors:
            logging.error(f"errors encountered during bulk update: {errors}")
    except BulkIndexError as bulk_error:
        logging.error(f"bulk update failed: {bulk_error}")
        raise

def load_final_price_callable(**context):
    product_final_price_dict = load_data_from_tmp_file(
        context=context, 
        xcom_key="product_final_price_file_path",
        task_id="transform_final_price_task",
    )

    client = elastic_conn(Variable.get("elastic_scheme"))

    actions = []
    for product_id, final_price in product_final_price_dict.items():
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
        logging.info(f"update success, updated documents count: {success}")
        if errors:
            logging.error(f"errors encountered during bulk update: {errors}")
    except BulkIndexError as bulk_error:
        logging.error(f"bulk update failed: {bulk_error}")
        raise

def cleanup_temp_files_callable(**context):
    tmp_file_keys = [
        {"xcom_key": "product_ids_file_path", "task_id": "get_product_ids_task"},
        {"xcom_key": "cities_file_path", "task_id": "get_city_task"},
        {"xcom_key": "subdivisions_file_path", "task_id": "get_subdivision_task"},
        {"xcom_key": "product_base_price_file_path", "task_id": "transform_base_price_task"},
        {"xcom_key": "product_final_price_file_path", "task_id": "transform_final_price_task"},
    ]

    for tmp_file in tmp_file_keys:
        file_path = context["ti"].xcom_pull(
            key=tmp_file.get("xcom_key"), task_ids=tmp_file.get("task_id")
        )
        if file_path:
            clean_tmp_file(file_path)

# DAG 

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='DAG to upload product_price data from Price service to Elasticsearch index',
    start_date=datetime(2025, 5, 22),
    schedule="30 * * * *",
    max_active_runs=1,
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
