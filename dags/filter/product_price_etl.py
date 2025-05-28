import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Any, Dict, List
from decimal import Decimal

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

ASTANT_CITY_ID = "cc4316f8-4333-11ea-a22d-005056b6dbd7"
ASTANA_OFFICE_SUBDIVISION_ID = "3bd9bf4f-7dd7-11e8-a213-005056b6dbd7"

# Configurations

logging.basicConfig(level=logging.INFO)

# Functions

class DocumentBasePrice:
    def __init__(self, city_id: str, price: Decimal):
        self.city_id = city_id
        self.price = price

    def to_dict(self):
        return {
            "city_id": self.city_id,
            "price": self.price,
        }

class DocumentFinalPrice:
    def __init__(self, city_id: str, subdivision_id: str, is_i_shop: bool, price: Decimal):
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
    logging.info("get_city task done")

def get_subdivision_callable(**context):
    logging.info("get_subdivision task done")

def transform_base_price_callable(**context):
    logging.info("transform_base_price task done")

def transform_final_price_callable(**context):
    logging.info("transform_final_price task done")

def load_base_price_callable(**context):
    logging.info("load_base_price task done")

def load_final_price_callable(**context):
    logging.info("load_final_price task done")

def cleanup_temp_files_callable(**context):
    file_path = context["ti"].xcom_pull(
        key="product_ids_file_path", task_ids="get_product_ids_task"
    )
    clean_tmp_file(file_path)

# DAG 

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='DAG to upload product_price data from Price service to Elasticsearch index',
    start_date=datetime(2025, 5, 22, 0, 5, 0, 30),
    schedule="*/60 * * * *",
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

    [get_city, get_subdivision] >> get_product_ids >>  [transform_base_price >> load_base_price, transform_final_price >> load_final_price] >> cleanup_temp_files
