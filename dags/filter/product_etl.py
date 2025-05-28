import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any, Dict, List

import requests
from elasticsearch import helpers
from elasticsearch.helpers import BulkIndexError

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook
from airflow.utils.trigger_rule import TriggerRule

# DAG parameters
DAG_ID="product_etl"
default_args = {
    "owner": "Olzhas",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Constants
REQUEST_MAX_RETRIES=3
REQUEST_RETRY_DELAY=1
REQUEST_TIMEOUT=60

INDEX_NAME = "product_v1"

DEFAULT_LANGUAGE = "ru"
TARGET_LANGUAGES = ["ru", "kz"]

# Configurations
logging.basicConfig(level=logging.INFO)

# Functions
def fetch_with_retry(url: str, params=None, retries=REQUEST_MAX_RETRIES):
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.RequestException:
            if attempt < retries - 1:
                time.sleep(REQUEST_RETRY_DELAY * (attempt + 1))
            else:
                raise

def clean_tmp_file(file_path: str):
    if file_path == "":
        return

    if os.path.exists(file_path):
        os.remove(file_path)
        logging.info(f"Temporary file removed: {file_path}")
    else:
        logging.info(f"File does not exist: {file_path}")


class DocumentProduct:
    def __init__(self, p: dict):
        self.id = p.get("id", "")
        self.code = p.get("code", "")
        self.slug = p.get("slug", "")
        self.created_at = p.get("created_at", datetime.now())

        self.type = p.get("type", 0)
        self.service_type = p.get("service_type", 0)

        self.category_id = p.get("category_id", "")
        self.group_id = p.get("group_id", "")
        self.published = p.get("published", False)
        self.kbt = p.get("kbt", False)
        self.not_available_for_offline_sell = p.get("not_available_for_offline_sell", False)
        self.not_available_for_online_sell = p.get("not_available_for_online_sell", False)
        self.imei_track = p.get("imei_track", False)

        self.name_i18n = {lang: p.get("name_i18n", {}).get(lang, "") for lang in TARGET_LANGUAGES}
        self.description_i18n = {lang: p.get("description_i18n", {}).get(lang, "") for lang in TARGET_LANGUAGES}

        self.image_urls = p.get("image_urls", [])

        self.categories = self._parse_categories(p.get("breadcrumbs", []))
        self.pre_order = self._parse_preorder(p.get("pre_order", None))

        self.properties = self._parse_properties(p.get("property_model", {}))

    def _parse_categories(self, breadcrumbs):
        categories = []
        for cat in breadcrumbs:
            if not cat.get("id"):
                continue
            categories.append({
                "id": cat.get("id", ""),
                "slug": cat.get("slug", ""),
                "depth": cat.get("depth", 0),
                "name_i18n": {
                    "ru": cat.get("name_i18n", {}).get("ru", ""),
                    "kz": cat.get("name_i18n", {}).get("kz", ""),
                }
            })
        return categories

    def _parse_preorder(self, pre_order):
        if not pre_order:
            return None
        return {
            "prepayment_amount": pre_order.get("prepayment_amount", 0),
            "prepayment_percent": pre_order.get("prepayment_percent", 0),
            "active_from": pre_order.get("active_from"),
            "active_to": pre_order.get("active_to"),
            "sell_from": pre_order.get("sell_from"),
            "count": pre_order.get("count", 0),
        }

    def _parse_properties(self, property_model: dict) -> List[Dict[str, Any]]:
        properties = []
        ord_counter = 0

        groups = property_model.get("groups", [])
        for group in groups:
            flags = group.get("flags", {})
            if flags.get("hidden", False):
                continue

            attributes = group.get("attributes", [])
            for attr in attributes:
                ord_counter += 1

                if not attr.get("as_filter", False):
                    continue
                if not attr.get("slug"):
                    continue
                if not attr.get("value"):
                    continue

                slugs = attr["value"].get("slugs", [])
                values_i18n = attr["value"].get("values_i18n", [])  # Список словарей с локализацией

                for i, v_slug in enumerate(slugs):
                    if not v_slug:
                        continue
                    
                    p = {
                        "name_slug": attr.get("slug"),
                        "name": {},
                        "value_slug": v_slug,
                        "value": {},
                        "ord": ord_counter,
                    }

                    name = attr.get("name_i18n", {})
                    for lang in TARGET_LANGUAGES:
                        p["name"][lang] = name.get(lang, "")

                    attr_type = attr.get("type", "")
                    data = attr.get("data", {})
                    if i < len(values_i18n):
                        val_i18n = values_i18n[i]
                    else:
                        val_i18n = {}

                    if attr_type == "boolean":
                        default_val = val_i18n.get(DEFAULT_LANGUAGE, "")
                        if default_val == "true":
                            p["value"]["ru"] = "Да"
                            p["value"]["kz"] = "Иә"
                        else:
                            p["value"]["ru"] = "Нет"
                            p["value"]["kz"] = "Жоқ"
                    elif attr_type == "text":
                        for lang in TARGET_LANGUAGES:
                            p["value"][lang] = val_i18n.get(lang, "")
                    elif attr_type == "number":
                        number = val_i18n.get(DEFAULT_LANGUAGE, "")
                        for lang in TARGET_LANGUAGES:
                            unit = data.get("m_unit_i18n", {}).get(lang, "")
                            p["value"][lang] = f"{number} {unit}".strip()
                    elif attr_type in ("select", "multi-select"):
                        options = data.get("options", [])
                        default_val = val_i18n.get(DEFAULT_LANGUAGE, "")
                        for option in options:
                            if option.get("value") == default_val:
                                label_i18n = option.get("label_i18n", {})
                                for lang in TARGET_LANGUAGES:
                                    p["value"][lang] = label_i18n.get(lang, "")
                                break
                    else:
                        continue

                    properties.append(p)
        return properties

def encode_document_product(p: dict) -> dict:
    dp = DocumentProduct(p)
    return dp.__dict__


# task #1: Get raw product data from NSI service and store data in temporary storage.
def extract_data_callable(**context):
    BASE_URL = "http://nsi.default"
    MAX_WORKERS = 7
    EXTRACT_DATA_FILE_PATH = f"/tmp/{DAG_ID}.extract_data.json"

    # Get product_ids from nsi

    nsi_product_ids = []
    page_size = 1000

    initial_response = requests.get(
        f"{BASE_URL}/product",
        params={"list_params.only_count": True},
        timeout=REQUEST_TIMEOUT,
    )
    initial_response.raise_for_status()
    initial_payload = initial_response.json()
    total_count = int(initial_payload.get("pagination_info", {}).get("total_count", 0))
    total_pages = (total_count + page_size - 1) // page_size 

    def fetch_page(page: int):
        try:
            data = fetch_with_retry(
                f"{BASE_URL}/product",
                params={
                    "list_params.page": page,
                    "list_params.page_size": page_size,
                    "archived": False,
                },
            )
            return [item["id"] for item in data.get("results", []) if "id" in item]
        except Exception as e:
            logging.error(f"Error in processing page {page}: {e}")
            return []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_page = {
            executor.submit(fetch_page, page): page for page in range(total_pages)
        }

        for future in as_completed(future_to_page):
            page = future_to_page[future]
            try:
                ids = future.result()
                nsi_product_ids.extend(ids)
            except Exception as exc:
                logging.error(f"Error in processing page {page}: {exc}")

    # Get product details from nsi

    collected_products = []

    def fetch_product_details(id: str):
        url = f"{BASE_URL}/product/{id}"
        params = {
            "with_properties": True,
            "with_breadcrumbs": True,
            "with_image_urls": True,
            "with_pre_order": True,
        }

        try:
            return fetch_with_retry(url, params=params)
        except Exception as e:
            logging.error(f"fetch_product_details.fetch_with_retry: id: {id}, error: {e}")
            return None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_product_details, pid) for pid in nsi_product_ids]

        for future in as_completed(futures):
            result = future.result()
            if result:
                collected_products.append(result)

    # Save products data

    try:
        with open(EXTRACT_DATA_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(collected_products, f, ensure_ascii=False)
    except IOError as e:
        raise Exception(f"Task failed: couldn't save file to {EXTRACT_DATA_FILE_PATH}") from e
    
    logging.info(f"Products are extracted: {len(collected_products)}")
    context["ti"].xcom_push(key="extract_data_file_path", value=EXTRACT_DATA_FILE_PATH)


# task #2: Transform product data to a target format and store in temporary store.
def transform_data_callable(**context):
    MAX_WORKERS = 7
    TRANSFORM_DATA_FILE_PATH = f"/tmp/{DAG_ID}.transform_data.json"

    file_path = context["ti"].xcom_pull(
        key="extract_data_file_path", task_ids="extract_data_task"
    )

    with open(file_path, "r", encoding="utf-8") as f:
        collected_products = json.load(f)

    transformed_products = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(encode_document_product, p) for p in collected_products]
        for future in as_completed(futures):
            try:
                transformed_products.append(future.result())
            except Exception as e:
                logging.error(f"Failed to process product: {e}")

    try:
        with open(TRANSFORM_DATA_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(transformed_products, f, ensure_ascii=False)
    except IOError as e:
        raise Exception(f"Task failed: couldn't save file to {TRANSFORM_DATA_FILE_PATH}") from e
    
    logging.info(f"Products data are saved: {len(transformed_products)}")
    context["ti"].xcom_push(key="transform_data_file_path", value=TRANSFORM_DATA_FILE_PATH)


# task #3: Delete products in Elasticsearch that are not present in the transformed data.
def delete_different_data_callable(**context):
    file_path = context["ti"].xcom_pull(
        key="transform_data_file_path", task_ids="transform_data_task"
    )

    if not file_path or not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        transformed_products = json.load(f)
    
    hosts = ["http://mdm.default:9200"]
    es_hook = ElasticsearchPythonHook(
        hosts=hosts,
    )
    client = es_hook.get_conn

    existing_ids_query = {
        "query": {
            "match_all": {}
        },
        "_source": False,
        "fields": ["_id"]
    }

    try:
        response = client.search(index=INDEX_NAME, body=existing_ids_query, size=10000, scroll="2m")
        scroll_id = response["_scroll_id"]
        existing_ids = {hit["_id"] for hit in response["hits"]["hits"]}

        while len(response["hits"]["hits"]) > 0:
            response = client.scroll(scroll_id=scroll_id, scroll="2m")
            existing_ids.update(hit["_id"] for hit in response["hits"]["hits"])

    except Exception as e:
        logging.error(f"Failed to fetch existing IDs from Elasticsearch: {e}")
        return

    incoming_ids = {product.get("id") for product in transformed_products if product.get("id")}
    ids_to_delete = existing_ids - incoming_ids

    delete_actions = [
        {
            "_op_type": "delete",
            "_index": INDEX_NAME,
            "_id": product_id,
        }
        for product_id in ids_to_delete
    ]

    if delete_actions:
        try:
            success, errors = helpers.bulk(
                client, delete_actions, refresh="wait_for", stats_only=False
            )
            logging.info(f"Successfully deleted {success} documents.")
            if errors:
                logging.error(f"Errors encountered during bulk delete: {errors}")
        except BulkIndexError as bulk_error:
            raise Exception(f"Bulk delete failed: {bulk_error}")
    
    logging.info(f"Products are deleted: {len(ids_to_delete)}")


# task #4: Upload transformed product data to Elasticsearch.
def load_data_callable(**context):
    file_path = context["ti"].xcom_pull(
        key="transform_data_file_path", task_ids="transform_data_task"
    )

    if not file_path or not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        transformed_products = json.load(f)
    
    hosts = ["http://mdm.default:9200"]
    es_hook = ElasticsearchPythonHook(
        hosts=hosts,
    )
    client = es_hook.get_conn

    actions = [
        {
            "_op_type": "update",
            "_index": INDEX_NAME,
            "_id": product.get("id"),
            "doc": product,
            "doc_as_upsert": True,
        }
        for product in transformed_products
        if product.get("id")
    ]

    try:
        success, errors = helpers.bulk(
            client, actions, refresh="wait_for", stats_only=False
        )
        logging.info(f"Successfully updated {success} documents.")
        if errors:
            logging.error(f"Errors encountered during bulk update: {errors}")
    except BulkIndexError as bulk_error:
        raise Exception(f"Bulk update failed: {bulk_error}") 


# task #5: Delete temporary data.
def cleanup_temp_files_callable(**context):
    file_path = context["ti"].xcom_pull(
        key="extract_data_file_path", task_ids="extract_data_task"
    )
    clean_tmp_file(file_path)

    file_path = context["ti"].xcom_pull(
        key="transform_data_file_path", task_ids="transform_data_task"
    )
    clean_tmp_file(file_path)


# DAG initialization.
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='DAG to upload products data from NSI service to Elasticsearch index',
    start_date=datetime(2025, 5, 22),
    schedule="*/60 * * * *",
    catchup=False,
    tags=["nsi", "elasticsearch"],
) as dag:
    extract_data = PythonOperator(
        task_id="extract_data_task",
        python_callable=extract_data_callable,
        provide_context=True,
    )

    transform_data = PythonOperator(
        task_id="transform_data_task",
        python_callable=transform_data_callable,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    delete_different_data = PythonOperator(
        task_id="delete_different_data_task",
        python_callable=delete_different_data_callable,
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

    extract_data >> transform_data >> delete_different_data >> load_data >> cleanup_temp_files
