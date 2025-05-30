import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
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
DAG_ID = "product_properties"
default_args = {
    "owner": "Amir",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Constants

INDEX_NAME = "product_test_v1"

DEFAULT_LANGUAGE = "ru"
TARGET_LANGUAGES = ["ru", "kz"]


# Functions
def transform_properties(product: dict) -> Dict[str, Any]:
    grps = []
    groups = product.get("property_model", {}).get("groups", [])
    for group in groups:
        attributes = []
        for attribute in group.get("attributes"):
            attribute_data = attribute.get("data")
            attribute_value = attribute.get("value")
            label_map = {
                option.get("label"): {
                    "label_i18n": option.get("label_i18n", {}),
                    "m_unit_i18n": attribute_data.get("m_unit_i18n"),
                }
                for option in attribute_data.get("options", [])
            }

            values = []
            for slug, v in zip(
                attribute_value.get("slugs"), attribute_value.get("values_i18n")
            ):
                option = label_map.get(v.get("ru"), {})
                label_i18n = (
                    option.get("label_i18n") or v
                )  # если label_i18n None или ""
                m_unit_i18n = attribute_data.get("m_unit_i18n")
                values.append(
                    {
                        "slug": slug,
                        "label_i18n": label_i18n,
                        "m_unit_i18n": m_unit_i18n,
                    }
                )

            attributes.append(
                {
                    "id": attribute.get("id"),
                    "name_i18n": attribute.get("name_i18n"),
                    "type": attribute.get("type"),
                    "ord": attribute.get("ord"),
                    "values": values,
                    "flags": attribute.get("flags"),
                    "slug": attribute.get("slug"),
                }
            )

        grps.append(
            {
                "id": group.get("id"),
                "name_i18n": group.get("name_i18n"),
                "ord": group.get("ord"),
                "attributes": attributes,
            }
        )
    p = {product.get("id"): grps}
    logging.info(f"product {p}")
    return p


# Tasks


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
        timeout=10,
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
            "no_description": True,
        }

        try:
            return fetch_with_retry(url, params=params)
        except Exception as e:
            logging.error(
                f"fetch_product_details.fetch_with_retry: id: {id}, error: {e}"
            )
            return None

    # with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    #     futures = [
    #         executor.submit(fetch_product_details, pid) for pid in nsi_product_ids
    #     ]

    #     for future in as_completed(futures):
    #         result = future.result()
    #         if result:
    #             collected_products.append(result)

    product = fetch_product_details(nsi_product_ids[0])
    collected_products = [product]
    # Save products data

    try:
        with open(EXTRACT_DATA_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(collected_products, f, ensure_ascii=False)
    except IOError as e:
        raise Exception(
            f"Task failed: couldn't save file to {EXTRACT_DATA_FILE_PATH}"
        ) from e

    logging.info(f"Products are extracted: {len(collected_products)}")
    context["ti"].xcom_push(key="extract_data_file_path", value=EXTRACT_DATA_FILE_PATH)


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
        futures = [executor.submit(transform_properties, p) for p in collected_products]
        for future in as_completed(futures):
            try:
                transformed_products.append(future.result())
            except Exception as e:
                logging.error(f"Failed to process product: {e}")

    try:
        with open(TRANSFORM_DATA_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(transformed_products, f, ensure_ascii=False)
    except IOError as e:
        raise Exception(
            f"Task failed: couldn't save file to {TRANSFORM_DATA_FILE_PATH}"
        ) from e

    logging.info(f"Products data are saved: {len(transformed_products)}")
    context["ti"].xcom_push(
        key="transform_data_file_path", value=TRANSFORM_DATA_FILE_PATH
    )


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


# def cleanup_temp_files_callable(**context):
#     file_path = context["ti"].xcom_pull(
#         key="extract_data_file_path", task_ids="extract_data_task"
#     )
#     clean_tmp_file(file_path)

#     file_path = context["ti"].xcom_pull(
#         key="transform_data_file_path", task_ids="transform_data_task"
#     )
#     clean_tmp_file(file_path)


# Dag

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="DAG to upload products data from NSI service to Elasticsearch index",
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

    load_data = PythonOperator(
        task_id="load_data_task",
        python_callable=load_data_callable,
        provide_context=True,
    )

    # cleanup_temp_files = PythonOperator(
    #     task_id="cleanup_temp_files_task",
    #     python_callable=cleanup_temp_files_callable,
    #     provide_context=True,
    #     trigger_rule=TriggerRule.ALL_DONE,
    # )

    (extract_data >> transform_data)
