import os
import json
import logging
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook

from elasticsearch import helpers
from elasticsearch.helpers import BulkIndexError

DAG_ID = "product_actions"

INDEX_NAME = "product_v2"
DATA_FILE_PATH = f"/tmp/{DAG_ID}.product_action_site.json"


def fetch_data_callable(**context):
    url = f"{Variable.get("site_api_host")}/v2/airflow/product/action"
    page_size = 100

    def fetch_page(page: int) -> list:
        resp = requests.get(
            url,
            params={
                "page": page,
                "per_page": page_size,
            },
            timeout=10,
        )
        resp.raise_for_status()
        payload = resp.json()
        return payload.get("products", [])

    initial_response = requests.get(
        url,
        params={
                "page": 1,
                "per_page": page_size,
            },
        timeout=10,
    )
    initial_response.raise_for_status()
    initial_payload = initial_response.json()
    total_count = int(initial_payload.get("meta", {}).get("total", 0))
    # total_pages = int(initial_payload.get("meta", {}).get("last_page", 0))
    total_pages = (total_count + page_size - 1) // page_size
    logging.info(f"total_pages = {total_pages}")
    logging.info(f"initial_response data: len={len(initial_payload.get("products"))}, meta= {initial_payload.get("meta")}")

    all_results = initial_payload.get("products", [])
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(fetch_page, page): page
            for page in range(2, total_pages + 1)
        }
        for future in as_completed(futures):
            try:
                result = future.result()
                logging.info(f"Page {futures[future]} loaded with {len(result)} items")
                all_results.extend(result)
            except Exception as e:
                logging.error(f"Error loading page {futures[future]}: {e}")


    logging.info(f"Fetched data: len={len(all_results)}")
    with open(DATA_FILE_PATH, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False)

    logging.info(f"Data saved to {DATA_FILE_PATH}")
    context["ti"].xcom_push(key="data_file_path", value=DATA_FILE_PATH)


def delete_previous_data_callable(**context):
    file_path = context["ti"].xcom_pull(
        key="data_file_path", task_ids="fetch_data_task"
    )
    
    with open(file_path, "r", encoding="utf-8") as f:
        items = json.load(f)
    if not items:
        return
    
    incoming_ids = [item["id"] for item in items if item.get("id") is not None]

    
    
    hosts = ["http://mdm.default:9200"]
    es_hook = ElasticsearchPythonHook(
        hosts=hosts,
    )
    client = es_hook.get_conn

    existing_ids_query = {
        "query": {
            "nested": {
                "path": "actions",
                "query": {
                    "match_all": {}
                }
            }
        }
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
        raise
    finally:
        if scroll_id:
            client.clear_scroll(scroll_id=scroll_id)

    
    logging.info(f"existing_ids len = {len(existing_ids)}")
    logging.info(f"incoming_ids len = {len(incoming_ids)}")

    ids_to_delete = existing_ids - set(incoming_ids)

    ids_to_delete = existing_ids
    actions = [
        {
            "_op_type": "update",
            "_index": INDEX_NAME,
            "_id": doc_id,
            "doc": {"actions": []}
        }
        for doc_id in ids_to_delete
    ]
    
    try:
        success, errors = helpers.bulk(
            client, actions, refresh="wait_for", stats_only=False
        )
        logging.info(f"delete success, deleted document count: {success}")
        if errors:
            logging.error(f"error during bulk delete: {errors}")
    except Exception as bulk_error:
            logging.error(f"bulk delete failed, error: {bulk_error}")
            raise
        
    logging.info(f"Updated {len(ids_to_delete)} fields to empty actions")

    
    
def upsert_to_es_callable(**context):
    file_path = context["ti"].xcom_pull(
        key="data_file_path", task_ids="fetch_data_task"
    )

    if not file_path or not os.path.exists(file_path):
        logging.info("Data file not found.")
        return

    with open(file_path, "r", encoding="utf-8") as f:
        items = json.load(f)

    if not items:
        return
    logging.info(f"items LEN={len(items)}")

    hosts = ["http://mdm.default:9200"]
    es_hook = ElasticsearchPythonHook(hosts=hosts)
    client = es_hook.get_conn
    
    actions = [
        {
            "_op_type": "update",
            "_index": INDEX_NAME,
            "_id": item.get("id"),
            "doc": item,
        }
        for item in items
        if item.get("id")
    ]
    logging.info(f"ACTIONS COUNT {len(actions)}.")

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
    "owner": "Sultan",
    "depends_on_past": False,
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval="0 * * * *",
    start_date=datetime(2025, 5, 27),
    catchup=False,
    tags=["elasticsearch", "site", "product"],
    description = "Этот DAG переносит информацию о акциях заполняемых на сайте в Elasticearch"
) as dag:

    fetch_data = PythonOperator(
        task_id="fetch_data_task",
        python_callable=fetch_data_callable,
        provide_context=True,
    )
    
    delete_previous_data = PythonOperator(
        task_id="delete_previous_data_task",
        python_callable=delete_previous_data_callable,
        provide_context=True,
    )
    
    upsert_to_es = PythonOperator(
        task_id="upsert_to_es_task",
        python_callable=upsert_to_es_callable,
        provide_context=True,
    )

    fetch_data >> delete_previous_data >> upsert_to_es
