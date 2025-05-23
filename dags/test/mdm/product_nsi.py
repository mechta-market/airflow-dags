import json
import os
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook
from airflow.utils.trigger_rule import TriggerRule

from elasticsearch import helpers
from elasticsearch.helpers import BulkIndexError


DICTIONARY_NAME = "product"
DATA_FILE_PATH = f"/tmp/{DICTIONARY_NAME}_nsi.json"
INDEX_NAME = f"{DICTIONARY_NAME}_nsi"




def fetch_data_callable(**context):
    """Получаем все товары из NSI с многопоточностью и сохраняем в файл."""
    url = f"http://nsi.default/{DICTIONARY_NAME}"
    headers = {"Authorization": get_token()}
    page_size = 1000

    def fetch_page(page: int) -> list:
        resp = requests.get(
            url,
            params={
                "list_params.page": page,
                "list_params.page_size": page_size,
                "list_params.with_total_count": True,
            },
            headers=headers,
            timeout=10,
        )
        resp.raise_for_status()
        payload = resp.json()
        return payload.get("results", [])

    initial_response = requests.get(
        url,
        params={"list_params.only_count": True},
        headers=headers,
        timeout=10,
    )
    initial_response.raise_for_status()
    initial_payload = initial_response.json()
    total_count = int(initial_payload.get("pagination_info", {}).get("total_count", 0))
    total_pages = (total_count + page_size - 1) // page_size

    all_results = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(fetch_page, page): page for page in range(total_pages)
        }
        for future in as_completed(futures):
            try:
                all_results.extend(future.result())
            except Exception as e:
                print(f"Error loading page {futures[future]}: {e}")

    print(f"Fetched data: len={len(all_results)}")

    with open(DATA_FILE_PATH, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False)

    print(f"Data saved to {DATA_FILE_PATH}")
    context["ti"].xcom_push(key="data_file_path", value=DATA_FILE_PATH)


def upsert_to_es_callable(**context):
    """Загружаем данные в Elasticsearch пакетно."""
    file_path = context["ti"].xcom_pull(
        key="data_file_path", task_ids="fetch_data_task"
    )

    if not file_path or not os.path.exists(file_path):
        print("Data file not found.")
        return

    hosts = ["http://mdm.default:9200"]
    es_hook = ElasticsearchPythonHook(
        hosts=hosts,
        
    )
    client = es_hook.get_conn

    with open(file_path, "r", encoding="utf-8") as f:
        items = json.load(f)

    actions = [
        {
            "_op_type": "update",
            "_index": INDEX_NAME,
            "_id": item.get("id"),
            "doc": item,
            "doc_as_upsert": True,
        }
        for item in items
        if item.get("id")
    ]

    try:
        success, errors = helpers.bulk(
            client, actions, refresh="wait_for", stats_only=False
        )
        print(f"Successfully updated {success} documents.")
        if errors:
            print(f"Errors encountered: {errors}")
    except BulkIndexError as bulk_error:
        print(f"Bulk update failed: {bulk_error}")


def cleanup_temp_files_callable(**context):
    """Удаляет временный файл после загрузки."""
    file_path = context["ti"].xcom_pull(
        key="data_file_path", task_ids="fetch_data_task"
    )
    if file_path and os.path.exists(file_path):
        os.remove(file_path)
        print(f"Temporary file {file_path} removed.")
    else:
        print(f"File {file_path} does not exist.")


default_args = {
    "owner": "Amir",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="product_nsi",
    default_args=default_args,
    schedule_interval="*/60 * * * *",
    start_date=datetime(2025, 5, 14),
    catchup=False,
    tags=["nsi", "elasticsearch"],
) as dag:

    fetch_data = PythonOperator(
        task_id="fetch_data_task",
        python_callable=fetch_data_callable,
        provide_context=True,
    )

    upsert_to_es = PythonOperator(
        task_id="upsert_to_es_task",
        python_callable=upsert_to_es_callable,
        provide_context=True,
    )

    cleanup_temp_files = PythonOperator(
        task_id="cleanup_temp_files_task",
        python_callable=cleanup_temp_files_callable,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    fetch_data >> upsert_to_es >> cleanup_temp_files
