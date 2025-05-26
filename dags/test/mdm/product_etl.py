import requests
import json
import time
import os
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "Olzhas",
    "depends_on_past": False,
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
}

logging.basicConfig(level=logging.INFO)

DAG_ID="product_etl"

MAX_RETRIES = 3
RETRY_DELAY = 1
REQUEST_TIMEOUT = 60

def fetch_with_retry(url: str, params=None, retries=MAX_RETRIES):
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.RequestException:
            if attempt < retries - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                raise

# task #1: Get raw product data from NSI service and store in temporary storage.
def extract_data_callable(**context):
    BASE_URL = "http://nsi.default"
    MAX_WORKERS = 3

    EXTRACT_DATA_FILE_PATH = f"/tmp/{DAG_ID}.extract_data.json"

    # Fetch required product_ids

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
    total_pages = (total_count + page_size - 1)

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
            logging.error(f"Ошибка при получении страницы {page}: {e}")
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

    # Fetch product details data.

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

    logging.info(f"Products are extracted: {len(collected_products)}")

    # Save collected products data.
    try:
        with open(EXTRACT_DATA_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(collected_products, f, ensure_ascii=False)
    except IOError as e:
        raise Exception(f"Task failed: couldn't save file to {EXTRACT_DATA_FILE_PATH}") from e
    
    logging.info("Extracted data saved to file succesfully")
    context["ti"].xcom_push(key="extract_data_file_path", value=EXTRACT_DATA_FILE_PATH)


# task #2: Трансформация информации об каждом товаре в целевой формат и сохранить во временном локальном хранилище.
def transform_data_callable(**context):
    file_path = context["ti"].xcom_pull(
        key="extract_data_file_path", task_ids="extract_data_task"
    )

    # Load extracted data
    with open(file_path, "r", encoding="utf-8") as f:
        items = json.load(f)

    logging.info(f"Products count: {len(items)}")

    # Transform data

    # Save data to temporary storage


# task #3: Сохранить информацию о товарах в Elasticsearch.
# def load_data_callable(file_path: str):
#     INDEX_NAME = Variable.get(f"{DAG_ID}.elastic_index", default_var="product_v1")
#
#     pass


def clean_tmp_file(file_path: str):
    if file_path == "":
        return

    if os.path.exists(file_path):
        os.remove(file_path)
        logging.info(f"Temporary file removed: {file_path}")
    else:
        logging.info(f"File does not exist: {file_path}")


# task #4: Удалить временные данные.
def cleanup_temp_files_callable(**context):
    file_path = context["ti"].xcom_pull(
        key="extract_data_file_path", task_ids="extract_data_task"
    )
    clean_tmp_file(file_path)


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='DAG to upload product info from NSI service to Elasticsearch index',
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
    
    cleanup_temp_files = PythonOperator(
        task_id="cleanup_temp_files_task",
        python_callable=cleanup_temp_files_callable,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    extract_data >> transform_data >> cleanup_temp_files
