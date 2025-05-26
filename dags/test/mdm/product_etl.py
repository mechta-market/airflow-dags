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
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "Olzhas",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

logging.basicConfig(level=logging.INFO)

DAG_ID="product_etl"

EXTRACT_DATA_FILE_PATH = f"/tmp/{DAG_ID}.extract_data.json"
INDEX_NAME = Variable.get(f"{DAG_ID}.elastic_index", default_var="product_v1")


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

    # Fetch required product_ids

    def fetch_product_ids():
        product_ids = []
        page = 0
        page_size = 1000

        while True:
            try:
                data = fetch_with_retry(
                    f"{BASE_URL}/product",
                    params={
                        "list_params.page": page,
                        "list_params.page_size": page_size,
                        "archived": False,
                    }
                )

                batch = data.get("results", [])
                if not batch:
                    break

                batch_ids = [item["id"] for item in batch if "id" in item]
                product_ids.extend(batch_ids)
                page += 1

            except Exception as e:
                logging.error(f"Error fetching page: {page}, error: {e}")
                break
        
        return product_ids
    
    nsi_product_ids = fetch_product_ids()

    # Fetch product details data.

    collected_products = []
    lock = Lock()
    MAX_WORKERS = 3

    def fetch_product_details(id: str, lock, collected_products):
        url = f"{BASE_URL}/product/{id}"
        params = {
            "with_properties": True,
            "with_breadcrumbs": True,
            "with_image_urls": True,
            "with_pre_order": True,
        }

        try:
            data = fetch_with_retry(url, params=params)
            with lock:
                collected_products.append(data)
        except Exception as e:
            logging.error(f"fetch_product_details.fetch_with_retry: id: {id}, error: {e}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_product_details, pid, lock, collected_products) for pid in nsi_product_ids]
        for future in as_completed(futures):
            pass

    logging.info(f"Products are extracted: {len(collected_products)}")

    # Save collected products data.
    try:
        with open(EXTRACT_DATA_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(collected_products, f, ensure_ascii=False, indent=2)
    except IOError as e:
        logging.error(f"Error saving file: {e}")
    
    logging.info("Extracted data saved to file succesfully")
    context["ti"].xcom_push(key="extract_data_file_path", value=EXTRACT_DATA_FILE_PATH)


# task #2: Трансформация информации об каждом товаре в целевой формат и сохранить во временном локальном хранилище.
def transform_data_callable(**context):
    # Load extracted data
    with open(EXTRACT_DATA_FILE_PATH, "r", encoding="utf-8") as f:
        items = json.load(f)

    print(len(items))

    # Transform data

    # Save data to temporary storage


# task #3: Сохранить информацию о товарах в Elasticsearch.
# def load_data_callable(file_path: str):
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
