import requests
import json
import time
import os
import logging
from typing import List, Dict, Any
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    MAX_WORKERS = 7

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

################################################################################

DEFAULT_LANGUAGE = "ru"
TARGET_LANGUAGES = ["ru", "kz"]

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

        # Заполняем свойства из PropertyModel
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
                continue  # Пропускаем скрытые группы

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
                            unit = data.get("munit_i18n", {}).get(lang, "")
                            p["value"][lang] = f"{number} {unit}".strip()
                    elif attr_type in ("select", "multi-select"):
                        options = data.get("options", [])
                        default_val = val_i18n.get(DEFAULT_LANGUAGE, "")
                        for option in options:
                            if option.get("value") == default_val:
                                label_i18n = option.get("LabelI18n", {})
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

# task #2: Трансформация информации об каждом товаре в целевой формат и сохранить во временном локальном хранилище.
def transform_data_callable(**context):
    file_path = context["ti"].xcom_pull(
        key="extract_data_file_path", task_ids="extract_data_task"
    )

    # Load extracted data
    with open(file_path, "r", encoding="utf-8") as f:
        collected_products = json.load(f)

    TRANSFORM_DATA_FILE_PATH = f"/tmp/{DAG_ID}.transform_data.json"
    
    logging.info(f"Products count: {len(collected_products)}")

    # Transform data in parallel
    # with ThreadPoolExecutor(max_workers=8) as executor:
        # transformed_products = list(executor.map(encode_document_product, collected_products))

    transformed_products = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(encode_document_product, p) for p in collected_products]
        for future in as_completed(futures):
            try:
                transformed_products.append(future.result())
            except Exception as e:
                logging.error(f"Failed to process product: {e}")

    # Save collected products data.
    try:
        with open(TRANSFORM_DATA_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(transformed_products, f, ensure_ascii=False)
    except IOError as e:
        raise Exception(f"Task failed: couldn't save file to {TRANSFORM_DATA_FILE_PATH}") from e
    
    logging.info("Transformed data saved to file succesfully")
    context["ti"].xcom_push(key="transform_data_file_path", value=TRANSFORM_DATA_FILE_PATH)

#####################################################################################################################

# task #3: Сохранить информацию о товарах в Elasticsearch.
def load_data_callable(**context):
    # INDEX_NAME = Variable.get(f"{DAG_ID}.elastic_index", default_var="product_v1")

    pass


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

    file_path = context["ti"].xcom_pull(
        key="transform_data_file_path", task_ids="transform_data_task"
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

    extract_data >> transform_data >> load_data >> cleanup_temp_files
