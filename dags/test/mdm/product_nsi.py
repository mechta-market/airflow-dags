import json
import os
import requests
import pendulum
from concurrent.futures import ThreadPoolExecutor, as_completed
from airflow import Variable
from airflow.decorators import dag, task
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook
from elasticsearch import helpers
from elasticsearch.helpers import BulkIndexError

DATA_FILE_PATH = "/tmp/product_nsi.json"
DICTIONARY_NAME = "product"
INDEX_NAME = f"{DICTIONARY_NAME}_nsi"


@dag(
    dag_id="product_nsi",
    schedule="*/60 * * * *",
    start_date=pendulum.datetime(2025, 5, 14, tz="UTC"),
    catchup=False,
    tags=["nsi", "elasticsearch"],
)
def nsi_to_es():

    @task()
    def fetch_data() -> str:
        """Получаем все товары из NSI с многопоточностью."""
        url = f"https://api.mdev.kz/nsi/{DICTIONARY_NAME}"
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

        # Первый запрос для определения общего количества страниц
        initial_response = requests.get(
            url,
            params={
                "list_params.only_count": True,
            },
            headers=headers,
            timeout=10,
        )
        initial_response.raise_for_status()
        initial_payload = initial_response.json()
        total_count = int(
            initial_payload.get("pagination_info", {}).get("total_count", 0)
        )
        total_pages = (total_count + page_size - 1) // page_size

        all_results = []

        # Параллельная загрузка страниц
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(fetch_page, page): page for page in range(total_pages)
            }
            for future in as_completed(futures):
                try:
                    all_results.extend(future.result())
                except Exception as e:
                    print(f"Error loading the page {futures[future]}: {e}")

        print(f"fetched data: len={len(all_results)}")

        # Сохранение данных в файл
        with open(DATA_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(all_results, f, ensure_ascii=False)

        print(f"Data saved to {DATA_FILE_PATH}")
        return DATA_FILE_PATH

    @task()
    def upsert_to_es(file_path: str):
        """Пакетная загрузка данных в Elasticsearch."""
        hosts = ["https://mdm.zeon.mdev.kz"]
        es_hook = ElasticsearchPythonHook(
            hosts=hosts,
            es_conn_args={"basic_auth": ("mdm", get_elasticsearch_password())},
        )
        client = es_hook.get_conn

        # Загрузка данных из файла
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

    @task()
    def cleanup_temp_files(file_path: str):
        """Удаляет временный файл после использования."""
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"Temporary file {file_path} removed.")
        else:
            print(f"File {file_path} does not exist.")

    data_file = fetch_data()
    upsert_to_es(data_file) >> cleanup_temp_files(data_file)


dag = nsi_to_es()


def get_token() -> str:
    access_token = Variable.get("access_token")
    return f"Bearer {access_token}"


def get_elasticsearch_password() -> str:
    el_password = Variable.get("elasticsearch_password")
    return el_password
