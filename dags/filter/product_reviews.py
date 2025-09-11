import csv
import gzip
import logging
import os
import requests
import tempfile
import time
from typing import Any, Dict, List, Set
from datetime import datetime

from airflow.hooks.base import BaseHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Variable

from elasticsearch import helpers

from helpers.utils import elastic_conn, put_to_s3, get_from_s3, parse_int, parse_float


# DAG parameters

DAG_ID = "product_reviews"
default_args = {
    "owner": "Olzhas",
    "depends_on_past": False,
}

# Constants

INDEX_NAME = "product_test"
S3_FILE_NAME = f"{DAG_ID}/product_reviews.json"

# Errors

ErrTokenNotFound = ValueError("connection: token not found")
ErrTaskIdNotFound = ValueError("task_id not found")
ErrUndefinedTaskState = ValueError("undefined task state")
ErrFileUrlEmpty = ValueError("file_url empty")
ErrServiceNA = ValueError("service not available")

# Functions


class AplautClient:
    def __init__(self):
        self.__conn_id = "aplaut"
        self.__token = self.__get_token()

    def __get_token(self) -> str:
        base_conn = BaseHook.get_connection(self.__conn_id)
        token = base_conn.extra_dejson.get("token")
        if not token:
            raise ErrTokenNotFound
        return token

    def request(self, method: str, endpoint: str, body: Dict = None) -> Any:
        http_hook = HttpHook(http_conn_id=self.__conn_id, method=method)

        headers = {
            "Authorization": f"Bearer {self.__token}",
            "Content-Type": "application/json",
        }

        response = None

        try:
            response = http_hook.run(
                endpoint=endpoint,
                headers=headers,
                json=body,
                # extra_options={"timeout": 60},
            )
        except Exception as e:
            if response:
                logging.error(f"response={response.text}")
            logging.error(f"aplaut api request failed: exception={e}")
            raise

        return response


class ElasticsearchClient:
    def __init__(self):
        self.client = self.__get_elastic_client()

    def __get_elastic_client(self):
        scheme = Variable.get("elastic_scheme")
        return elastic_conn(scheme)

    def get_existing_product_ids(self, index: str, batch_size: int = 5000) -> Set[str]:
        existing_ids = set()
        request_body = {
            "_source": False,
            "query": {"match_all": {}},
        }

        scroll_id = None

        try:
            response = self.client.search(
                index=index, body=request_body, size=batch_size, scroll="2m"
            )
            scroll_id = response["_scroll_id"]
            existing_ids = {hit["_id"] for hit in response["hits"]["hits"]}
            while len(response["hits"]["hits"]) > 0:
                response = self.client.scroll(scroll_id=scroll_id, scroll="2m")
                existing_ids.update(hit["_id"] for hit in response["hits"]["hits"])
        except Exception as e:
            logging.error(f"failed to fetch ids from Elasticsearch: {e}")
            raise
        finally:
            if scroll_id:
                self.client.clear_scroll(scroll_id=scroll_id)

        return existing_ids

    def bulk_update_records(self, actions: List[Dict]):
        try:
            success, errors = helpers.bulk(
                self.client, actions, refresh="wait_for", stats_only=False
            )
            logging.info(f"update success, updated documents count={success}")
            if errors:
                logging.error(f"error during bulk update: {errors}")
        except Exception as bulk_error:
            logging.error(f"bulk update failed, error: {bulk_error}")
            raise


def download_file(url: str, local_path: str) -> None:
    try:
        with requests.get(url, stream=True, timeout=300) as response:
            response.raise_for_status()
            with open(local_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)
        logging.info(f"File downloaded successfully: {local_path}")
    except Exception as e:
        logging.error(f"File download failed: {e}")
        raise


def process_csv_file(file_path: str) -> List[Dict]:
    result = []

    try:
        with gzip.open(file_path, mode="rt", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                result.append(
                    {
                        "product_id": row.get("EXTERNAL ID", "").strip(),
                        "reviews_count": parse_int(row.get("REVIEWS COUNT", 0)),
                        "rating": parse_float(row.get("RATING", 0)),
                    }
                )

        logging.info(f"processed records count={len(result)}")
        return result

    except Exception as e:
        logging.error(f"CSV processing failed: {e}")
        raise


def fetch_data_callable():

    MAX_RETRIES = 15
    POLLING_INTERVAL = 120
    MAX_DEBOUNCE = 1
    DEBOUNCE_WAIT = 30

    aplaut_client = AplautClient()

    ## * Create task

    task_id = ""
    try:
        body = {
            "data": {
                "type": "export_tasks",
                "attributes": {
                    "records_type": "products",
                    "search_options": {},
                    "format": "csv",
                },
            }
        }

        response = aplaut_client.request(
            method="POST",
            endpoint="/v4/export_tasks",
            data=body,
        )
        task_id = response.json().get("data", {}).get("id")

    except Exception as e:
        logging.error(f"failed to create export task: {e}")
        raise

    if not task_id:
        raise ErrTaskIdNotFound

    ## * Track task state and get file_url

    logging.info(f"fetched task_id={task_id}")

    retry_count = 0
    debounce_count = 0
    file_url = None

    while retry_count < MAX_RETRIES:
        response = {}

        try:
            response = aplaut_client.request(
                method="GET",
                endpoint=f"/v4/export_tasks/{task_id}",
            )
        except Exception:
            raise

        attributes = response.json().get("data", {}).get("attributes", {})
        state = attributes.get("state", "")

        if not state:
            raise ErrUndefinedTaskState

        logging.info(f"tracking state: retry_count={retry_count} state={state}")

        if state in ("waiting", "processing", "suspended"):
            time.sleep(POLLING_INTERVAL)
            retry_count += 1
            continue

        if state == "completed":
            file_url = attributes.get("archive_url", "")

            if file_url:
                # success
                break
            elif debounce_count < MAX_DEBOUNCE:
                # suggested to debounce one more time,
                # if request state="success" and no url provided.
                debounce_count += 1
                time.sleep(DEBOUNCE_WAIT)
                continue
            else:
                raise ErrFileUrlEmpty

        logging.error(f"undefined state={state}")
        raise ErrUndefinedTaskState

    if not file_url:
        logging.error(f"source service didn't provide file_url")
        raise ErrServiceNA

    ## * File work

    local_filename = "aplaud_product_reviews.csv.gz"
    local_dir = tempfile.gettempdir()
    local_path = os.path.join(local_dir, local_filename)

    download_file(file_url, local_path)
    source_data = process_csv_file(local_path)

    put_to_s3(data=source_data, s3_key=S3_FILE_NAME)


def upsert_to_es_callable():
    es_client = ElasticsearchClient()

    source_data = get_from_s3(s3_key=S3_FILE_NAME)
    logging.info(f"source_data count={len(source_data)}")

    existing_ids = es_client.get_existing_product_ids(INDEX_NAME)
    logging.info(f"existings_ids count={len(existing_ids)}")

    actions = [
        {
            "_op_type": "update",
            "_index": INDEX_NAME,
            "_id": record.get("product_id"),
            "doc": {
                "site_data": {
                    "reviews_count": record.get("reviews_count"),
                    "rating": record.get("rating"),
                }
            },
            "retry_on_conflict": 3,
        }
        for record in source_data
        if record.get("product_id") and record.get("product_id") in existing_ids
    ]
    logging.info(f"actions count={len(actions)}")

    if actions:
        es_client.bulk_update_records(actions)
    else:
        logging.info("no records to update")


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule="0 0 * * *",
    start_date=datetime(2025, 8, 22),
    catchup=False,
    tags=["elasticsearch", "site", "product"],
    description="The DAG uploads product reviews data to Elasticsearch",
) as dag:
    fetch_data = PythonOperator(
        task_id="fetch_data_task",
        python_callable=fetch_data_callable,
    )

    upsert_to_es = PythonOperator(
        task_id="upsert_to_es_task",
        python_callable=upsert_to_es_callable,
    )

    fetch_data >> upsert_to_es
