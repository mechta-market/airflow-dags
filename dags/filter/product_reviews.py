import logging
import requests
import gzip
import csv
import tempfile
import os
import time
from typing import Any
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.hooks.base import BaseHook

from elasticsearch import helpers
from elasticsearch.helpers import BulkIndexError

from helpers.utils import elastic_conn, put_to_s3, get_from_s3

# ? выгружать только те данные, у которых были изменения.
# ? "search_options": {"filter": {"updated_at": {"gt": "YYYY-MM-DDThh:mm:ss+05:00"}}}

# ? "export_format": "[.id?, .reviews_count?, .rating?]"

# ? ежедневный лимит на длительность всех успешно завершенных экспортов = 60 минут.
# ? далее очередь с низким приоритетом и сброс в начале следующего дня.

# ? ограничение в коль-во строк / единиц контента = 500 000 строк за один экспорт.

DAG_ID = "product_reviews"
default_args = {
    "owner": "Olzhas",
    "depends_on_past": False,
}

INDEX_NAME = "product_test"

S3_FILE_NAME = f"{DAG_ID}/product_reviews.json"

# errors
ErrTokenNotFound = ValueError("connection: token not found")
ErrTaskIdNotFound = ValueError("task_id not found")


def request_aplaut(
    method="",
    endpoint="",
    headers={},
    body={},
) -> Any:
    # Authorization part:
    base_conn = BaseHook.get_connection("aplaut")
    token = base_conn.extra_dejson.get("token")
    if not token:
        raise ErrTokenNotFound

    aplaut_conn = HttpHook(http_conn_id="aplaut", method=method)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    response = {}

    try:
        response = aplaut_conn.run(
            endpoint=endpoint,
            headers=headers,
            data=body,
        )
    except Exception:
        logging.error(f"response={response.text}, exception={Exception}")
        raise

    return response


def fetch_data_callable():
    ## * Create task

    # task_id = ""
    # try:
    #     response = request_aplaut(
    #         method="POST",
    #         endpoint="/v4/export_tasks",
    #         body={
    #             "data": {
    #                 "type": "export_tasks",
    #                 "attributes": {
    #                     "records_type": "products",
    #                     "search_options": {},  # format request
    #                     "format": "csv",
    #                 },
    #             },
    #         },
    #     )

    #     task_id = response.json().get("data").get("id")
    # except Exception:
    #     raise

    # if not task_id:
    #     raise ErrTaskIdNotFound

    ## * Track task state and get file_url

    task_id = "68a87b53d3343f001c66b534"

    state = ""
    file_url = ""

    try_count = 0

    while True:
        response = {}

        try:
            response = request_aplaut(
                method="GET",
                endpoint=f"/v4/export_tasks/{task_id}",
            )
        except Exception:
            raise

        attributes = response.json().get("data").get("attributes")
        state = attributes.get("state")

        if not state:
            raise ValueError("test")

        if state == "":  # in progress?
            continue

        if state == "completed":
            file_url = attributes.get("archive_url")

            if file_url == "":  # if empty, debounce on one more try with wait time.
                if try_count < 1:
                    try_count = try_count + 1
                    time.sleep(5)
                    continue
                else:
                    raise ValueError("file_url is empty")

        break

    ## * Get file

    local_filename = "source_product_reviews.csv.gz"

    local_dir = tempfile.gettempdir()
    local_path = os.path.join(local_dir, local_filename)

    with requests.get(file_url, stream=True) as r:
        r.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    logging.info(f"stored={local_path}")

    source_data = []

    with gzip.open(local_path, mode="rt", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            source_data.append(
                {
                    "product_id": row.get("EXTERNAL ID"),
                    "reviews_count": row.get("REVIEWS COUNT"),
                    "rating": row.get("RATING"),
                }
            )

    logging.info(f"records count={len(source_data)}")

    put_to_s3(data=source_data, s3_key=S3_FILE_NAME)


# def delete_previous_data_callable():
#     # temp skip
#     logging.info("done")


def upsert_to_es_callable():
    source_data = get_from_s3(s3_key=S3_FILE_NAME)

    existing_ids_query = {
        "_source": False,
        "fields": ["_id"],
        "query": {"match_all": {}},
    }

    client = elastic_conn(Variable.get("elastic_scheme"))

    existing_ids = set()
    scroll_id = Any
    try:
        response = client.search(
            index=INDEX_NAME, body=existing_ids_query, size=5000, scroll="2m"
        )
        scroll_id = response["_scroll_id"]
        existing_ids = {hit["_id"] for hit in response["hits"]["hits"]}
        while len(response["hits"]["hits"]) > 0:
            response = client.scroll(scroll_id=scroll_id, scroll="2m")
            existing_ids.update(hit["_id"] for hit in response["hits"]["hits"])
    except Exception as e:
        logging.error(f"failed to fetch ids from Elasticsearch: {e}")
        raise
    finally:
        if scroll_id:
            client.clear_scroll(scroll_id=scroll_id)

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

    logging.info(f"source_data count={len(source_data)}")
    logging.info(f"existings_ids count={len(existing_ids)}")
    logging.info(f"actions count={len(actions)}")

    try:
        success, errors = helpers.bulk(
            client, actions, refresh="wait_for", stats_only=False
        )
        logging.info(f"update success, updated documents count={success}")
        if errors:
            logging.error(f"error during bulk update: {errors}")
    except Exception as bulk_error:
        logging.error(f"bulk update failed, error: {bulk_error}")
        raise


# def calculate_category_avg_reviews_callable():
#     logging.info("calculate it!")


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

    # delete_previous_data = PythonOperator(
    #     task_id="delete_previous_data_task",
    #     python_callable=delete_previous_data_callable,
    # )

    upsert_to_es = PythonOperator(
        task_id="upsert_to_es_task",
        python_callable=upsert_to_es_callable,
    )

    # calculate_category_avg_reviews = PythonOperator(
    #     task_id="calculate_category_avg_reviews_task",
    #     python_callable=calculate_category_avg_reviews_callable,
    # )

    fetch_data >> upsert_to_es  # >> delete_previous_data
