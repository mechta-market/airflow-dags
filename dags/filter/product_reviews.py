import logging
import requests
from typing import Any
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook
from airflow.providers.http.hooks.http import HttpHook

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

INDEX_NAME = ""  # "product_v2"

S3_FILE_NAME = f"{DAG_ID}/product_reviews.json"


def fetch_data_callable():
    # create task and check the status.
    # if status=completed, get the file.
    # save the file to s3.

    aplaut_conn = HttpHook(http_conn_id="aplaut")
    token = aplaut_conn.get_connection("aplaut").extra_dejson.get("token")
    headers = {"Authorization": f"Bearer {token}"}

    try:
        response = aplaut_conn.run(
            extra_options={"method": "GET"},
            endpoint="/v4/export_tasks/68a87b53d3343f001c66b534", 
            headers=headers)
        response.raise_for_status()
        logging.info(f"success: {response}")
    except Exception:  
        logging.error(f"fail: {response}")

    logging.info("done")


# def delete_previous_data_callable():
#     logging.info("done")


# def upsert_to_es_callable():
#     logging.info("done")


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

    # upsert_to_es = PythonOperator(
    #     task_id="upsert_to_es_task",
    #     python_callable=upsert_to_es_callable,
    # )

    # calculate_category_avg_reviews = PythonOperator(
    #     task_id="calculate_category_avg_reviews_task",
    #     python_callable=calculate_category_avg_reviews_callable,
    # )

    fetch_data # >> delete_previous_data >> upsert_to_es
