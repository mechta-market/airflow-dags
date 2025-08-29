import json
import logging
import time
import requests
from typing import Any

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook


ZERO_UUID = "00000000-0000-0000-0000-000000000000"
BUCKET_NAME = "airflow"
S3_CONN_ID = "s3"


def request_to_1c(host: str, dic_name: str) -> dict:
    url = f"{host}/send/by_db_name/AstOffice/getbaseinfo/{dic_name}"

    try:
        resp = requests.post(url, timeout=30)
        resp.raise_for_status()
    except Exception as e: 
        logging.error(f"request_to_1c error, response={resp.text[:2000]}")
        raise e

    return resp.json()


def request_to_1c_with_data(host: str, dic_name: str, payload) -> dict:
    url = f"{host}/send/by_db_name/AstOffice/getbaseinfo/{dic_name}"

    resp = requests.post(url, timeout=30, json=payload)
    resp.raise_for_status()
    return resp.json()


def normalize_zero_uuid_fields(item: dict, fields: list[str]) -> dict:
    """Заменяет ZERO_UUID на пустую строку в указанных полях."""
    for field in fields:
        if item.get(field) == ZERO_UUID:
            item[field] = ""
    return item


def request_to_site_api(host: str, endpoint: str) -> dict:
    """Отправляет запрос к API сайта и возвращает ответ в виде словаря."""
    url = f"{host}/{endpoint}"

    response = requests.get(url)
    response.raise_for_status()
    if not response.status_code < 300:
        logging.error(f"error code: {response.status_code}")
        return
    return response.json()


def request_to_nsi_api(host: str, endpoint: str) -> dict:
    """Отправляет запрос к API NSI и возвращает ответ в виде словаря."""
    url = f"{host}/{endpoint}"

    response = requests.get(url, timeout=30)
    response.raise_for_status()
    if not response.status_code < 300:
        logging.error(f"error code: {response.status_code}")
        return
    return response.json()


def elastic_conn(scheme: str) -> Any:
    hosts = [scheme]
    es_hook = ElasticsearchPythonHook(
        hosts=hosts,
    )
    return es_hook.get_conn


def put_to_s3(
    data: Any,
    s3_key: str,
):
    data_bytes = json.dumps(data, ensure_ascii=False).encode("utf-8")

    # Загружаем в S3
    s3 = S3Hook(aws_conn_id=S3_CONN_ID)
    s3.load_bytes(
        bytes_data=data_bytes, key=s3_key, bucket_name=BUCKET_NAME, replace=True
    )

    logging.info(f"data saved to s3://{BUCKET_NAME}/{s3_key}")


def get_from_s3(s3_key: str) -> Any:
    s3 = S3Hook(aws_conn_id=S3_CONN_ID)

    # Загружаем из S3
    file_obj = s3.get_key(key=s3_key, bucket_name=BUCKET_NAME)
    file_content = file_obj.get()["Body"].read()
    items = json.loads(file_content)
    return items


def fetch_with_retry(url: str, params=None, retries=3):
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            return response.json()
        except requests.RequestException:
            if attempt < retries - 1:
                time.sleep(1 * (attempt + 1))
            else:
                raise
