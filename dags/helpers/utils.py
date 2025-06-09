import logging
import requests
from typing import Any

from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook


ZERO_UUID = "00000000-0000-0000-0000-000000000000"


def request_to_1c(host: str, dic_name: str) -> dict:
    url = f"{host}/send/by_db_name/AstOffice/getbaseinfo/{dic_name}"

    resp = requests.post(url, timeout=30)
    resp.raise_for_status()
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
        logging.error(f"ERROR_CODE: {response.status_code}")
        return
    return response.json()


def request_to_nsi_api(host: str, endpoint: str) -> dict:
    """Отправляет запрос к API NSI и возвращает ответ в виде словаря."""
    url = f"{host}/{endpoint}"

    response = requests.get(url, timeout=30)
    response.raise_for_status()
    if not response.status_code < 300:
        logging.error(f"ERROR_CODE: {response.status_code}")
        return
    return response.json()


def elastic_conn(scheme: str) -> Any:
    hosts = [scheme]
    es_hook = ElasticsearchPythonHook(
        hosts=hosts,
    )
    return es_hook.get_conn
