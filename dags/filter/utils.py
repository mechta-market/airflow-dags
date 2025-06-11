import json
import time
import os
import logging
from typing import Any

from airflow.utils.state import State

import requests

# Constants

REQUEST_MAX_RETRIES=3
REQUEST_RETRY_DELAY=1
REQUEST_TIMEOUT=60

# Functions

def fetch_with_retry(url: str, params=None, retries=REQUEST_MAX_RETRIES):
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.RequestException:
            if attempt < retries - 1:
                time.sleep(REQUEST_RETRY_DELAY * (attempt + 1))
            else:
                raise

def clean_tmp_file(file_path: str):
    if file_path == "" or file_path is None:
        return

    if os.path.exists(file_path):
        os.remove(file_path)
        logging.info(f"clean_tmp_file: file removed: {file_path}")
    else:
        logging.info(f"clean_tmp_file: file not found: {file_path}")

def load_data_from_tmp_file(context, xcom_key: str, task_id: str) -> Any:
    file_path = context["ti"].xcom_pull(key=xcom_key, task_ids=task_id)

    if not file_path or not os.path.exists(file_path):
        raise FileNotFoundError(f"file not found: {file_path}")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logging.error(f"couldn't load data from {file_path}, error: {e}")
        raise

    return data

def save_data_to_tmp_file(context, xcom_key:str, data: Any, file_path: str):
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception as e:
        logging.error(f"couldn't save data to {file_path}, error: {e}")
        raise

    context["ti"].xcom_push(key=xcom_key, value=file_path)

def check_errors_callable(**context):
    dag_run = context["dag_run"]

    failed_tasks = [
        ti.task_id
        for ti in dag_run.get_task_instances()
        if ti.state == State.FAILED and ti.task_id != context["task_instance"].task_id
    ]

    if failed_tasks:
        error_msg = f"DAG finished with failed tasks: {', '.join(failed_tasks)}"
        logging.error(error_msg)
        raise