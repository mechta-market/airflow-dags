import logging
import requests
from typing import List, Dict, Any
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from elasticsearch import helpers

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from filter.utils import fetch_with_retry, check_errors_callable
from helpers.utils import elastic_conn, put_to_s3, get_from_s3

DAG_ID = "employee"
DEFAULT_ARGS = {
    "owner": "Askar",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
INDEX_NAME = "employee"
PAGE_SIZE = 1000
S3_EXTRACT = f"{DAG_ID}/extracted.json"
S3_TRANSFORM = f"{DAG_ID}/transformed.json"


class DocumentEmployee:
    def __init__(self, p: Dict):
        self.id = p.get("id", "")
        self.id_utp = p.get("id_utp", "")
        self.created_at = p.get("created_at", "")
        self.modified_at = p.get("modified_at", "")
        self.iin = p.get("iin", "")
        self.phone_number = p.get("phone_number", "")
        self.subdivision_id = p.get("subdivision_id", "")
        self.subdivision_id_utp = p.get("subdivision_id_utp", "")
        self.subdivision_name = p.get("subdivision_name", "")
        self.last_name = p.get("last_name", "")
        self.first_name = p.get("first_name", "")
        self.middle_name = p.get("middle_name", "")
        self.gender = p.get("gender", "")
        self.birth_date = p.get("birth_date", "")
        self.position_id = p.get("position_id", "")
        self.position_name = p.get("position_name", "")
        self.active = p.get("active", False)
        self.status = p.get("status", "")


def encode_employee(p: Dict) -> Dict:
    """
    Преобразование «сырых» данных сотрудника в документ для Elasticsearch.
    """
    return DocumentEmployee(p).__dict__


def extract_data_callable():
    """
    Извлечение данных сотрудников из employee и сохранение в S3.
    """
    MAX_WORKERS = 2

    url = Variable.get("employee_host") + "/employee"
    logging.info("Start extracting employees from %s", url)

    def get_total_pages() -> int:
        try:
            response = requests.get(
                url,
                params={"list_params.only_count": True},
                timeout=10,
            )
            response.raise_for_status()
            total = int(
                response.json().get("pagination_info", {}).get("total_count", 0)
            )
            pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
            logging.info("Total pages: %d (page_size=%d)", pages, PAGE_SIZE)
            return pages
        except requests.RequestException as e:
            logging.error("Failed to fetch total pages: %s", e)
            raise

    def fetch_page(page: int) -> List[Dict]:
        data = fetch_with_retry(
            url,
            params={"list_params.page": page, "list_params.page_size": PAGE_SIZE},
        )
        results = data.get("results", [])
        return results

    total_pages = get_total_pages()
    extracted_employees: List[Dict] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(fetch_page, page): page for page in range(total_pages)
        }
        for f in as_completed(futures):
            page = futures[f]
            try:
                page_items = f.result()
                extracted_employees.extend(page_items)
                logging.info(f"page {page} is processed")
            except Exception as e:
                logging.error(f"Error fetching page {page}: {e}")
                raise

    if not extracted_employees:
        logging.error("No pages extracted, failing DAG run")
        raise ValueError("no employees extracted")

    put_to_s3(data=extracted_employees, s3_key=S3_EXTRACT)
    logging.info(f"extracted employees count: {len(extracted_employees)}")


def transform_data_callable():
    """
    Преобразование извлечённых данных в документы для Elasticsearch.
    """
    MAX_WORKERS = 7

    collected_employees: List[Dict] = get_from_s3(S3_EXTRACT)
    transformed_employees: List[Dict] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(encode_employee, p) for p in collected_employees]
        for future in as_completed(futures):
            try:
                transformed_employees.append(future.result())
            except Exception as e:
                logging.error("Error transforming employee: %s", e)
                raise

    put_to_s3(data=transformed_employees, s3_key=S3_TRANSFORM)

    logging.info(f"transformed employees count: {len(transformed_employees)}")


def delete_different_data_callable():
    """
    Удаление данных из Elasticsearch, которые не присутствуют в S3.
    """
    transformed_employees: List[Dict] = get_from_s3(S3_TRANSFORM)
    transformed_employees_ids = {
        employee.get("id") for employee in transformed_employees if employee.get("id")
    }
    client = elastic_conn(Variable.get("elastic_scheme"))

    existing_ids_query = {
        "_source": False,
        "fields": ["_id"],
        "query": {"match_all": {}},
    }

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

    ids_to_delete = existing_ids - transformed_employees_ids

    logging.info(f"count of ids to delete: {len(ids_to_delete)}")

    delete_actions = [
        {
            "_op_type": "delete",
            "_index": INDEX_NAME,
            "_id": employee_id,
        }
        for employee_id in ids_to_delete
    ]

    if delete_actions:
        try:
            success, errors = helpers.bulk(
                client, delete_actions, refresh="wait_for", stats_only=False
            )
            logging.info(f"delete success, deleted document count: {success}")
            if errors:
                logging.error(f"error during bulk delete: {errors}")
        except Exception as bulk_error:
            logging.error(f"bulk delete failed, error: {bulk_error}")
            raise


def load_data_callable():
    """
    Загрузка преобразованных данных в Elasticsearch.
    """
    transformed_employees = get_from_s3(s3_key=S3_TRANSFORM)

    client = elastic_conn(Variable.get("elastic_scheme"))

    actions = [
        {
            "_op_type": "update",
            "_index": INDEX_NAME,
            "_id": employee.get("id"),
            "doc": employee,
            "doc_as_upsert": True,
            "retry_on_conflict": 3,
        }
        for employee in transformed_employees
        if employee.get("id")
    ]

    try:
        success, errors = helpers.bulk(
            client, actions, refresh="wait_for", stats_only=False
        )
        logging.info(f"update success, updated documents count: {success}")
        if errors:
            logging.error(f"error during bulk update: {errors}")
    except Exception as bulk_error:
        logging.error(f"bulk update failed, error: {bulk_error}")
        raise


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 7, 25),
    schedule="50 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["elasticsearch", "employee"],
) as dag:
    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_data_callable,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data_callable,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    delete = PythonOperator(
        task_id="delete",
        python_callable=delete_different_data_callable,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_data_callable,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    check = PythonOperator(
        task_id="check_errors",
        python_callable=check_errors_callable,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    extract >> transform >> delete >> load >> check
