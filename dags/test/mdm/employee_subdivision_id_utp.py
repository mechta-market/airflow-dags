import logging
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from helpers.utils import put_to_s3, get_from_s3

DAG_ID = "employee_subdivision_id_utp"
DEFAULT_ARGS = {
    "owner": "Askar",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
S3_EXTRACT = "employee/transformed.json"
S3_TRANSFORM = "employee/transformed.json"
PAGE_SIZE = 1000


def enrich_subdivision():
    employees = get_from_s3(s3_key=S3_EXTRACT) or []

    payload = {"query": {"exists": {"field": "zup_id"}}, "size": PAGE_SIZE}

    url = Variable.get("subdivision_mdm_host") + "/_search"

    logging.info("Fetching subdivision mapping: %s", url)
    resp = requests.post(url, json=payload, timeout=30)
    resp.raise_for_status()
    hits = resp.json().get("hits", {}).get("hits", [])

    mapping = {
        h["_source"]["zup_id"]: h["_id"]
        for h in hits
        if h.get("_source", {}).get("zup_id")
    }
    logging.info("Loaded %d subdivision mappings", len(mapping))

    for emp in employees:
        emp["subdivision_id_utp"] = mapping.get(emp.get("subdivision_id"), "")

    put_to_s3(data=employees, s3_key=S3_TRANSFORM)
    logging.info("Enriched %d employees", len(employees))


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    schedule_interval="45 * * * *",
    start_date=datetime(2025, 7, 26),
    catchup=False,
    tags=["mdm", "employee"],
) as dag:

    enrich = PythonOperator(
        task_id="enrich_subdivision_id_utp",
        python_callable=enrich_subdivision,
    )
