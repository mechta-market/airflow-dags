import pendulum
import logging
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models.xcom import XCom

DAG_ID = "cleanup_old_xcoms_daily"

default_args = {
    "owner": "airflow",
}


def delete_old_xcoms():
    cutoff_time = pendulum.now("UTC").subtract(hours=1)

    deleted_count = XCom.delete(
        dag_id="product",
    )
    logging.info(f"deleted {deleted_count} XCom rows older than {cutoff_time}")


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=datetime(2025, 6, 10),
    schedule="0 * * * *",
    max_active_runs=1,
    catchup=False,
    tags=["maintenance", "xcom", "cleanup"],
) as dag:

    cleanup_task = PythonOperator(
        task_id="delete_old_xcoms",
        python_callable=delete_old_xcoms,
    )

    cleanup_task
