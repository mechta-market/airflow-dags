from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

DAG_ID = "cleanup_old_xcoms_daily"

default_args = {
    "owner": "airflow",
}


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=datetime(2025, 6, 10),
    schedule="0 * * * *",
    max_active_runs=1,
    catchup=False,
    tags=["maintenance", "xcom", "cleanup"],
) as dag:

    cleanup_task = BashOperator(
        task_id="delete_old_xcoms",
        bash_command="airflow xcom clear --older-than 1h --yes",
    )

    cleanup_task
