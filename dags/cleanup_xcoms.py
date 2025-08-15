import pendulum
import logging

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models.xcom import XCom
from airflow.utils.session import provide_session

from sqlalchemy.orm import Session

DAG_ID = "cleanup_old_xcoms_daily"

default_args = {
    "owner": "airflow",
}


@provide_session
def delete_old_xcoms(session: Session = None):
    cutoff_date = pendulum.now("UTC").subtract(hours=1)
    logging.info(f"cutoff_date {cutoff_date}")
    deleted = (
        session.query(XCom)
        .filter(XCom.execution_date < cutoff_date)
        .delete(synchronize_session=False)
    )
    session.commit()
    logging.info(f"deleted {deleted} XCom rows older than {cutoff_date}")


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 * * * *",
    max_active_runs=1,
    catchup=False,
    tags=["maintenance", "xcom", "cleanup"],
) as dag:

    cleanup_task = PythonOperator(
        task_id="delete_old_xcoms",
        python_callable=delete_old_xcoms,
    )

    cleanup_task
