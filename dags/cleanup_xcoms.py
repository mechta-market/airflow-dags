import pendulum
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models.xcom import XCom
from sqlalchemy.orm import Session
from airflow.utils.session import provide_session


DAG_ID = "cleanup_old_xcoms_daily"

default_args = {
    "owner": "airflow",
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
}


@provide_session
def delete_old_xcoms(session: Session = None, **kwargs):
    cutoff_date = pendulum.now("UTC").subtract(hours=1)
    logging.info(f"cutoff_date {cutoff_date}")
    deleted = (
        session.query(XCom)
        .filter(XCom.execution_date < cutoff_date)
        .delete(synchronize_session=False)
    )
    session.commit()
    logging.info(f"Deleted {deleted} XCom rows older than {cutoff_date}")


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 0 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["maintenance", "xcom", "cleanup"],
) as dag:

    cleanup_task = PythonOperator(
        task_id="delete_old_xcoms",
        python_callable=delete_old_xcoms,
        provide_context=True,
    )

    cleanup_task
