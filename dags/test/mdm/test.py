from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def test_s3_conn():
    hook = S3Hook(aws_conn_id="s3")
    client = hook.get_conn()
    print("Buckets:", client.list_buckets())
    print("Client:", client)


default_args = {
    "owner": "Amir",
    "depends_on_past": False,
}

# Extra:
# {
#   "endpoint_url": "https://storage.yandexcloud.kz/",
#   "region_name": "us-east-1"
# }

with DAG(
    dag_id="test",
    default_args=default_args,
    schedule_interval="*/10 * * * *",
    start_date=datetime(2025, 5, 14),
    catchup=False,
    tags=["s3"],
) as dag:

    test_s3_conn = PythonOperator(
        task_id="test_s3_conn",
        python_callable=test_s3_conn,
        provide_context=True,
    )

    test_s3_conn
