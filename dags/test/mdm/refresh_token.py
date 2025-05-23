import json
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator


def get_new_jwt_from_api() -> str:
    refresh_token = Variable.get("refresh_token")
    url = "https://api.mdev.kz/account/profile/auth/token"
    data = {"refresh_token": refresh_token}
    try:
        resp = requests.post(url, json=data, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        return payload.get("access_token", "")
    except requests.RequestException as e:
        print(f"Ошибка при запросе нового JWT: {e}")
        return ""


def refresh_token():
    new_token = get_new_jwt_from_api()
    Variable.set("access_token", new_token)


default_args = {
    "owner": "Amir",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=f"refresh_token",
    default_args=default_args,
    schedule_interval="*/10 * * * *",
    start_date=datetime(2025, 5, 14),
    catchup=False,
    tags=["nsi", "elasticsearch"],
) as dag:

    refresh_jwt = PythonOperator(
        task_id="refresh_token_task",
        python_callable=refresh_token,
        provide_context=True,
    )

    refresh_jwt
