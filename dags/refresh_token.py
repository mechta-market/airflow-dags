import json
import requests
import pendulum

from airflow.decorators import dag, task
from airflow.sdk import Variable


@dag(
    dag_id="refresh_token",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2025, 5, 14, tz="UTC"),
    catchup=False,
    tags=["nsi", "token"],
)
@task
def refresh_jwt():
    """Обновление JWT токена."""
    new_token = get_new_jwt_from_api()
    Variable.set("access_token", new_token)


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


refresh_jwt()
