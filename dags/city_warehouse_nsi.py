import json
import requests
import pendulum

from airflow.sdk import Variable
from airflow.decorators import dag, task
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook

DICTIONARY_NAME = "city_warehouse"
INDEX_NAME = f"{DICTIONARY_NAME}_nsi"


@dag(
    dag_id=f"{DICTIONARY_NAME}_nsi",
    schedule="*/10 * * * *",
    start_date=pendulum.datetime(2025, 5, 14, tz="UTC"),
    catchup=False,
    tags=["nsi", "elasticsearch"],
)
def nsi_to_es():

    @task()
    def fetch_data() -> list:
        """Получаем данные из NSI и возвращаем список."""
        url = f"https://api.mdev.kz/nsi/{DICTIONARY_NAME}"
        headers = {
            "Authorization": get_token(),
        }
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        payload = resp.json()

        return payload.get("results", [])

    @task()
    def upsert_to_es(items: list[dict]):
        """Загружаем данные в Elasticsearch."""
        hosts = ["https://mdm.zeon.mdev.kz"]

        es_hook = ElasticsearchPythonHook(
            hosts=hosts,
            es_conn_args={"basic_auth": ("mdm", get_elasticsearch_password())},
        )

        client = es_hook.get_conn

        for item in items:
            doc_id = item.get("city_id")
            if not doc_id:
                continue

            client.update(
                index=INDEX_NAME,
                id=doc_id,
                body={"doc": item, "doc_as_upsert": True},
            )

    data = fetch_data()
    upsert_to_es(data)


dag = nsi_to_es()


def get_token() -> str:
    access_token = Variable.get("access_token")
    return f"Bearer {access_token}"


def get_elasticsearch_password() -> str:
    el_password = Variable.get("elasticsearch_password")
    return el_password
