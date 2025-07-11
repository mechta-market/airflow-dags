import logging
import requests
from typing import Any, Dict, List
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from elasticsearch import helpers

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from filter.utils import (
    fetch_with_retry,
    check_errors_callable,
)
from helpers.utils import elastic_conn, put_to_s3, get_from_s3

# DAG parameters

DAG_ID = "product"
default_args = {
    "owner": "Olzhas",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Constants

INDEX_NAME = "product_v2"

DEFAULT_LANGUAGE = "ru"
TARGET_LANGUAGES = ["ru", "kz"]

S3_FILE_NAME_EXTRACTED_DATA = f"{DAG_ID}/extracted_data.json"
S3_FILE_NAME_TRANSFORMED_PRODUCTS = f"{DAG_ID}/transformed_products.json"

# Configurations

logging.basicConfig(level=logging.INFO)

# Functions


class DocumentProduct:
    def __init__(self, p: dict):
        self.id = p.get("id", "")
        self.code = p.get("code", "")
        self.slug = p.get("slug", "")
        self.prev_slug = p.get("prev_slug", [])

        self.created_at = p.get("created_at")

        self.type = p.get("type", 0)
        self.service_type = p.get("service_type", 0)

        self.category_id = p.get("category_id", "")
        self.group_id = p.get("group_id", "")
        self.published = p.get("published", False)
        self.kbt = p.get("kbt", False)
        self.not_available_for_offline_sell = p.get(
            "not_available_for_offline_sell", False
        )
        self.not_available_for_online_sell = p.get(
            "not_available_for_online_sell", False
        )
        self.imei_track = p.get("imei_track", False)

        self.name_i18n = self._parse_i18n(p.get("name_i18n", {}))
        self.description_i18n = self._parse_i18n(p.get("description_i18n", {}))

        self.image_urls = p.get("image_urls", [])
        self.video_urls = self._parse_video_urls(p.get("video", {}))

        self.categories = self._parse_categories(p.get("breadcrumbs", []))
        self.pre_order = self._parse_pre_order(p.get("pre_order", None))

        self.properties = self._parse_properties(p.get("property_model", {}))
        self.all_properties = self._parse_all_properties(p.get("property_model", {}))
        self.similar_products = self._parse_similar_products(
            p.get("similar_products", [])
        )

    def _parse_i18n(self, field_i18n) -> dict:
        return {lang: field_i18n.get(lang, "") for lang in TARGET_LANGUAGES}

    def _parse_video_urls(self, obj) -> List[str]:
        if not obj:
            return []

        return obj.get("data", [])

    def _parse_categories(self, breadcrumbs) -> List[dict]:
        categories = []
        for cat in breadcrumbs:
            if not cat.get("id"):
                continue
            categories.append(
                {
                    "id": cat.get("id", ""),
                    "slug": cat.get("slug", ""),
                    "depth": cat.get("depth", 0),
                    "name_i18n": self._parse_i18n(cat.get("name_i18n", {})),
                }
            )
        return categories

    def _parse_pre_order(self, pre_order) -> dict:
        if not pre_order:
            return None
        return {
            "prepayment_amount": pre_order.get("prepayment_amount", 0),
            "prepayment_percent": pre_order.get("prepayment_percent", 0),
            "active_from": pre_order.get("active_from"),
            "active_to": pre_order.get("active_to"),
            "sell_from": pre_order.get("sell_from"),
            "count": pre_order.get("count", 0),
        }

    def _parse_properties(self, property_model: dict) -> List[Dict[str, Any]]:
        properties = []
        attr_ord_counter = 0

        groups = property_model.get("groups", [])
        for group in groups:
            flags = group.get("flags", {})
            if flags.get("hidden", False):
                continue

            attributes = group.get("attributes", [])
            for attr in attributes:
                attr_ord_counter += 1

                if not attr.get("as_filter", False):
                    continue
                if not attr.get("slug"):
                    continue
                if not attr.get("value"):
                    continue

                slugs = attr["value"].get("slugs", [])
                values_i18n = attr["value"].get("values_i18n", [])

                for i, v_slug in enumerate(slugs):
                    if not v_slug:
                        continue

                    p = {
                        "name_slug": attr.get("slug"),
                        "name": {},
                        "value_slug": v_slug,
                        "value": {},
                        "ord": attr_ord_counter,
                    }

                    name = attr.get("name_i18n", {})
                    for lang in TARGET_LANGUAGES:
                        p["name"][lang] = name.get(lang, "")

                    attr_type = attr.get("type", "")
                    data = attr.get("data", {})
                    if i < len(values_i18n):
                        val_i18n = values_i18n[i]
                    else:
                        val_i18n = {}

                    if attr_type == "boolean":
                        default_val = val_i18n.get(DEFAULT_LANGUAGE, "")
                        if default_val == "true":
                            p["value"]["ru"] = "Да"
                            p["value"]["kz"] = "Иә"
                        else:
                            p["value"]["ru"] = "Нет"
                            p["value"]["kz"] = "Жоқ"
                    elif attr_type == "text":
                        for lang in TARGET_LANGUAGES:
                            p["value"][lang] = val_i18n.get(lang, "")
                    elif attr_type == "number":
                        number = val_i18n.get(DEFAULT_LANGUAGE, "")
                        for lang in TARGET_LANGUAGES:
                            unit = data.get("m_unit_i18n", {}).get(lang, "")
                            p["value"][lang] = f"{number} {unit}".strip()
                    elif attr_type in ("select", "multi-select"):
                        options = data.get("options", [])
                        default_val = val_i18n.get(DEFAULT_LANGUAGE, "")
                        for option in options:
                            if option.get("value") == default_val:
                                label_i18n = option.get("label_i18n", {})
                                for lang in TARGET_LANGUAGES:
                                    p["value"][lang] = label_i18n.get(lang, "")
                                break
                    else:
                        continue

                    properties.append(p)
        return properties

    def _parse_all_properties(self, property_model: dict) -> List[Dict[str, Any]]:
        groups = property_model.get("groups") or []
        result_groups: List[Dict[str, Any]] = []

        for group in groups:
            attributes: List[Dict[str, Any]] = []
            for attribute in group.get("attributes") or []:
                data = attribute.get("data") or {}
                opts = data.get("options") or []
                m_unit_i18n = data.get("m_unit_i18n") or {}

                # Строим fast lookup для меток
                label_map = {
                    opt.get("label", ""): {
                        "label_i18n": opt.get("label_i18n", {}),
                        "m_unit_i18n": m_unit_i18n,
                    }
                    for opt in opts
                    if isinstance(opt, dict)
                }

                value = attribute.get("value") or {}
                slugs = value.get("slugs") or []
                vals_i18n = value.get("values_i18n") or []
                attr_type = attribute.get("type")

                # Собираем values
                values: List[Dict[str, Any]] = []
                for slug, v_i18n in zip(slugs, vals_i18n):
                    ru_label = v_i18n.get("ru", "")
                    opt_info = label_map.get(ru_label, {})
                    label_i18n = opt_info.get("label_i18n") or v_i18n

                    # Если числовой тип, добавляем единицу измерения в каждую языковую версию
                    if attr_type == "number" and m_unit_i18n:
                        ru_text = label_i18n.get("ru", "")
                        combined = {
                            lang: f"{ru_text} {m_unit_i18n.get(lang, '')}".strip()
                            for lang in m_unit_i18n
                        }
                        label_i18n = combined

                    # Если boolean тип, меняем значения на Да/Нет
                    if attr_type == "boolean":
                        if ru_label == "true":
                            label_i18n = {
                                "kz": "Иә",
                                "ru": "Да",
                            }
                        if ru_label == "false":
                            label_i18n = {
                                "kz": "Жоқ",
                                "ru": "Нет",
                            }
                    values.append(
                        {
                            "slug": slug,
                            "label_i18n": label_i18n,
                        }
                    )

                attributes.append(
                    {
                        "id": attribute.get("id"),
                        "name_i18n": attribute.get("name_i18n") or {},
                        "type": attr_type,
                        "ord": attribute.get("ord"),
                        "flags": attribute.get("flags") or {},
                        "slug": attribute.get("slug"),
                        "values": values,
                    }
                )

            result_groups.append(
                {
                    "id": group.get("id"),
                    "name_i18n": group.get("name_i18n") or {},
                    "ord": group.get("ord"),
                    "flags": group.get("flags") or {},
                    "attributes": attributes,
                }
            )

        transformed = {"groups": result_groups}
        return transformed

    def _parse_similar_products(self, similar_products) -> List[Dict[str, Any]]:
        result = []
        for similar_product in similar_products:
            data = similar_product.get("data", {})
            options = data.get("options", [])
            m_unit_i18n = data.get("m_unit_i18n", {})
            type = similar_product.get("type", "")
            # Строим fast lookup для меток
            label_map = {}
            for opt in options:
                if isinstance(opt, dict):
                    value = opt.get("value", "")
                    label_map[value] = {
                        "label_i18n": opt.get("label_i18n", {}),
                    }

            products = similar_product.get("products", [])
            products_res = []
            for product in products:
                product_prop = product.get("property", {})
                prop_value = product_prop.get("value")
                value_i18n = product_prop.get("value_i18n")
                if prop_value or prop_value in label_map:

                    if type == "select" or type == "multi-select":
                        label_info = label_map[prop_value]
                        label_i18n = label_info.get("label_i18n", {})

                        value_i18n = {
                            "ru": label_i18n.get("ru", ""),
                            "kz": label_i18n.get("kz", ""),
                        }
                    elif type == "number":
                        m_unit_ru = m_unit_i18n.get("ru", "")
                        m_unit_kz = m_unit_i18n.get("kz", "")

                        value_i18n = {
                            "ru": f"{prop_value} {m_unit_ru}".strip(),
                            "kz": f"{prop_value} {m_unit_kz}".strip(),
                        }
                    elif type == "boolean":
                        if prop_value == "true":
                            val_ru = "Да"
                            val_kz = "Иә"
                        elif prop_value == "false":
                            val_ru = "Нет"
                            val_kz = "Жоқ"

                        value_i18n = {
                            "ru": val_ru,
                            "kz": val_kz,
                        }
                products_res.append(
                    {
                        "id": product.get("id"),
                        "slug": product.get("slug"),
                        "name_i18n": product.get("name_i18n", {}),
                        "main_image_url": product.get("main_image_url"),
                        "property": {"value_i18n": value_i18n},
                    }
                )

            result.append(
                {
                    "id": similar_product.get("id"),
                    "name_i18n": similar_product.get("name_i18n", {}),
                    "products": products_res,
                }
            )
        return result


def encode_document_product(p: dict) -> dict:
    dp = DocumentProduct(p)
    return dp.__dict__


# Tasks


def extract_data_callable():
    BASE_URL = Variable.get("nsi_host")

    MAX_WORKERS = 2
    PAGE_SIZE = 1000

    def get_total_pages() -> int:
        try:
            response = requests.get(
                f"{BASE_URL}/product",
                params={
                    "list_params.only_count": True,
                    "archived": False,
                },
                timeout=10,
            )
            response.raise_for_status()
            total_count = int(
                response.json().get("pagination_info", {}).get("total_count", 0)
            )
            return (total_count + PAGE_SIZE - 1) // PAGE_SIZE
        except requests.RequestException as e:
            logging.error(f"Failed to fetch total pages: {e}")
            raise

    def fetch_page(page: int) -> List[str]:
        response = fetch_with_retry(
            f"{BASE_URL}/product",
            params={
                "list_params.page": page,
                "list_params.page_size": PAGE_SIZE,
                "archived": False,
            },
        )
        return [
            product["id"] for product in response.get("results", []) if "id" in product
        ]

    def fetch_product_details(id: str) -> dict:
        url = f"{BASE_URL}/product/{id}"
        params = {
            "with_properties": True,
            "with_breadcrumbs": True,
            "with_image_urls": True,
            "with_video": True,
            "with_pre_order": True,
            "with_similar_products": True,
        }
        return fetch_with_retry(url, params=params)

    total_pages = get_total_pages()
    logging.info(f"total_page: {total_pages}")

    product_ids: List[str] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(fetch_page, page): page for page in range(total_pages)
        }
        for f in as_completed(futures):
            page = futures[f]
            try:
                product_ids.extend(f.result())
                logging.info(f"page {page} is processed")
            except Exception as e:
                logging.error(f"error in processing page {page}: {e}")
                raise

    extracted_products: List[dict] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_product_details, id) for id in product_ids]
        for f in as_completed(futures):
            try:
                result = f.result()
                if result:
                    extracted_products.append(result)
            except Exception as e:
                logging.error(f"error in processing product details: {e}")
                raise

    put_to_s3(data=extracted_products, s3_key=S3_FILE_NAME_EXTRACTED_DATA)

    logging.info(f"extracted products count: {len(extracted_products)}")


def transform_data_callable():
    MAX_WORKERS = 7

    collected_products: List[dict] = get_from_s3(s3_key=S3_FILE_NAME_EXTRACTED_DATA)

    transformed_products: List[dict] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(encode_document_product, p) for p in collected_products
        ]
        for future in as_completed(futures):
            try:
                transformed_products.append(future.result())
            except Exception as e:
                logging.error(f"Failed to process product: {e}")
                raise

    put_to_s3(data=transformed_products, s3_key=S3_FILE_NAME_TRANSFORMED_PRODUCTS)

    logging.info(f"transformed products count: {len(transformed_products)}")


def delete_different_data_callable():
    transformed_products: List[dict] = get_from_s3(
        s3_key=S3_FILE_NAME_TRANSFORMED_PRODUCTS
    )
    transformed_product_ids = {
        product.get("id") for product in transformed_products if product.get("id")
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

    ids_to_delete = existing_ids - transformed_product_ids

    delete_actions = [
        {
            "_op_type": "delete",
            "_index": INDEX_NAME,
            "_id": product_id,
        }
        for product_id in ids_to_delete
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
    transformed_products = get_from_s3(s3_key=S3_FILE_NAME_TRANSFORMED_PRODUCTS)

    client = elastic_conn(Variable.get("elastic_scheme"))

    actions = [
        {
            "_op_type": "update",
            "_index": INDEX_NAME,
            "_id": product.get("id"),
            "doc": product,
            "doc_as_upsert": True,
            "retry_on_conflict": 3,
        }
        for product in transformed_products
        if product.get("id")
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
    default_args=default_args,
    description="DAG to upload products from NSI service to Elasticsearch index",
    start_date=datetime(2025, 6, 10),
    schedule="0 */6 * * *",
    max_active_runs=1,
    catchup=False,
    tags=["elasticsearch", "nsi", "product"],
) as dag:
    extract_data = PythonOperator(
        task_id="extract_data_task",
        python_callable=extract_data_callable,
        provide_context=True,
    )

    transform_data = PythonOperator(
        task_id="transform_data_task",
        python_callable=transform_data_callable,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    delete_different_data = PythonOperator(
        task_id="delete_different_data_task",
        python_callable=delete_different_data_callable,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    load_data = PythonOperator(
        task_id="load_data_task",
        python_callable=load_data_callable,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    check_errors = PythonOperator(
        task_id="check_errors_task",
        python_callable=check_errors_callable,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        extract_data
        >> transform_data
        >> delete_different_data
        >> load_data
        >> check_errors
    )
