import requests

ZERO_UUID = "00000000-0000-0000-0000-000000000000"

def request_to_1c(host: str, dic_name: str) -> dict:
    url = f"{host}/send/by_db_name/AstOffice/getbaseinfo/{dic_name}"

    resp = requests.post(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def normalize_zero_uuid_fields(item: dict, fields: list[str]) -> dict:
    """Заменяет ZERO_UUID на пустую строку в указанных полях."""
    for field in fields:
        if item.get(field) == ZERO_UUID:
            item[field] = ""
    return item
