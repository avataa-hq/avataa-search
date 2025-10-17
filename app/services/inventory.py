import time

import requests
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from fastapi import HTTPException
from requests.exceptions import ConnectionError

from elastic.config import (
    INVENTORY_INDEX,
    PARAMS_INDEX,
    TMO_INDEX,
    PERMISSION_INDEX,
)
from services.auth import get_auth_token
from services.utils import correct_date, permission_type_mapper, generate_docs
from settings.config import INVENTORY_API_URL


def load_inventory_objects(client: Elasticsearch):
    objects = []
    offset = 0
    while True:
        token = get_auth_token()
        response = None
        for attempt in range(5):
            try:
                response = requests.get(
                    f"{INVENTORY_API_URL}/objects/?with_parameters=true&limit=1000&offset={offset}",
                    headers={
                        "Authorization": f"{token['token_type']} {token['access_token']}"
                    },
                    timeout=5,
                )
            except ConnectionError as exc:
                time.sleep(5)
                if attempt == 4:
                    raise HTTPException(status_code=503, detail=exc)
                continue

        if response.status_code != 200:
            continue

        response = response.json()

        bulk(client, generate_docs(correct_date(response), INVENTORY_INDEX))

        if len(response) < 1000:
            print(f"Objects loaded: {offset + len(response)}")
            break

        offset += 1000

    return objects


def load_params(client: Elasticsearch):
    token = get_auth_token()
    for attempt in range(5):
        try:
            response = requests.get(
                f"{INVENTORY_API_URL}/param_types/",
                headers={
                    "Authorization": f"{token['token_type']} {token['access_token']}"
                },
                timeout=5,
            )
            response = response.json()
            bulk(client, generate_docs(response, PARAMS_INDEX))
        except ConnectionError as exc:
            time.sleep(5)
            if attempt == 4:
                raise HTTPException(status_code=503, detail=exc)


def load_tmo(client: Elasticsearch):
    token = get_auth_token()
    for attempt in range(5):
        try:
            response = requests.get(
                f"{INVENTORY_API_URL}/object_types/",
                headers={
                    "Authorization": f"{token['token_type']} {token['access_token']}"
                },
                timeout=5,
            )
            response = response.json()
            bulk(client, generate_docs(response, TMO_INDEX))
        except ConnectionError as exc:
            time.sleep(5)
            if attempt == 4:
                raise HTTPException(status_code=503, detail=exc)


def load_permissions(client: Elasticsearch):
    data = []
    for type_ in ["tmo", "mo", "tprm"]:
        data_by_type = load_permissions_by_type(permission_type_mapper(type_))
        if data_by_type:
            data.extend(data_by_type)

    bulk(client, generate_docs(data, PERMISSION_INDEX))


def load_permissions_by_type(type_: str):
    response = None

    token = get_auth_token()
    for attempt in range(5):
        try:
            response = requests.get(
                f"{INVENTORY_API_URL}/security/{type_}/",
                headers={
                    "Authorization": f"{token['token_type']} {token['access_token']}"
                },
                timeout=5,
            )
            if response.status_code < 300:
                response = response.json()
                # print(f'loading security/{type_} failed: {response.status_code}')
                break
        except ConnectionError as exc:
            time.sleep(5)
            if attempt == 4:
                raise HTTPException(status_code=503, detail=exc)

    if response is None:
        return

    result = []
    for item in response:
        result.append(
            {
                "id": item["id"],
                "permission": item["permission"],
                "read": item["read"],
                "admin": item["admin"],
                "parent_id": item["itemId"],
                "root_permission_id": item["rootPermissionId"],
                "permission_name": item["permissionName"],
                "parent_type": permission_type_mapper(type_),
            }
        )

    return result
