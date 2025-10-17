import time

import requests
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from fastapi import HTTPException

from elastic.config import HIERARCHY_INDEX
from services.auth import get_auth_token
from services.utils import generate_docs
from settings.config import HIERARCHY_API_URL


def load_hierarchies(client: Elasticsearch):
    token = get_auth_token()
    hierarchies = None
    for attempt in range(5):
        try:
            response = requests.get(
                f"{HIERARCHY_API_URL}/hierarchy",
                headers={
                    "Authorization": f"{token['token_type']} {token['access_token']}"
                },
                timeout=5,
            )
            if response.status_code < 300:
                hierarchies = response.json()
                break
        except ConnectionError as exc:
            time.sleep(5)
            if attempt == 4:
                raise HTTPException(status_code=503, detail=exc)

    levels = []
    if hierarchies is None:
        return

    for hierarchy in hierarchies:
        for attempt in range(5):
            try:
                response = requests.get(
                    f"{HIERARCHY_API_URL}/hierarchy/{hierarchy['id']}/level",
                    headers={
                        "Authorization": f"{token['token_type']} {token['access_token']}"
                    },
                    timeout=5,
                )
                if response.status_code == 200:
                    hier_levels = response.json()
                    for level in hier_levels:
                        level["hierarchy_name"] = hierarchy["name"]
                    levels.extend(hier_levels)
                break
            except ConnectionError as exc:
                time.sleep(5)
                if attempt == 4:
                    raise HTTPException(status_code=503, detail=exc)
            except TypeError as exc:
                print(hierarchy)
                raise exc

    bulk(client, generate_docs(levels, HIERARCHY_INDEX))
