from elasticsearch import ConflictError

from elastic.client import async_client as client
from elastic.config import PERMISSION_INDEX


async def create_permission(objects: list[dict], type_: str):
    for obj in objects:
        obj = prepare_message(obj, type_)
        await client.index(index=PERMISSION_INDEX, document=obj)


async def update_permission(objects: list[dict], type_: str):
    for obj in objects:
        query = {
            "bool": {
                "must": [
                    {"match": {"parent_type": type_}},
                    {"match": {"id": obj["id"]}},
                ]
            }
        }

        response = await client.search(index=PERMISSION_INDEX, query=query)
        try:
            id_ = response["hits"]["hits"][0]["_id"]
            await client.index(index=PERMISSION_INDEX, document=obj, id=id_)
        except IndexError:
            pass
        except KeyError:
            pass


async def delete_permission(objects: list[dict], type_: str):
    ids = [obj["id"] for obj in objects]

    query = {
        "bool": {
            "must": {"match": {"parent_type": type_}},
            "should": [{"match": {"id": id_}} for id_ in ids],
            "minimum_should_match": 1,
        }
    }

    try:
        await client.delete_by_query(index=PERMISSION_INDEX, query=query)
    except ConflictError:
        pass


def prepare_message(msg: dict, type_: str):
    return {
        "id": msg["id"],
        "permission": msg["permission"],
        "read": msg["read"],
        "admin": msg["admin"],
        "parent_id": msg["parent_id"],
        "root_permission_id": msg["root_permission_id"],
        "permission_name": msg["permission_name"],
        "parent_type": type_,
    }
