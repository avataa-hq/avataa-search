from elasticsearch import ConflictError

from elastic.client import async_client as client
from elastic.config import INVENTORY_INDEX


async def on_create_mo(msg):
    for doc in msg["objects"]:
        await client.index(index=INVENTORY_INDEX, document=doc)


async def on_update_mo(msg):
    for item in msg["objects"]:
        resp = await client.search(
            index=INVENTORY_INDEX, query={"match": {"id": item["id"]}}
        )
        try:
            resp = resp["hits"]["hits"][0]
            await client.index(
                index=INVENTORY_INDEX, id=resp["_id"], document=item
            )
        except KeyError:
            pass
        except IndexError:
            pass


async def on_delete_mo(msg):
    ids = []

    for item in msg["objects"]:
        ids.append(item["id"])

    ids = [{"match": {"id": mo_id}} for mo_id in ids]
    query = {"bool": {"should": ids, "minimum_should_match": 1}}
    try:
        await client.delete_by_query(index=INVENTORY_INDEX, query=query)
    except ConflictError:
        pass
