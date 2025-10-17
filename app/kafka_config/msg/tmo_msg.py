from elastic.client import async_client as client
from elastic.config import INVENTORY_INDEX, TMO_INDEX


async def on_create_tmo(msg):
    for doc in msg["objects"]:
        await client.index(index=TMO_INDEX, document=doc)


async def on_update_tmo(msg):
    for item in msg["objects"]:
        resp = await client.search(
            index=TMO_INDEX, query={"match": {"id": item["id"]}}
        )
        try:
            resp = resp["hits"]["hits"][0]
            await client.index(index=TMO_INDEX, id=resp["_id"], document=item)
        except KeyError:
            pass
        except IndexError:
            pass


async def on_delete_tmo(msg):
    ids = []

    for item in msg["objects"]:
        ids.append(item["id"])

    tmo_ids = [{"match": {"id": tmo_id}} for tmo_id in ids]
    query = {"bool": {"should": tmo_ids, "minimum_should_match": 1}}
    await client.delete_by_query(index=TMO_INDEX, query=query)

    query = {"bool": {"should": tmo_ids, "minimum_should_match": 1}}
    await client.delete_by_query(index=INVENTORY_INDEX, query=query)
