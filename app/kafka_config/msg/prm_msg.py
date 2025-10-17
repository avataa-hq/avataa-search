import time

from elastic.client import async_client as client
from elastic.config import INVENTORY_INDEX


async def on_create_prm(msg):
    for item in msg["objects"]:
        for attempt in range(5):
            resp = await client.search(
                index=INVENTORY_INDEX, query={"match": {"id": item["mo_id"]}}
            )
            if len(resp["hits"]["hits"]) == 0:
                time.sleep(1)
            else:
                break
        try:
            resp = resp["hits"]["hits"][0]
        except KeyError:
            pass
        except IndexError:
            break

        if "params" in resp["_source"]:
            resp["_source"]["params"].append(item)
        else:
            resp["_source"]["params"] = [item]

        await client.index(
            index=INVENTORY_INDEX, id=resp["_id"], document=resp["_source"]
        )


async def on_delete_prm(msg):
    for item in msg["objects"]:
        resp = await client.search(
            index=INVENTORY_INDEX, query={"match": {"id": item["mo_id"]}}
        )
        if len(resp["hits"]["hits"]) == 0:
            return

        resp = resp["hits"]["hits"][0]
        try:
            for param in resp["_source"]["params"]:
                if param["id"] == item["id"]:
                    resp["_source"]["params"].remove(param)
                    break
            await client.index(
                index=INVENTORY_INDEX, id=resp["_id"], document=resp["_source"]
            )
        except KeyError:
            pass


async def on_update_prm(msg):
    for item in msg["objects"]:
        resp = await client.search(
            index=INVENTORY_INDEX, query={"match": {"id": item["mo_id"]}}
        )
        try:
            resp = resp["hits"]["hits"][0]
            for param in resp["_source"]["params"]:
                if param["id"] == item["id"]:
                    param["value"] = item["value"]
            await client.index(
                index=INVENTORY_INDEX, id=resp["_id"], document=resp["_source"]
            )
        except KeyError:
            pass
        except IndexError:
            pass
