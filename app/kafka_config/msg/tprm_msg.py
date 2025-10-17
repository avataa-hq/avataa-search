from elastic.client import async_client as client
from elastic.config import PARAMS_INDEX


async def on_create_tprm(msg):
    for tprm in msg["objects"]:
        await client.index(index=PARAMS_INDEX, document=tprm)


async def on_update_tprm(msg):
    objects = []

    for item in msg["objects"]:
        objects.append({"id": item["id"], "searchable": item["searchable"]})

    for item in objects:
        await client.update_by_query(
            index=PARAMS_INDEX,
            query={"match": {"id": item["id"]}},
            script={
                "source": f"ctx._source.searchable='{str(item['searchable']).lower()}'"
            },
            requests_per_second=1,
            # scroll="1s",
            # scroll_size=1,
            # search_timeout="1s",
            slices=1,
        )


async def on_delete_tprm(msg):
    ids = set()

    for item in msg["objects"]:
        ids.add(item["id"])

    ids = [{"match": {"id": tprm_id}} for tprm_id in ids]
    await client.delete_by_query(
        index=PARAMS_INDEX,
        query={"bool": {"should": ids, "minimum_should_match": 1}},
    )
