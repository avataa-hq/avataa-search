from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import BulkIndexError, async_bulk

from services.hierarchy_services.elastic.configs import HIERARCHY_OBJ_INDEX
from services.hierarchy_services.kafka.consumers.changes_topic.events.limits import (
    QUERY_MAX_SIZE,
)


async def on_update_obj(
    msg: dict[str : list[dict]] | None, elastic_client: AsyncElasticsearch
):
    if not msg or not msg["objects"]:
        return

    objs = {i["id"]: i for i in msg["objects"]}
    obj_ids = list(objs.keys())
    chunks_obj_ids = (
        obj_ids[i : i + QUERY_MAX_SIZE]
        for i in range(0, len(obj_ids), QUERY_MAX_SIZE)
    )

    # collecting existing objects
    exists_objs = {}
    for chunk_obj_ids in chunks_obj_ids:
        search_query = {"terms": {"id": chunk_obj_ids}}
        obj_exists = await elastic_client.search(
            index=HIERARCHY_OBJ_INDEX, query=search_query, size=QUERY_MAX_SIZE
        )
        chunk_obj_exists = {
            i["_source"]["id"]: i["_source"] for i in obj_exists["hits"]["hits"]
        }
        exists_objs.update(chunk_obj_exists)

    # create actions for bulk operation
    actions = []
    for obj_id, item in exists_objs.items():
        update_item = objs[obj_id]
        item.update(update_item)
        actions.append(
            dict(
                _index=HIERARCHY_OBJ_INDEX,
                _op_type="update",
                _id=item["id"],
                doc=item,
            )
        )

    # save
    try:
        await async_bulk(client=elastic_client, refresh="true", actions=actions)
    except BulkIndexError as e:
        print(*actions, sep="\n")
        print(*e.errors, sep="\n")
        raise e
