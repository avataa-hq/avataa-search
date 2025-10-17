from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import BulkIndexError, async_bulk

from services.hierarchy_services.elastic.configs import HIERARCHY_OBJ_INDEX
from services.hierarchy_services.kafka.consumers.changes_topic.events.limits import (
    QUERY_MAX_SIZE,
)


async def on_create_obj(
    msg: dict[str : list[dict]] | None, elastic_client: AsyncElasticsearch
):
    if not msg or not msg["objects"]:
        return

    objects = {i["id"]: i for i in msg["objects"]}
    object_ids = list(objects.keys())
    chunks_obj_ids = (
        object_ids[i : i + QUERY_MAX_SIZE]
        for i in range(0, len(object_ids), QUERY_MAX_SIZE)
    )

    # collecting not yet existing object ids
    not_exists_obj_ids = set()
    for chunk_obj_ids in chunks_obj_ids:
        search_query = {"terms": {"id": chunk_obj_ids}}
        obj_exists = await elastic_client.search(
            index=HIERARCHY_OBJ_INDEX, query=search_query, size=QUERY_MAX_SIZE
        )
        chunk_obj_id_exists = {
            i["_source"]["id"] for i in obj_exists["hits"]["hits"]
        }
        chunk_not_exists_obj_ids = set(chunk_obj_ids).difference(
            chunk_obj_id_exists
        )
        not_exists_obj_ids.update(chunk_not_exists_obj_ids)

    # create actions for bulk operation
    actions = []
    for not_exists_obj_id in not_exists_obj_ids:
        item = objects[not_exists_obj_id]
        actions.append(
            dict(
                _index=HIERARCHY_OBJ_INDEX,
                _op_type="index",
                _id=item["id"],
                _source=item,
            )
        )

    # save
    try:
        await async_bulk(client=elastic_client, refresh="true", actions=actions)
    except BulkIndexError as e:
        print(*actions, sep="\n")
        print(*e.errors, sep="\n")
        raise e
