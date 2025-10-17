from elasticsearch import AsyncElasticsearch

from services.hierarchy_services.elastic.configs import (
    HIERARCHY_OBJ_INDEX,
    HIERARCHY_NODE_DATA_INDEX,
)
from services.hierarchy_services.kafka.consumers.changes_topic.events.limits import (
    TERMS_MAX_SIZE,
)


async def on_delete_obj(
    msg: dict[str : list[dict]] | None, elastic_client: AsyncElasticsearch
):
    if not msg or not msg["objects"]:
        return

    objs = {i["id"]: i for i in msg["objects"]}
    obj_ids = list(objs.keys())
    chunks_obj_ids = (
        obj_ids[i : i + TERMS_MAX_SIZE]
        for i in range(0, len(obj_ids), TERMS_MAX_SIZE)
    )

    for chunk_obj_ids in chunks_obj_ids:
        await delete_cascade(
            node_ids=chunk_obj_ids, elastic_client=elastic_client
        )

        # delete existing objects
        search_query = {"terms": {"id": chunk_obj_ids}}
        await elastic_client.delete_by_query(
            index=HIERARCHY_OBJ_INDEX,
            query=search_query,
            ignore_unavailable=True,
            refresh=True,
        )


async def delete_cascade(
    node_ids: list[int], elastic_client: AsyncElasticsearch
):
    # delete node data
    delete_node_data_query = {"terms": {"node_id": node_ids}}
    await elastic_client.delete_by_query(
        index=HIERARCHY_NODE_DATA_INDEX,
        query=delete_node_data_query,
        ignore_unavailable=True,
        refresh=True,
    )
