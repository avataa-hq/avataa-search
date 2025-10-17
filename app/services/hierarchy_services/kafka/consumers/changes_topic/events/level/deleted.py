from elasticsearch import AsyncElasticsearch

from services.hierarchy_services.elastic.configs import (
    HIERARCHY_LEVELS_INDEX,
    HIERARCHY_OBJ_INDEX,
    HIERARCHY_NODE_DATA_INDEX,
)
from services.hierarchy_services.kafka.consumers.changes_topic.events.limits import (
    TERMS_MAX_SIZE,
)


async def on_delete_level(
    msg: dict[str : list[dict]] | None, elastic_client: AsyncElasticsearch
):
    if not msg or not msg["objects"]:
        return

    levels = {int(i["id"]): i for i in msg["objects"]}
    level_ids = list(levels.keys())
    chunks_level_ids = (
        level_ids[i : i + TERMS_MAX_SIZE]
        for i in range(0, len(level_ids), TERMS_MAX_SIZE)
    )

    for chunk_level_ids in chunks_level_ids:
        await delete_cascade(
            level_ids=chunk_level_ids, elastic_client=elastic_client
        )

        # delete existing levels
        search_query = {"terms": {"id": chunk_level_ids}}
        await elastic_client.delete_by_query(
            index=HIERARCHY_LEVELS_INDEX,
            query=search_query,
            ignore_unavailable=True,
            refresh=True,
        )


async def delete_cascade(
    level_ids: list[int], elastic_client: AsyncElasticsearch
):
    # delete node data
    delete_node_data_query = {"terms": {"level_id": level_ids}}
    await elastic_client.delete_by_query(
        index=HIERARCHY_NODE_DATA_INDEX,
        query=delete_node_data_query,
        ignore_unavailable=True,
        refresh=True,
    )

    # delete obj
    delete_obj_query = {"terms": {"level_id": level_ids}}
    await elastic_client.delete_by_query(
        index=HIERARCHY_OBJ_INDEX,
        query=delete_obj_query,
        ignore_unavailable=True,
        refresh=True,
    )
