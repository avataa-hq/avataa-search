from elasticsearch import AsyncElasticsearch

from services.hierarchy_services.elastic.configs import (
    HIERARCHY_HIERARCHIES_INDEX,
    HIERARCHY_LEVELS_INDEX,
    HIERARCHY_OBJ_INDEX,
    HIERARCHY_NODE_DATA_INDEX,
)
from services.hierarchy_services.kafka.consumers.changes_topic.events.limits import (
    TERMS_MAX_SIZE,
    QUERY_MAX_SIZE,
)


async def on_delete_hierarchy(
    msg: dict[str : list[dict]] | None, elastic_client: AsyncElasticsearch
):
    if not msg or not msg["objects"]:
        return

    hierarchies = {int(i["id"]): i for i in msg["objects"]}
    hierarchy_ids = list(hierarchies.keys())
    chunks_hierarchy_ids = (
        hierarchy_ids[i : i + TERMS_MAX_SIZE]
        for i in range(0, len(hierarchy_ids), TERMS_MAX_SIZE)
    )

    for chunk_hierarchy_ids in chunks_hierarchy_ids:
        await delete_cascade(
            hierarchy_ids=chunk_hierarchy_ids, elastic_client=elastic_client
        )

        # delete existing hierarchies
        search_query = {"terms": {"id": chunk_hierarchy_ids}}
        await elastic_client.delete_by_query(
            index=HIERARCHY_HIERARCHIES_INDEX,
            query=search_query,
            ignore_unavailable=True,
            refresh=True,
        )


async def delete_cascade(
    hierarchy_ids: list[int], elastic_client: AsyncElasticsearch
):
    # delete node data
    search_levels_query = {"terms": {"hierarchy_id": hierarchy_ids}}
    scroll_time = "1m"
    response = await elastic_client.search(
        index=HIERARCHY_LEVELS_INDEX,
        query=search_levels_query,
        size=QUERY_MAX_SIZE,
        scroll=scroll_time,
    )
    scroll_id = response["_scroll_id"]
    while True:
        if not len(response["hits"]["hits"]):
            break
        level_ids = [i["_source"]["id"] for i in response["hits"]["hits"]]
        delete_node_data_query = {"terms": {"level_id": level_ids}}
        await elastic_client.delete_by_query(
            index=HIERARCHY_NODE_DATA_INDEX,
            query=delete_node_data_query,
            ignore_unavailable=True,
            refresh=True,
        )
        response = await elastic_client.scroll(
            scroll_id=scroll_id, scroll=scroll_time
        )
    await elastic_client.clear_scroll(scroll_id=scroll_id)

    # delete obj
    delete_obj_query = {"terms": {"hierarchy_id": hierarchy_ids}}
    await elastic_client.delete_by_query(
        index=HIERARCHY_OBJ_INDEX,
        query=delete_obj_query,
        ignore_unavailable=True,
        refresh=True,
    )

    # delete levels
    delete_levels_query = {"terms": {"hierarchy_id": hierarchy_ids}}
    await elastic_client.delete_by_query(
        index=HIERARCHY_LEVELS_INDEX,
        query=delete_levels_query,
        ignore_unavailable=True,
        refresh=True,
    )
