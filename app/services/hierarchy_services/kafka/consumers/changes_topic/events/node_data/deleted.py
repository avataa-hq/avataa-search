from elasticsearch import AsyncElasticsearch

from services.hierarchy_services.elastic.configs import (
    HIERARCHY_NODE_DATA_INDEX,
)
from services.hierarchy_services.kafka.consumers.changes_topic.events.limits import (
    TERMS_MAX_SIZE,
)


async def on_delete_node_data(
    msg: dict[str : list[dict]] | None, elastic_client: AsyncElasticsearch
):
    if not msg or not msg["objects"]:
        return

    node_data = {int(i["id"]): i for i in msg["objects"]}
    node_data_ids = list(node_data.keys())
    chunks_node_data_ids = (
        node_data_ids[i : i + TERMS_MAX_SIZE]
        for i in range(0, len(node_data_ids), TERMS_MAX_SIZE)
    )

    # delete existing node_datas
    for chunk_node_data_ids in chunks_node_data_ids:
        # delete existing node_datas
        search_query = {"terms": {"id": chunk_node_data_ids}}
        await elastic_client.delete_by_query(
            index=HIERARCHY_NODE_DATA_INDEX,
            query=search_query,
            ignore_unavailable=True,
            refresh=True,
        )
