from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import BulkIndexError, async_bulk

from services.hierarchy_services.elastic.configs import (
    HIERARCHY_NODE_DATA_INDEX,
)
from services.hierarchy_services.kafka.consumers.changes_topic.events.limits import (
    QUERY_MAX_SIZE,
)


async def on_create_node_data(
    msg: dict[str : list[dict]] | None, elastic_client: AsyncElasticsearch
):
    if not msg or not msg["objects"]:
        return

    nodes = {int(i["id"]): i for i in msg["objects"]}
    node_ids = list(nodes.keys())
    chunks_node_ids = (
        node_ids[i : i + QUERY_MAX_SIZE]
        for i in range(0, len(node_ids), QUERY_MAX_SIZE)
    )

    # collecting not yet existing node ids
    not_exists_node_ids = set()
    for chunk_node_ids in chunks_node_ids:
        search_query = {"terms": {"id": chunk_node_ids}}
        obj_exists = await elastic_client.search(
            index=HIERARCHY_NODE_DATA_INDEX,
            query=search_query,
            size=QUERY_MAX_SIZE,
        )
        chunk_node_id_exists = {
            int(i["_source"]["id"]) for i in obj_exists["hits"]["hits"]
        }
        chunk_not_exists_node_ids = set(chunk_node_ids).difference(
            chunk_node_id_exists
        )
        not_exists_node_ids.update(chunk_not_exists_node_ids)

    # create actions for bulk operation
    actions = []
    for not_exists_node_id in not_exists_node_ids:
        item = nodes[not_exists_node_id]
        actions.append(
            dict(
                _index=HIERARCHY_NODE_DATA_INDEX,
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
