from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import BulkIndexError, async_bulk

from services.hierarchy_services.elastic.configs import (
    HIERARCHY_NODE_DATA_INDEX,
)
from services.hierarchy_services.kafka.consumers.changes_topic.events.limits import (
    QUERY_MAX_SIZE,
)


async def on_update_node_data(
    msg: dict[str : list[dict]] | None, elastic_client: AsyncElasticsearch
):
    if not msg or not msg["objects"]:
        return

    node_data = {int(i["id"]): i for i in msg["objects"]}
    node_data_ids = list(node_data.keys())
    chunks_node_data_ids = (
        node_data_ids[i : i + QUERY_MAX_SIZE]
        for i in range(0, len(node_data_ids), QUERY_MAX_SIZE)
    )

    # collecting existing node datas
    exists_node_datas = {}
    for chunk_node_data_ids in chunks_node_data_ids:
        search_query = {"terms": {"id": chunk_node_data_ids}}
        node_datas_exists = await elastic_client.search(
            index=HIERARCHY_NODE_DATA_INDEX,
            query=search_query,
            size=QUERY_MAX_SIZE,
        )
        chunk_node_data_exists = {
            int(i["_source"]["id"]): i["_source"]
            for i in node_datas_exists["hits"]["hits"]
        }
        exists_node_datas.update(chunk_node_data_exists)

    # create actions for bulk operation
    actions = []
    for node_data_id, item in exists_node_datas.items():
        update_item = node_data[node_data_id]
        item.update(update_item)
        actions.append(
            dict(
                _index=HIERARCHY_NODE_DATA_INDEX,
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
