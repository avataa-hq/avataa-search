from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import BulkIndexError, async_bulk

from services.hierarchy_services.elastic.configs import (
    HIERARCHY_HIERARCHIES_INDEX,
)
from services.hierarchy_services.kafka.consumers.changes_topic.events.limits import (
    QUERY_MAX_SIZE,
)


async def on_update_hierarchy(
    msg: dict[str : list[dict]] | None, elastic_client: AsyncElasticsearch
):
    if not msg or not msg["objects"]:
        return

    hierarchies = {int(i["id"]): i for i in msg["objects"]}
    hierarchy_ids = list(hierarchies.keys())
    chunks_hierarchy_ids = (
        hierarchy_ids[i : i + QUERY_MAX_SIZE]
        for i in range(0, len(hierarchy_ids), QUERY_MAX_SIZE)
    )

    # collecting existing hierarchies
    exists_hierarchies = {}
    for chunk_hierarchy_ids in chunks_hierarchy_ids:
        search_query = {"terms": {"id": chunk_hierarchy_ids}}
        hierarchy_exists = await elastic_client.search(
            index=HIERARCHY_HIERARCHIES_INDEX,
            query=search_query,
            size=QUERY_MAX_SIZE,
        )
        chunk_hierarchies_exists = {
            int(i["_source"]["id"]): i["_source"]
            for i in hierarchy_exists["hits"]["hits"]
        }
        exists_hierarchies.update(chunk_hierarchies_exists)

    # create actions for bulk operation
    actions = []
    for hierarchy_id, item in exists_hierarchies.items():
        update_item = hierarchies[hierarchy_id]
        item.update(update_item)
        actions.append(
            dict(
                _index=HIERARCHY_HIERARCHIES_INDEX,
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
