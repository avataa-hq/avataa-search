from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import BulkIndexError, async_bulk

from services.hierarchy_services.elastic.configs import (
    HIERARCHY_HIERARCHIES_INDEX,
)
from services.hierarchy_services.kafka.consumers.changes_topic.events.limits import (
    QUERY_MAX_SIZE,
)


async def on_create_hierarchy(
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

    # collecting not yet existing hierarchy ids
    not_exists_hierarchy_ids = set()
    for chunk_hierarchy_ids in chunks_hierarchy_ids:
        search_query = {"terms": {"id": chunk_hierarchy_ids}}
        hierarchy_exists = await elastic_client.search(
            index=HIERARCHY_HIERARCHIES_INDEX,
            query=search_query,
            size=QUERY_MAX_SIZE,
        )
        chunk_hierarchy_id_exists = {
            int(i["_source"]["id"]) for i in hierarchy_exists["hits"]["hits"]
        }
        chunk_not_exists_hierarchy_ids = set(chunk_hierarchy_ids).difference(
            chunk_hierarchy_id_exists
        )
        not_exists_hierarchy_ids.update(chunk_not_exists_hierarchy_ids)

    # create actions for bulk operation
    actions = []
    for not_exists_hierarchy_id in not_exists_hierarchy_ids:
        item = hierarchies[not_exists_hierarchy_id]
        actions.append(
            dict(
                _index=HIERARCHY_HIERARCHIES_INDEX,
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
