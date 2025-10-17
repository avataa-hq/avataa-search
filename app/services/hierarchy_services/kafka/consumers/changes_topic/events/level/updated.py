from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import BulkIndexError, async_bulk

from services.hierarchy_services.elastic.configs import HIERARCHY_LEVELS_INDEX
from services.hierarchy_services.kafka.consumers.changes_topic.events.limits import (
    QUERY_MAX_SIZE,
)


async def on_update_level(
    msg: dict[str : list[dict]] | None, elastic_client: AsyncElasticsearch
):
    if not msg or not msg["objects"]:
        return

    level = {int(i["id"]): i for i in msg["objects"]}
    level_ids = list(level.keys())
    chunks_level_ids = (
        level_ids[i : i + QUERY_MAX_SIZE]
        for i in range(0, len(level_ids), QUERY_MAX_SIZE)
    )

    # collecting existing levels
    exists_levels = {}
    for chunk_level_ids in chunks_level_ids:
        search_query = {"terms": {"id": chunk_level_ids}}
        levels_exists = await elastic_client.search(
            index=HIERARCHY_LEVELS_INDEX,
            query=search_query,
            size=QUERY_MAX_SIZE,
        )
        chunk_levels_exists = {
            int(i["_source"]["id"]): i["_source"]
            for i in levels_exists["hits"]["hits"]
        }
        exists_levels.update(chunk_levels_exists)

    # create actions for bulk operation
    actions = []
    for level_id, item in exists_levels.items():
        update_item = level[level_id]
        item.update(update_item)
        actions.append(
            dict(
                _index=HIERARCHY_LEVELS_INDEX,
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
