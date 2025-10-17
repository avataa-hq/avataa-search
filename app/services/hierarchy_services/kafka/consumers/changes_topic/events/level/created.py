from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import BulkIndexError, async_bulk

from services.hierarchy_services.elastic.configs import HIERARCHY_LEVELS_INDEX
from services.hierarchy_services.kafka.consumers.changes_topic.events.limits import (
    QUERY_MAX_SIZE,
)


async def on_create_level(
    msg: dict[str : list[dict]] | None, elastic_client: AsyncElasticsearch
):
    if not msg or not msg["objects"]:
        return

    levels = {int(i["id"]): i for i in msg["objects"]}
    level_ids = list(levels.keys())
    chunks_level_ids = (
        level_ids[i : i + QUERY_MAX_SIZE]
        for i in range(0, len(level_ids), QUERY_MAX_SIZE)
    )

    # collecting not yet existing level ids
    not_exists_level_ids = set()
    for chunk_level_ids in chunks_level_ids:
        search_query = {"terms": {"id": chunk_level_ids}}
        level_exists = await elastic_client.search(
            index=HIERARCHY_LEVELS_INDEX,
            query=search_query,
            size=QUERY_MAX_SIZE,
        )
        chunk_level_id_exists = {
            int(i["_source"]["id"]) for i in level_exists["hits"]["hits"]
        }
        chunk_not_exists_level_ids = set(chunk_level_ids).difference(
            chunk_level_id_exists
        )
        not_exists_level_ids.update(chunk_not_exists_level_ids)

    # create actions for bulk operation
    actions = []
    for not_exists_level_id in not_exists_level_ids:
        item = levels[not_exists_level_id]
        actions.append(
            dict(
                _index=HIERARCHY_LEVELS_INDEX,
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
