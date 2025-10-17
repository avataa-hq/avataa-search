from typing import Union

from elasticsearch import AsyncElasticsearch

from services.hierarchy_services.elastic.configs import HIERARCHY_LEVELS_INDEX


async def with_level_updated(
    payload_before: Union[dict, None],
    payload_after: Union[dict, None],
    elastic_client: AsyncElasticsearch,
):
    if payload_after:
        level_id = payload_after["id"]

        search_query = {"match": {"id": level_id}}
        level_exists = await elastic_client.search(
            index=HIERARCHY_LEVELS_INDEX, query=search_query, size=1
        )
        level_exists = level_exists["hits"]["hits"]

        if level_exists:
            level_exists = level_exists[0]["_source"]

            level_exists.update(payload_after)

            await elastic_client.index(
                index=HIERARCHY_LEVELS_INDEX,
                id=level_id,
                document=level_exists,
                refresh="true",
            )
