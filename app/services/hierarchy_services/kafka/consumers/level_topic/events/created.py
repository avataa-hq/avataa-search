from typing import Union

from elasticsearch import AsyncElasticsearch

from services.hierarchy_services.elastic.configs import HIERARCHY_LEVELS_INDEX


async def with_level_created(
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

        if not level_exists:
            await elastic_client.index(
                index=HIERARCHY_LEVELS_INDEX,
                id=str(level_id),
                document=payload_after,
                refresh="true",
            )
