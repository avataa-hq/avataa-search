from typing import Union

from elasticsearch import AsyncElasticsearch

from services.hierarchy_services.elastic.configs import HIERARCHY_LEVELS_INDEX


async def with_level_deleted(
    payload_before: Union[dict, None],
    payload_after: Union[dict, None],
    elastic_client: AsyncElasticsearch,
):
    if payload_before:
        level_id = payload_before["id"]

        search_query = {"match": {"id": level_id}}
        await elastic_client.delete_by_query(
            index=HIERARCHY_LEVELS_INDEX,
            query=search_query,
            ignore_unavailable=True,
            refresh=True,
        )
