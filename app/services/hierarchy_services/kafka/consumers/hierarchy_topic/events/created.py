from typing import Union

from elasticsearch import AsyncElasticsearch

from services.hierarchy_services.elastic.configs import (
    HIERARCHY_HIERARCHIES_INDEX,
)


async def with_hierarchy_created(
    payload_before: Union[dict, None],
    payload_after: Union[dict, None],
    elastic_client: AsyncElasticsearch,
):
    if payload_after:
        hierarchy_id = payload_after["id"]

        search_query = {"match": {"id": hierarchy_id}}
        hierarchy_exists = await elastic_client.search(
            index=HIERARCHY_HIERARCHIES_INDEX, query=search_query, size=1
        )
        hierarchy_exists = hierarchy_exists["hits"]["hits"]

        if not hierarchy_exists:
            await elastic_client.index(
                index=HIERARCHY_HIERARCHIES_INDEX,
                id=str(hierarchy_id),
                document=payload_after,
                refresh="true",
            )
