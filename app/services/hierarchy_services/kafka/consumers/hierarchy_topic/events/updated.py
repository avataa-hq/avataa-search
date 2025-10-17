from typing import Union

from elasticsearch import AsyncElasticsearch

from services.hierarchy_services.elastic.configs import (
    HIERARCHY_HIERARCHIES_INDEX,
)


async def with_hierarchy_updated(
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

        if hierarchy_exists:
            hierarchy_exists = hierarchy_exists[0]["_source"]

            hierarchy_exists.update(payload_after)
            try:
                await elastic_client.index(
                    index=HIERARCHY_HIERARCHIES_INDEX,
                    id=hierarchy_id,
                    document=hierarchy_exists,
                    refresh="true",
                )
            except Exception as e:
                print(e)
