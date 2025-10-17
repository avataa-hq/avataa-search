from typing import Union

from elasticsearch import AsyncElasticsearch

from services.hierarchy_services.elastic.configs import HIERARCHY_OBJ_INDEX


async def with_node_updated(
    payload_before: Union[dict, None],
    payload_after: Union[dict, None],
    elastic_client: AsyncElasticsearch,
):
    if payload_after:
        node_id = payload_after["id"]

        search_query = {"match": {"id": node_id}}
        try:
            node_exists = await elastic_client.search(
                index=HIERARCHY_OBJ_INDEX, query=search_query, size=1
            )
        except Exception:
            return
        node_exists = node_exists["hits"]["hits"]

        if node_exists:
            node_exists = node_exists[0]["_source"]

            node_exists.update(payload_after)

            try:
                await elastic_client.index(
                    index=HIERARCHY_OBJ_INDEX,
                    id=node_id,
                    document=node_exists,
                    refresh="true",
                )
            except Exception as e:
                print(e)
