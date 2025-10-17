from typing import Union

from elasticsearch import AsyncElasticsearch

from services.hierarchy_services.elastic.configs import HIERARCHY_OBJ_INDEX


async def with_node_deleted(
    payload_before: Union[dict, None],
    payload_after: Union[dict, None],
    elastic_client: AsyncElasticsearch,
):
    if payload_before:
        node_id = payload_before["id"]

        search_query = {"match": {"id": node_id}}
        try:
            await elastic_client.delete_by_query(
                index=HIERARCHY_OBJ_INDEX,
                query=search_query,
                ignore_unavailable=True,
                refresh=True,
            )
        except Exception as e:
            print(e)
