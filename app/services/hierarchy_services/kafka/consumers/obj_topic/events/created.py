from typing import Union

from elasticsearch import AsyncElasticsearch

from services.hierarchy_services.elastic.configs import HIERARCHY_OBJ_INDEX


async def with_node_created(
    payload_before: Union[dict, None],
    payload_after: Union[dict, None],
    elastic_client: AsyncElasticsearch,
):
    if payload_after:
        node_id = payload_after["id"]

        try:
            await elastic_client.index(
                index=HIERARCHY_OBJ_INDEX,
                id=str(node_id),
                document=payload_after,
                refresh="true",
            )
        except Exception as e:
            print(e)
