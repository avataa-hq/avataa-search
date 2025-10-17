import json
from typing import Union

from elasticsearch import AsyncElasticsearch

from services.hierarchy_services.elastic.configs import (
    HIERARCHY_NODE_DATA_INDEX,
)


async def with_node_data_created(
    payload_before: Union[dict, None],
    payload_after: Union[dict, None],
    elastic_client: AsyncElasticsearch,
):
    if payload_after:
        node_data_id = payload_after["id"]

        unfolded_key = payload_after.get("unfolded_key")
        if unfolded_key:
            payload_after["unfolded_key"] = json.loads(unfolded_key)

        try:
            await elastic_client.index(
                index=HIERARCHY_NODE_DATA_INDEX,
                id=str(node_data_id),
                document=payload_after,
                refresh="true",
            )
        except Exception as e:
            print(e)
