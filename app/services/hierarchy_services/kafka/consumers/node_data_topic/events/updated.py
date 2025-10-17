import json
from typing import Union

from elasticsearch import AsyncElasticsearch

from services.hierarchy_services.elastic.configs import (
    HIERARCHY_NODE_DATA_INDEX,
)


async def with_node_data_updated(
    payload_before: Union[dict, None],
    payload_after: Union[dict, None],
    elastic_client: AsyncElasticsearch,
):
    if payload_after:
        node_data_id = payload_after["id"]

        search_query = {"match": {"id": node_data_id}}
        try:
            node_exists = await elastic_client.search(
                index=HIERARCHY_NODE_DATA_INDEX, query=search_query, size=1
            )
        except Exception:
            return
        node_exists = node_exists["hits"]["hits"]

        if node_exists:
            node_exists = node_exists[0]["_source"]

            unfolded_key = payload_after.get("unfolded_key")
            if unfolded_key:
                payload_after["unfolded_key"] = json.loads(unfolded_key)

            node_exists.update(payload_after)
            try:
                await elastic_client.index(
                    index=HIERARCHY_NODE_DATA_INDEX,
                    id=node_data_id,
                    document=node_exists,
                    refresh="true",
                )
            except Exception as e:
                print(e)
