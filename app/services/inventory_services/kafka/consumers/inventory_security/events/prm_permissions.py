from typing import List

from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk, BulkIndexError

from services.inventory_services.elastic.security.configs import (
    INVENTORY_SECURITY_PRM_PERMISSION_INDEX,
)


async def with_prm_permissions_create(
    msg: List[dict], async_client: AsyncElasticsearch
):
    """Kafka event handler for PRM permission crete event"""
    actions = list()
    for item in msg:
        item_id = item.get("id")
        if item_id:
            action_item = dict(
                _index=INVENTORY_SECURITY_PRM_PERMISSION_INDEX,
                _op_type="index",
                _id=item_id,
                _source=item,
            )
            actions.append(action_item)

    if actions:
        try:
            await async_bulk(
                client=async_client, refresh="true", actions=actions
            )
        except BulkIndexError as e:
            print(e.errors)
            raise e


async def with_prm_permissions_update(
    msg: List[dict], async_client: AsyncElasticsearch
):
    """Kafka event handler for PRM permission update event"""
    await with_prm_permissions_create(msg=msg, async_client=async_client)


async def with_prm_permissions_delete(
    msg: List[dict], async_client: AsyncElasticsearch
):
    """Kafka event handler for PRM permission delete event"""
    ids_to_delete = [item_id for item in msg if (item_id := item.get("id"))]

    if ids_to_delete:
        delete_query = {
            "bool": {
                "should": [
                    {"terms": {"id": ids_to_delete}},
                    {"terms": {"parent_id": ids_to_delete}},
                ],
                "minimum_should_match": 1,
            }
        }
        await async_client.delete_by_query(
            index=INVENTORY_SECURITY_PRM_PERMISSION_INDEX, query=delete_query
        )
