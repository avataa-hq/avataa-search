from collections import defaultdict

import pytest
from elasticsearch import AsyncElasticsearch

from kafka_config.config import KAFKA_HIERARCHY_CHANGES_TOPIC
from services.hierarchy_services.elastic.configs import (
    HIERARCHY_HIERARCHIES_INDEX,
)
from services.hierarchy_services.kafka.consumers.changes_topic.configs import (
    HierarchyMessageType,
    ObjEventStatus,
)
from services.hierarchy_services.kafka.consumers.changes_topic.protobuf.hierarchy_producer_msg_pb2 import (
    ListHierarchyPermission,
)
from services.hierarchy_services.kafka.consumers.changes_topic.utils import (
    HierarchyChangesTopicHandler,
)
from kafka.utils import KafkaMSGMock


@pytest.mark.asyncio(loop_scope="session")
async def test_create_obj_not_existing(
    async_elastic_session: AsyncElasticsearch,
    new_permission_data: ListHierarchyPermission,
):
    kfk_msg = KafkaMSGMock(
        msg_key=f"{HierarchyMessageType.HIERARCHY_PERMISSION.value}:{ObjEventStatus.CREATED.value}",
        msg_value=new_permission_data.SerializeToString(),
        msg_topic=KAFKA_HIERARCHY_CHANGES_TOPIC,
    )
    handler = HierarchyChangesTopicHandler(kafka_msg=kfk_msg)
    await handler.process_the_message()

    query = {
        "terms": {"id": [i.parent_id for i in new_permission_data.objects]}
    }
    result = await async_elastic_session.search(
        query=query, index=HIERARCHY_HIERARCHIES_INDEX
    )
    assert not result["hits"]["hits"]


@pytest.mark.asyncio(loop_scope="session")
async def test_create_permission_existing(
    async_elastic_session: AsyncElasticsearch,
    existing_permission_data: ListHierarchyPermission,
):
    kfk_msg = KafkaMSGMock(
        msg_key=f"{HierarchyMessageType.HIERARCHY_PERMISSION.value}:{ObjEventStatus.CREATED.value}",
        msg_value=existing_permission_data.SerializeToString(),
        msg_topic=KAFKA_HIERARCHY_CHANGES_TOPIC,
    )
    handler = HierarchyChangesTopicHandler(kafka_msg=kfk_msg)
    await handler.process_the_message()

    # comparison
    query = {
        "terms": {"id": [i.parent_id for i in existing_permission_data.objects]}
    }
    result = await async_elastic_session.search(
        query=query, index=HIERARCHY_HIERARCHIES_INDEX
    )
    assert result["hits"]["hits"]

    collected_permissions = defaultdict(set)
    for permission_item in existing_permission_data.objects:
        if not permission_item.read:
            continue
        collected_permissions[permission_item.parent_id].add(
            permission_item.permission
        )

    for item_data in result["hits"]["hits"]:
        source = item_data["_source"]
        permissions = source.get("permissions", [])
        assert permissions

        incoming_permissions = collected_permissions[source["id"]]
        assert not set(permissions).difference(incoming_permissions)
        assert len(set(permissions)) == len(permissions)
