import pytest
from elasticsearch import AsyncElasticsearch

from kafka_config.config import KAFKA_HIERARCHY_CHANGES_TOPIC
from services.hierarchy_services.kafka.consumers.changes_topic.configs import (
    HierarchyMessageType,
    ObjEventStatus,
)
from services.hierarchy_services.kafka.consumers.changes_topic.protobuf.hierarchy_producer_msg_pb2 import (
    ListHierarchy,
)
from services.hierarchy_services.kafka.consumers.changes_topic.utils import (
    HierarchyChangesTopicHandler,
)
from kafka.utils import KafkaMSGMock


async def test_wrong_key(
    async_elastic_session: AsyncElasticsearch, new_hierarchy_msg: ListHierarchy
):
    kfk_msg = KafkaMSGMock(
        msg_key=f"test_wrong_key:{ObjEventStatus.CREATED.value}",
        msg_value=new_hierarchy_msg.SerializeToString(),
        msg_topic=KAFKA_HIERARCHY_CHANGES_TOPIC,
    )
    handler = HierarchyChangesTopicHandler(kafka_msg=kfk_msg)
    with pytest.raises(NotImplementedError):
        await handler.process_the_message()


async def test_wrong_key2(
    async_elastic_session: AsyncElasticsearch, new_hierarchy_msg: ListHierarchy
):
    kfk_msg = KafkaMSGMock(
        msg_key=f"{HierarchyMessageType.HIERARCHY.value}:wrong_create",
        msg_value=new_hierarchy_msg.SerializeToString(),
        msg_topic=KAFKA_HIERARCHY_CHANGES_TOPIC,
    )
    handler = HierarchyChangesTopicHandler(kafka_msg=kfk_msg)
    with pytest.raises(NotImplementedError):
        await handler.process_the_message()
