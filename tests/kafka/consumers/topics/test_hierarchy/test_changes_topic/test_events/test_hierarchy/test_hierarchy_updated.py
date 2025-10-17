import pytest
from elasticsearch import AsyncElasticsearch

from kafka.utils import KafkaMSGMock
from kafka_config.config import KAFKA_HIERARCHY_CHANGES_TOPIC
from services.hierarchy_services.elastic.configs import (
    HIERARCHY_HIERARCHIES_INDEX,
)
from services.hierarchy_services.kafka.consumers.changes_topic.configs import (
    HierarchyMessageType,
    ObjEventStatus,
)
from services.hierarchy_services.kafka.consumers.changes_topic.protobuf.custom_deserializer import (
    protobuf_kafka_msg_to_dict,
)
from services.hierarchy_services.kafka.consumers.changes_topic.protobuf.hierarchy_producer_msg_pb2 import (
    ListHierarchy,
)
from services.hierarchy_services.kafka.consumers.changes_topic.utils import (
    HierarchyChangesTopicHandler,
)


@pytest.mark.asyncio(loop_scope="session")
async def test_update_hierarchy_not_existing(
    async_elastic_session: AsyncElasticsearch, new_hierarchy_msg: ListHierarchy
):
    """Ignore if items not exist in hierarchy"""
    kfk_msg = KafkaMSGMock(
        msg_key=f"{HierarchyMessageType.HIERARCHY.value}:{ObjEventStatus.UPDATED.value}",
        msg_value=new_hierarchy_msg.SerializeToString(),
        msg_topic=KAFKA_HIERARCHY_CHANGES_TOPIC,
    )
    handler = HierarchyChangesTopicHandler(kafka_msg=kfk_msg)
    await handler.process_the_message()

    query = {"terms": {"id": [i.id for i in new_hierarchy_msg.objects]}}
    result = await async_elastic_session.search(
        query=query, index=HIERARCHY_HIERARCHIES_INDEX
    )
    assert not result["hits"]["hits"]


@pytest.mark.asyncio(loop_scope="session")
async def test_update_hierarchy_existing(
    async_elastic_session: AsyncElasticsearch,
    existing_hierarchy_msg: ListHierarchy,
):
    """
    Only if item exists
    """
    # change name
    new_hierarchy_msg_objects = []
    for obj in existing_hierarchy_msg.objects:
        obj.name += "_new"
        new_hierarchy_msg_objects.append(obj)
    new_hierarchy_msg = ListHierarchy(objects=new_hierarchy_msg_objects)

    kfk_msg = KafkaMSGMock(
        msg_key=f"{HierarchyMessageType.HIERARCHY.value}:{ObjEventStatus.UPDATED.value}",
        msg_value=new_hierarchy_msg.SerializeToString(),
        msg_topic=KAFKA_HIERARCHY_CHANGES_TOPIC,
    )
    handler = HierarchyChangesTopicHandler(kafka_msg=kfk_msg)
    await handler.process_the_message()

    # comparison
    query = {"terms": {"id": [i.id for i in existing_hierarchy_msg.objects]}}
    result = await async_elastic_session.search(
        query=query, index=HIERARCHY_HIERARCHIES_INDEX
    )
    assert result["hits"]["hits"]

    saved_hierarchy = [i["_source"] for i in result["hits"]["hits"]]
    assert len(saved_hierarchy) == len(new_hierarchy_msg_objects)
    income_hierarchy = protobuf_kafka_msg_to_dict(
        msg=new_hierarchy_msg,  # noqa
        including_default_value_fields=True,
    )["objects"]
    saved_hierarchy[0].pop("permissions", None)
    assert saved_hierarchy == income_hierarchy
