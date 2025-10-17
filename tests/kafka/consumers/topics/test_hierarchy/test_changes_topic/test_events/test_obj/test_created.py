import pytest
from elasticsearch import AsyncElasticsearch

from kafka_config.config import KAFKA_HIERARCHY_CHANGES_TOPIC
from services.hierarchy_services.elastic.configs import HIERARCHY_OBJ_INDEX
from services.hierarchy_services.kafka.consumers.changes_topic.configs import (
    HierarchyMessageType,
    ObjEventStatus,
)
from services.hierarchy_services.kafka.consumers.changes_topic.protobuf.hierarchy_producer_msg_pb2 import (
    ListNode,
)
from services.hierarchy_services.kafka.consumers.changes_topic.utils import (
    HierarchyChangesTopicHandler,
)
from kafka.utils import KafkaMSGMock
from services.inventory_services.protobuf_files.obj_proto.custom_deserializer import (
    protobuf_kafka_msg_to_dict,
)


@pytest.mark.asyncio(loop_scope="session")
async def test_create_obj_not_existing(
    async_elastic_session: AsyncElasticsearch, new_obj_msg: ListNode
):
    kfk_msg = KafkaMSGMock(
        msg_key=f"{HierarchyMessageType.OBJ.value}:{ObjEventStatus.CREATED.value}",
        msg_value=new_obj_msg.SerializeToString(),
        msg_topic=KAFKA_HIERARCHY_CHANGES_TOPIC,
    )
    handler = HierarchyChangesTopicHandler(kafka_msg=kfk_msg)
    await handler.process_the_message()

    query = {"terms": {"id": [i.id for i in new_obj_msg.objects]}}
    result = await async_elastic_session.search(
        query=query, index=HIERARCHY_OBJ_INDEX
    )
    assert result["hits"]["hits"]

    saved_obj = [i["_source"] for i in result["hits"]["hits"]]
    income_obj = protobuf_kafka_msg_to_dict(
        msg=new_obj_msg,  # noqa
        including_default_value_fields=True,
    )["objects"]
    assert saved_obj == income_obj


@pytest.mark.skip(reason="Not corrected implementing")
async def test_create_obj_existing(
    async_elastic_session: AsyncElasticsearch, existing_obj_msg: ListNode
):
    # change name
    new_obj_msg_objects = []
    for obj in existing_obj_msg.objects:
        obj.key += "_new"
        new_obj_msg_objects.append(obj)
    new_obj_msg = ListNode(objects=new_obj_msg_objects)

    kfk_msg = KafkaMSGMock(
        msg_key=f"{HierarchyMessageType.OBJ.value}:{ObjEventStatus.CREATED.value}",
        msg_value=new_obj_msg.SerializeToString(),
        msg_topic=KAFKA_HIERARCHY_CHANGES_TOPIC,
    )
    handler = HierarchyChangesTopicHandler(kafka_msg=kfk_msg)
    await handler.process_the_message()

    # comparison
    query = {"terms": {"id": [i.id for i in existing_obj_msg.objects]}}
    result = await async_elastic_session.search(
        query=query, index=HIERARCHY_OBJ_INDEX
    )
    assert result["hits"]["hits"]

    saved_obj = [i["_source"] for i in result["hits"]["hits"]]
    assert len(saved_obj) == len(new_obj_msg_objects)
    income_obj = protobuf_kafka_msg_to_dict(
        msg=new_obj_msg,  # noqa
        including_default_value_fields=True,
    )["objects"]
    assert saved_obj == income_obj
