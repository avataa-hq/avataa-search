import pytest
from elasticsearch import AsyncElasticsearch

from kafka_config.config import KAFKA_HIERARCHY_CHANGES_TOPIC
from services.hierarchy_services.elastic.configs import (
    HIERARCHY_NODE_DATA_INDEX,
)
from services.hierarchy_services.kafka.consumers.changes_topic.configs import (
    HierarchyMessageType,
    ObjEventStatus,
)
from services.hierarchy_services.kafka.consumers.changes_topic.protobuf.hierarchy_producer_msg_pb2 import (
    ListNodeData,
)
from services.hierarchy_services.kafka.consumers.changes_topic.utils import (
    HierarchyChangesTopicHandler,
)
from kafka.utils import KafkaMSGMock
from services.inventory_services.protobuf_files.obj_proto.custom_deserializer import (
    protobuf_kafka_msg_to_dict,
)


@pytest.mark.asyncio(loop_scope="session")
async def test_create_node_data_not_existing(
    async_elastic_session: AsyncElasticsearch, new_node_data_msg: ListNodeData
):
    kfk_msg = KafkaMSGMock(
        msg_key=f"{HierarchyMessageType.NODE_DATA.value}:{ObjEventStatus.CREATED.value}",
        msg_value=new_node_data_msg.SerializeToString(),
        msg_topic=KAFKA_HIERARCHY_CHANGES_TOPIC,
    )
    handler = HierarchyChangesTopicHandler(kafka_msg=kfk_msg)
    await handler.process_the_message()

    query = {"terms": {"id": [i.id for i in new_node_data_msg.objects]}}
    result = await async_elastic_session.search(
        query=query, index=HIERARCHY_NODE_DATA_INDEX
    )
    assert result["hits"]["hits"]

    saved_node_data = [i["_source"] for i in result["hits"]["hits"]]
    income_node_data = protobuf_kafka_msg_to_dict(
        msg=new_node_data_msg,  # noqa
        including_default_value_fields=True,
    )["objects"]
    assert saved_node_data == income_node_data


@pytest.mark.skip(reason="Not corrected implementing")
async def test_create_node_data_existing(
    async_elastic_session: AsyncElasticsearch,
    existing_node_data_msg: ListNodeData,
):
    # change name
    new_node_data_msg_objects = []
    for obj in existing_node_data_msg.objects:
        obj.mo_name += "_new"
        new_node_data_msg_objects.append(obj)
    new_node_data_msg = ListNodeData(objects=new_node_data_msg_objects)

    kfk_msg = KafkaMSGMock(
        msg_key=f"{HierarchyMessageType.NODE_DATA.value}:{ObjEventStatus.CREATED.value}",
        msg_value=new_node_data_msg.SerializeToString(),
        msg_topic=KAFKA_HIERARCHY_CHANGES_TOPIC,
    )
    handler = HierarchyChangesTopicHandler(kafka_msg=kfk_msg)
    await handler.process_the_message()

    # comparison
    query = {"terms": {"id": [i.id for i in existing_node_data_msg.objects]}}
    result = await async_elastic_session.search(
        query=query, index=HIERARCHY_NODE_DATA_INDEX
    )
    assert result["hits"]["hits"]

    saved_node_data = [i["_source"] for i in result["hits"]["hits"]]
    assert len(saved_node_data) == len(new_node_data_msg_objects)
    income_node_data = protobuf_kafka_msg_to_dict(
        msg=new_node_data_msg,  # noqa
        including_default_value_fields=True,
    )["objects"]
    assert saved_node_data == income_node_data
