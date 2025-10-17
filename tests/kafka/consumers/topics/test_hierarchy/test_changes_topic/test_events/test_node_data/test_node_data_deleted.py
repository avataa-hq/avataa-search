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


@pytest.mark.asyncio(loop_scope="session")
async def test_delete_node_data_not_existing(
    async_elastic_session: AsyncElasticsearch, new_node_data_msg: ListNodeData
):
    node_data_ids = [i.id for i in new_node_data_msg.objects]
    query = {"terms": {"id": node_data_ids}}
    result = await async_elastic_session.search(
        query=query, index=HIERARCHY_NODE_DATA_INDEX
    )
    count_before = result["hits"]["total"]["value"]
    assert count_before == 0

    kfk_msg = KafkaMSGMock(
        msg_key=f"{HierarchyMessageType.NODE_DATA.value}:{ObjEventStatus.DELETED.value}",
        msg_value=new_node_data_msg.SerializeToString(),
        msg_topic=KAFKA_HIERARCHY_CHANGES_TOPIC,
    )
    handler = HierarchyChangesTopicHandler(kafka_msg=kfk_msg)
    await handler.process_the_message()

    result = await async_elastic_session.search(
        query=query, index=HIERARCHY_NODE_DATA_INDEX
    )
    count_after = result["hits"]["total"]["value"]

    assert count_before == count_after


@pytest.mark.asyncio(loop_scope="session")
async def test_delete_node_data_existing(
    async_elastic_session: AsyncElasticsearch,
    existing_node_data_msg: ListNodeData,
):
    node_data_ids = [i.id for i in existing_node_data_msg.objects]
    query = {"terms": {"id": node_data_ids}}
    result = await async_elastic_session.search(
        query=query, index=HIERARCHY_NODE_DATA_INDEX
    )
    count_before = result["hits"]["total"]["value"]

    kfk_msg = KafkaMSGMock(
        msg_key=f"{HierarchyMessageType.NODE_DATA.value}:{ObjEventStatus.DELETED.value}",
        msg_value=existing_node_data_msg.SerializeToString(),
        msg_topic=KAFKA_HIERARCHY_CHANGES_TOPIC,
    )
    handler = HierarchyChangesTopicHandler(kafka_msg=kfk_msg)
    await handler.process_the_message()

    result = await async_elastic_session.search(
        query=query, index=HIERARCHY_NODE_DATA_INDEX
    )
    count_after = result["hits"]["total"]["value"]
    assert count_after == 0

    assert count_before != count_after
