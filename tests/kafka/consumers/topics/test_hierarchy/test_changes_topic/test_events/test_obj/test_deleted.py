import pytest
from elasticsearch import AsyncElasticsearch

from kafka_config.config import KAFKA_HIERARCHY_CHANGES_TOPIC
from services.hierarchy_services.elastic.configs import (
    HIERARCHY_OBJ_INDEX,
    HIERARCHY_NODE_DATA_INDEX,
)
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


@pytest.mark.asyncio(loop_scope="session")
async def test_delete_obj_not_existing(
    async_elastic_session: AsyncElasticsearch, new_obj_msg: ListNode
):
    obj_ids = [i.id for i in new_obj_msg.objects]
    query = {"terms": {"id": obj_ids}}
    result = await async_elastic_session.search(
        query=query, index=HIERARCHY_OBJ_INDEX
    )
    count_before = result["hits"]["total"]["value"]
    assert count_before == 0

    # node data count
    cnt_node_data_before = await async_elastic_session.count(
        index=HIERARCHY_NODE_DATA_INDEX
    )

    kfk_msg = KafkaMSGMock(
        msg_key=f"{HierarchyMessageType.OBJ.value}:{ObjEventStatus.DELETED.value}",
        msg_value=new_obj_msg.SerializeToString(),
        msg_topic=KAFKA_HIERARCHY_CHANGES_TOPIC,
    )
    handler = HierarchyChangesTopicHandler(kafka_msg=kfk_msg)
    await handler.process_the_message()

    result = await async_elastic_session.search(
        query=query, index=HIERARCHY_OBJ_INDEX
    )
    count_after = result["hits"]["total"]["value"]

    assert count_before == count_after

    # cascade
    # node data
    cnt_node_data_after = await async_elastic_session.count(
        index=HIERARCHY_NODE_DATA_INDEX
    )
    assert cnt_node_data_before == cnt_node_data_after
    assert cnt_node_data_after["count"] > 0


@pytest.mark.asyncio(loop_scope="session")
async def test_delete_obj_existing(
    async_elastic_session: AsyncElasticsearch, existing_obj_msg: ListNode
):
    obj_ids = [i.id for i in existing_obj_msg.objects]
    query = {"terms": {"id": obj_ids}}
    result = await async_elastic_session.search(
        query=query, index=HIERARCHY_OBJ_INDEX
    )
    count_before = result["hits"]["total"]["value"]

    # node data count
    cnt_node_data_before = await async_elastic_session.count(
        index=HIERARCHY_NODE_DATA_INDEX
    )

    kfk_msg = KafkaMSGMock(
        msg_key=f"{HierarchyMessageType.OBJ.value}:{ObjEventStatus.DELETED.value}",
        msg_value=existing_obj_msg.SerializeToString(),
        msg_topic=KAFKA_HIERARCHY_CHANGES_TOPIC,
    )
    handler = HierarchyChangesTopicHandler(kafka_msg=kfk_msg)
    await handler.process_the_message()

    result = await async_elastic_session.search(
        query=query, index=HIERARCHY_OBJ_INDEX
    )
    count_after = result["hits"]["total"]["value"]
    assert count_after == 0

    assert count_before != count_after

    # cascade
    # node data
    cnt_node_data_after = await async_elastic_session.count(
        index=HIERARCHY_NODE_DATA_INDEX
    )
    assert cnt_node_data_before["count"] > cnt_node_data_after["count"] > 0
