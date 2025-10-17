from typing import List

from kafka_config.config import KAFKA_GROUP_BUILDER_GROUP_TOPIC
from services.group_builder.kafka.consumers.group.protobuf import group_pb2
from tests.kafka.utils import KafkaMSGMock


def create_cleared_kafka_msg_for_group_topic(
    mo_ids: List[int],
    group_name: str,
    msg_class_name: str,
    msg_event: str,
    tmo_id: int,
):
    msg_value = group_pb2.GROUP(
        group_name=group_name, entity_id=mo_ids, tmo_id=tmo_id
    )
    msg_value = msg_value.SerializeToString()

    mocked_msg = KafkaMSGMock(
        msg_key=f"{msg_class_name}:{msg_event}",
        msg_topic=f"{KAFKA_GROUP_BUILDER_GROUP_TOPIC}",
        msg_value=msg_value,
    )

    return mocked_msg
