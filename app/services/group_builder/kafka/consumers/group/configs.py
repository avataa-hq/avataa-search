from services.group_builder.kafka.consumers.group.enum_models import GroupEvents
from services.group_builder.kafka.consumers.group.events.add import (
    with_group_add,
)
from services.group_builder.kafka.consumers.group.events.delete import (
    with_group_delete,
)
from services.group_builder.kafka.consumers.group.events.remove import (
    with_group_remove,
)
from services.group_builder.kafka.consumers.group.protobuf import group_pb2

GROUP_TOPIC_PROTOBUF_DESERIALIZERS = {
    "group": group_pb2.GROUP,
}

GROUP_HANDLERS_BY_MSG_EVENT = {
    GroupEvents.ADD.value: with_group_add,
    GroupEvents.REMOVE.value: with_group_remove,
    GroupEvents.DELETE.value: with_group_delete,
}

INVENTORY_GROUP_TYPE = "object_group"
PM_GROUP_TYPE = "process_group"
