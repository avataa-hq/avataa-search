from services.group_builder.kafka.consumers.statistic.enum_models import (
    GroupStatisticEvents,
)
from services.group_builder.kafka.consumers.statistic.events.create import (
    with_create_statistic,
)
from services.group_builder.kafka.consumers.statistic.events.delete import (
    with_delete_statistic,
)
from services.group_builder.kafka.consumers.statistic.events.update import (
    with_update_statistic,
)
from services.group_builder.kafka.consumers.statistic.protobuf import (
    statistic_pb2,
)

GROUP_STATISTIC_TOPIC_PROTOBUF_DESERIALIZERS = {
    "group_statistic": statistic_pb2.Statistic,
}

GROUP_STATISTIC_HANDLERS_BY_MSG_EVENT = {
    GroupStatisticEvents.CREATE.value: with_create_statistic,
    GroupStatisticEvents.UPDATE.value: with_update_statistic,
    GroupStatisticEvents.DELETE.value: with_delete_statistic,
}
