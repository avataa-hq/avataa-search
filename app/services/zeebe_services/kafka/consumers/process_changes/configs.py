from services.zeebe_services.kafka.consumers.process_changes.events.create import (
    with_create_process,
)
from services.zeebe_services.kafka.consumers.process_changes.protobuf import (
    kafka_process_pb2,
)


PROCESS_CHANGES_PROTOBUF_DESERIALIZERS = {
    "Process": kafka_process_pb2.Process,
}

PROCESS_HANDLER_BY_MSG_EVENT = {"created": with_create_process}

PROCESS_CHANGES_HANDLER_BY_MSG_CLASS_NAME = {
    "Process": PROCESS_HANDLER_BY_MSG_EVENT
}
