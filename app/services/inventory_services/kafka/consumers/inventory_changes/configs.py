from kafka_config.utils import ObjEventStatus
from services.inventory_services.kafka.consumers.inventory_changes.events.mo_msg import (
    on_create_mo,
    on_update_mo,
    on_delete_mo,
)
from services.inventory_services.kafka.consumers.inventory_changes.events.prm_msg import (
    on_create_prm,
    on_update_prm,
    on_delete_prm,
)
from services.inventory_services.kafka.consumers.inventory_changes.events.tmo_msg import (
    on_create_tmo,
    on_update_tmo,
    on_delete_tmo,
)
from services.inventory_services.kafka.consumers.inventory_changes.events.tprm_msg import (
    on_create_tprm,
    on_update_tprm,
    on_delete_tprm,
)
from services.inventory_services.protobuf_files.obj_proto import (
    inventory_instances_pb2,
)

INVENTORY_CHANGES_PROTOBUF_DESERIALIZERS = {
    "MO": inventory_instances_pb2.ListMO,
    "TMO": inventory_instances_pb2.ListTMO,
    "TPRM": inventory_instances_pb2.ListTPRM,
    "PRM": inventory_instances_pb2.ListPRM,
}

MO_HANDLERS_BY_MSG_EVENT = {
    ObjEventStatus.CREATED.value: on_create_mo,
    ObjEventStatus.UPDATED.value: on_update_mo,
    ObjEventStatus.DELETED.value: on_delete_mo,
}

PRM_HANDLERS_BY_MSG_EVENT = {
    ObjEventStatus.CREATED.value: on_create_prm,
    ObjEventStatus.UPDATED.value: on_update_prm,
    ObjEventStatus.DELETED.value: on_delete_prm,
}

TMO_HANDLERS_BY_MSG_EVENT = {
    ObjEventStatus.CREATED.value: on_create_tmo,
    ObjEventStatus.UPDATED.value: on_update_tmo,
    ObjEventStatus.DELETED.value: on_delete_tmo,
}

TPRM_HANDLERS_BY_MSG_EVENT = {
    ObjEventStatus.CREATED.value: on_create_tprm,
    ObjEventStatus.UPDATED.value: on_update_tprm,
    ObjEventStatus.DELETED.value: on_delete_tprm,
}

INVENTORY_CHANGES_HANDLER_BY_MSG_CLASS_NAME = {
    "MO": MO_HANDLERS_BY_MSG_EVENT,
    "PRM": PRM_HANDLERS_BY_MSG_EVENT,
    "TMO": TMO_HANDLERS_BY_MSG_EVENT,
    "TPRM": TPRM_HANDLERS_BY_MSG_EVENT,
}
