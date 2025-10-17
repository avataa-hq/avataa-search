from enum import Enum

from services.hierarchy_services.kafka.consumers.changes_topic.events.hierarchy.created import (
    on_create_hierarchy,
)
from services.hierarchy_services.kafka.consumers.changes_topic.events.hierarchy.deleted import (
    on_delete_hierarchy,
)
from services.hierarchy_services.kafka.consumers.changes_topic.events.hierarchy.updated import (
    on_update_hierarchy,
)
from services.hierarchy_services.kafka.consumers.changes_topic.events.level.created import (
    on_create_level,
)
from services.hierarchy_services.kafka.consumers.changes_topic.events.level.deleted import (
    on_delete_level,
)
from services.hierarchy_services.kafka.consumers.changes_topic.events.level.updated import (
    on_update_level,
)
from services.hierarchy_services.kafka.consumers.changes_topic.events.node_data.created import (
    on_create_node_data,
)
from services.hierarchy_services.kafka.consumers.changes_topic.events.node_data.deleted import (
    on_delete_node_data,
)
from services.hierarchy_services.kafka.consumers.changes_topic.events.node_data.updated import (
    on_update_node_data,
)
from services.hierarchy_services.kafka.consumers.changes_topic.events.obj.created import (
    on_create_obj,
)
from services.hierarchy_services.kafka.consumers.changes_topic.events.obj.deleted import (
    on_delete_obj,
)
from services.hierarchy_services.kafka.consumers.changes_topic.events.obj.updated import (
    on_update_obj,
)
from services.hierarchy_services.kafka.consumers.changes_topic.events.permission.created import (
    on_create_hierarchy_permission,
)
from services.hierarchy_services.kafka.consumers.changes_topic.events.permission.deleted import (
    on_delete_hierarchy_permission,
)
from services.hierarchy_services.kafka.consumers.changes_topic.events.permission.updated import (
    on_update_hierarchy_permission,
)
from services.hierarchy_services.kafka.consumers.changes_topic.protobuf import (
    hierarchy_producer_msg_pb2,
)


class HierarchyMessageType(Enum):
    HIERARCHY = "Hierarchy"
    LEVEL = "Level"
    OBJ = "Obj"
    NODE_DATA = "NodeData"
    HIERARCHY_PERMISSION = "HierarchyPermission"


class ObjEventStatus(Enum):
    CREATED = "created"
    UPDATED = "updated"
    DELETED = "deleted"


HIERARCHY_CHANGES_PROTOBUF_DESERIALIZERS = {
    HierarchyMessageType.HIERARCHY.value: hierarchy_producer_msg_pb2.ListHierarchy,
    HierarchyMessageType.LEVEL.value: hierarchy_producer_msg_pb2.ListLevel,
    HierarchyMessageType.OBJ.value: hierarchy_producer_msg_pb2.ListNode,
    HierarchyMessageType.NODE_DATA.value: hierarchy_producer_msg_pb2.ListNodeData,
    HierarchyMessageType.HIERARCHY_PERMISSION.value: hierarchy_producer_msg_pb2.ListHierarchyPermission,
}


HIERARCHY_HANDLERS_BY_MSG_EVENT = {
    ObjEventStatus.CREATED.value: on_create_hierarchy,
    ObjEventStatus.UPDATED.value: on_update_hierarchy,
    ObjEventStatus.DELETED.value: on_delete_hierarchy,
}

LEVEL_HANDLERS_BY_MSG_EVENT = {
    ObjEventStatus.CREATED.value: on_create_level,
    ObjEventStatus.UPDATED.value: on_update_level,
    ObjEventStatus.DELETED.value: on_delete_level,
}

OBJ_HANDLERS_BY_MSG_EVENT = {
    ObjEventStatus.CREATED.value: on_create_obj,
    ObjEventStatus.UPDATED.value: on_update_obj,
    ObjEventStatus.DELETED.value: on_delete_obj,
}

NODE_DATA_HANDLERS_BY_MSG_EVENT = {
    ObjEventStatus.CREATED.value: on_create_node_data,
    ObjEventStatus.UPDATED.value: on_update_node_data,
    ObjEventStatus.DELETED.value: on_delete_node_data,
}

HIERARCHY_PERMISSION_HANDLERS_BY_MSG_EVENT = {
    ObjEventStatus.CREATED.value: on_create_hierarchy_permission,
    ObjEventStatus.UPDATED.value: on_update_hierarchy_permission,
    ObjEventStatus.DELETED.value: on_delete_hierarchy_permission,
}


HIERARCHY_CHANGES_HANDLER_BY_MSG_CLASS_NAME = {
    HierarchyMessageType.HIERARCHY.value: HIERARCHY_HANDLERS_BY_MSG_EVENT,
    HierarchyMessageType.LEVEL.value: LEVEL_HANDLERS_BY_MSG_EVENT,
    HierarchyMessageType.OBJ.value: OBJ_HANDLERS_BY_MSG_EVENT,
    HierarchyMessageType.NODE_DATA.value: NODE_DATA_HANDLERS_BY_MSG_EVENT,
    HierarchyMessageType.HIERARCHY_PERMISSION.value: HIERARCHY_PERMISSION_HANDLERS_BY_MSG_EVENT,
}
