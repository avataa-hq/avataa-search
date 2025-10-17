from services.hierarchy_services.kafka.consumers.obj_topic.events.created import (
    with_node_created,
)
from services.hierarchy_services.kafka.consumers.obj_topic.events.deleted import (
    with_node_deleted,
)
from services.hierarchy_services.kafka.consumers.obj_topic.events.updated import (
    with_node_updated,
)
from services.hierarchy_services.kafka.consumers.obj_topic.models import (
    HierarchyObjTopicEvents,
)

EVENT_HANDLERS_BY_OBJ_EVENT = {
    HierarchyObjTopicEvents.CREATED.value: with_node_created,
    HierarchyObjTopicEvents.UPDATED.value: with_node_updated,
    HierarchyObjTopicEvents.DELETED.value: with_node_deleted,
}
