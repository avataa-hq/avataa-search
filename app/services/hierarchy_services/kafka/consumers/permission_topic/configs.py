from services.hierarchy_services.kafka.consumers.permission_topic.events.created import (
    with_hierarchy_permission_created,
)
from services.hierarchy_services.kafka.consumers.permission_topic.events.deleted import (
    with_hierarchy_permission_deleted,
)
from services.hierarchy_services.kafka.consumers.permission_topic.events.updated import (
    with_hierarchy_permission_updated,
)
from services.hierarchy_services.kafka.consumers.permission_topic.models import (
    HierarchyPermissionTopicEvents,
)

EVENT_HANDLERS_BY_PERMISSION_EVENT = {
    HierarchyPermissionTopicEvents.CREATED.value: with_hierarchy_permission_created,
    HierarchyPermissionTopicEvents.UPDATED.value: with_hierarchy_permission_updated,
    HierarchyPermissionTopicEvents.DELETED.value: with_hierarchy_permission_deleted,
}
