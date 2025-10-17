from services.hierarchy_services.kafka.consumers.hierarchy_topic.events.created import (
    with_hierarchy_created,
)
from services.hierarchy_services.kafka.consumers.hierarchy_topic.events.deleted import (
    with_hierarchy_deleted,
)
from services.hierarchy_services.kafka.consumers.hierarchy_topic.events.updated import (
    with_hierarchy_updated,
)
from services.hierarchy_services.kafka.consumers.hierarchy_topic.models import (
    HierarchyHierarchiesTopicEvents,
)

EVENT_HANDLERS_BY_HIERARCHY_EVENT = {
    HierarchyHierarchiesTopicEvents.CREATED.value: with_hierarchy_created,
    HierarchyHierarchiesTopicEvents.UPDATED.value: with_hierarchy_updated,
    HierarchyHierarchiesTopicEvents.DELETED.value: with_hierarchy_deleted,
}
