from services.hierarchy_services.kafka.consumers.level_topic.events.created import (
    with_level_created,
)
from services.hierarchy_services.kafka.consumers.level_topic.events.deleted import (
    with_level_deleted,
)
from services.hierarchy_services.kafka.consumers.level_topic.events.updated import (
    with_level_updated,
)
from services.hierarchy_services.kafka.consumers.level_topic.models import (
    HierarchyLevelsTopicEvents,
)

EVENT_HANDLERS_BY_LEVEL_EVENT = {
    HierarchyLevelsTopicEvents.CREATED.value: with_level_created,
    HierarchyLevelsTopicEvents.UPDATED.value: with_level_updated,
    HierarchyLevelsTopicEvents.DELETED.value: with_level_deleted,
}
