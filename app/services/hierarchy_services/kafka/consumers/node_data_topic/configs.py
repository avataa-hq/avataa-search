from services.hierarchy_services.kafka.consumers.node_data_topic.events.created import (
    with_node_data_created,
)
from services.hierarchy_services.kafka.consumers.node_data_topic.events.deleted import (
    with_node_data_deleted,
)
from services.hierarchy_services.kafka.consumers.node_data_topic.events.updated import (
    with_node_data_updated,
)
from services.hierarchy_services.kafka.consumers.node_data_topic.models import (
    HierarchyNodeDataTopicEvents,
)

EVENT_HANDLERS_BY_NODE_DATA_EVENT = {
    HierarchyNodeDataTopicEvents.CREATED.value: with_node_data_created,
    HierarchyNodeDataTopicEvents.UPDATED.value: with_node_data_updated,
    HierarchyNodeDataTopicEvents.DELETED.value: with_node_data_deleted,
}
