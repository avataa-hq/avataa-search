from kafka_config.config import (
    KAFKA_INVENTORY_CHANGES_TOPIC,
    KAFKA_ZEEBE_CHANGES_TOPIC,
    KAFKA_ZEEBE_PROCESS_INSTANCE_EXPORTER_TOPIC,
    KAFKA_HIERARCHY_HIERARCHIES_CHANGES_TOPIC,
    KAFKA_HIERARCHY_LEVELS_CHANGES_TOPIC,
    KAFKA_HIERARCHY_OBJ_CHANGES_TOPIC,
    KAFKA_GROUP_BUILDER_GROUP_TOPIC,
    KAFKA_GROUP_STATISTIC_TOPIC,
    KAFKA_INVENTORY_SECURITY_TOPIC,
    KAFKA_HIERARCHY_NODE_DATA_CHANGES_TOPIC,
    KAFKA_HIERARCHY_PERMISSIONS_CHANGES_TOPIC,
    KAFKA_HIERARCHY_CHANGES_TOPIC,
)
from services.group_builder.kafka.consumers.group.utils import GroupTopicHandler
from services.group_builder.kafka.consumers.statistic.utils import (
    GroupStatisticTopicHandler,
)
from services.hierarchy_services.kafka.consumers.changes_topic.utils import (
    HierarchyChangesTopicHandler,
)
from services.hierarchy_services.kafka.consumers.hierarchy_topic.utils import (
    HierarchyHierarchiesChangesTopicHandler,
)
from services.hierarchy_services.kafka.consumers.level_topic.utils import (
    HierarchyLevelsChangesTopicHandler,
)
from services.hierarchy_services.kafka.consumers.node_data_topic.utils import (
    HierarchyNodeDataTopicHandler,
)
from services.hierarchy_services.kafka.consumers.obj_topic.utils import (
    HierarchyObjChangesTopicHandler,
)
from services.hierarchy_services.kafka.consumers.permission_topic.utils import (
    HierarchyPermissionChangesTopicHandler,
)
from services.inventory_services.kafka.consumers.inventory_changes.utils import (
    InventoryChangesHandler,
)
from services.inventory_services.kafka.consumers.inventory_security.utils import (
    InventorySecurityHandler,
)
from services.zeebe_services.kafka.consumers.process_changes.utils import (
    ProcessChangesHandler,
)
from services.zeebe_services.kafka.consumers.process_instance_exporter.utils import (
    ProcessInstanceChangesHandler,
)

MSG_HANDLERS_BY_MSG_TOPIC = dict()

if KAFKA_INVENTORY_CHANGES_TOPIC:
    MSG_HANDLERS_BY_MSG_TOPIC[KAFKA_INVENTORY_CHANGES_TOPIC] = (
        InventoryChangesHandler
    )

if KAFKA_ZEEBE_CHANGES_TOPIC:
    MSG_HANDLERS_BY_MSG_TOPIC[KAFKA_ZEEBE_CHANGES_TOPIC] = ProcessChangesHandler

if KAFKA_ZEEBE_PROCESS_INSTANCE_EXPORTER_TOPIC:
    MSG_HANDLERS_BY_MSG_TOPIC[KAFKA_ZEEBE_PROCESS_INSTANCE_EXPORTER_TOPIC] = (
        ProcessInstanceChangesHandler
    )

if KAFKA_HIERARCHY_HIERARCHIES_CHANGES_TOPIC:
    MSG_HANDLERS_BY_MSG_TOPIC[KAFKA_HIERARCHY_HIERARCHIES_CHANGES_TOPIC] = (
        HierarchyHierarchiesChangesTopicHandler
    )

if KAFKA_HIERARCHY_LEVELS_CHANGES_TOPIC:
    MSG_HANDLERS_BY_MSG_TOPIC[KAFKA_HIERARCHY_LEVELS_CHANGES_TOPIC] = (
        HierarchyLevelsChangesTopicHandler
    )

if KAFKA_HIERARCHY_OBJ_CHANGES_TOPIC:
    MSG_HANDLERS_BY_MSG_TOPIC[KAFKA_HIERARCHY_OBJ_CHANGES_TOPIC] = (
        HierarchyObjChangesTopicHandler
    )

if KAFKA_HIERARCHY_NODE_DATA_CHANGES_TOPIC:
    MSG_HANDLERS_BY_MSG_TOPIC[KAFKA_HIERARCHY_NODE_DATA_CHANGES_TOPIC] = (
        HierarchyNodeDataTopicHandler
    )

if KAFKA_HIERARCHY_PERMISSIONS_CHANGES_TOPIC:
    MSG_HANDLERS_BY_MSG_TOPIC[KAFKA_HIERARCHY_PERMISSIONS_CHANGES_TOPIC] = (
        HierarchyPermissionChangesTopicHandler
    )

if KAFKA_HIERARCHY_CHANGES_TOPIC and not any(
    [
        KAFKA_HIERARCHY_HIERARCHIES_CHANGES_TOPIC,
        KAFKA_HIERARCHY_LEVELS_CHANGES_TOPIC,
        KAFKA_HIERARCHY_OBJ_CHANGES_TOPIC,
        KAFKA_HIERARCHY_NODE_DATA_CHANGES_TOPIC,
        KAFKA_HIERARCHY_PERMISSIONS_CHANGES_TOPIC,
    ]
):
    MSG_HANDLERS_BY_MSG_TOPIC[KAFKA_HIERARCHY_CHANGES_TOPIC] = (
        HierarchyChangesTopicHandler
    )

if KAFKA_GROUP_BUILDER_GROUP_TOPIC:
    MSG_HANDLERS_BY_MSG_TOPIC[KAFKA_GROUP_BUILDER_GROUP_TOPIC] = (
        GroupTopicHandler
    )

if KAFKA_GROUP_STATISTIC_TOPIC:
    MSG_HANDLERS_BY_MSG_TOPIC[KAFKA_GROUP_STATISTIC_TOPIC] = (
        GroupStatisticTopicHandler
    )

if KAFKA_INVENTORY_SECURITY_TOPIC:
    MSG_HANDLERS_BY_MSG_TOPIC[KAFKA_INVENTORY_SECURITY_TOPIC] = (
        InventorySecurityHandler
    )
