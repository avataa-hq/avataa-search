from indexes_mapping.inventory.zeebe_enums import ZeebeProcessInstanceFields
from services.group_builder.models import GroupStatisticUniqueFields
from services.inventory_services.models import (
    InventoryMODefaultFields,
    InventoryMOProcessedFields,
    InventoryMOAdditionalFields,
    InventoryFuzzySearchFields,
)

INVENTORY_PARAMETERS_FIELD_NAME = "parameters"
INVENTORY_PERMISSIONS_FIELD_NAME = "permissions"
INVENTORY_FUZZY_FIELD_NAME = "fuzzy_search_fields"

INVENTORY_OBJ_INDEX_MAPPING = {
    "properties": {
        InventoryMODefaultFields.ID.value: {"type": "long"},
        InventoryMODefaultFields.P_ID.value: {"type": "long"},
        InventoryMOProcessedFields.PARENT_NAME.value: {"type": "keyword"},
        InventoryMODefaultFields.NAME.value: {"type": "keyword"},
        InventoryMODefaultFields.TMO_ID.value: {"type": "long"},
        InventoryMODefaultFields.ACTIVE.value: {"type": "boolean"},
        InventoryMODefaultFields.VERSION.value: {"type": "long"},
        InventoryMODefaultFields.POINT_A_ID.value: {"type": "long"},
        InventoryMODefaultFields.POINT_B_ID.value: {"type": "long"},
        InventoryMODefaultFields.LATITUDE.value: {"type": "double"},
        InventoryMODefaultFields.LONGITUDE.value: {"type": "double"},
        InventoryMODefaultFields.STATUS.value: {"type": "keyword"},
        InventoryMODefaultFields.GEOMETRY.value: {
            "type": "object",
            "properties": {"path": {"type": "geo_shape"}},
        },
        InventoryMODefaultFields.MODEL.value: {"type": "keyword"},
        InventoryMODefaultFields.POV.value: {"type": "object"},
        InventoryMODefaultFields.DOCUMENT_COUNT.value: {"type": "long"},
        ZeebeProcessInstanceFields.PROCESS_INSTANCE_ID.value: {"type": "long"},
        ZeebeProcessInstanceFields.PROCESS_DEFINITION_KEY.value: {
            "type": "keyword"
        },
        ZeebeProcessInstanceFields.PROCESS_DEFINITION_ID.value: {
            "type": "long"
        },
        ZeebeProcessInstanceFields.START_DATE.value: {"type": "date"},
        ZeebeProcessInstanceFields.STATE.value: {"type": "keyword"},
        ZeebeProcessInstanceFields.END_DATE.value: {"type": "date"},
        ZeebeProcessInstanceFields.DURATION.value: {"type": "long"},
        INVENTORY_PARAMETERS_FIELD_NAME: {"type": "object"},
        InventoryMODefaultFields.CREATION_DATE.value: {"type": "date"},
        InventoryMODefaultFields.MODIFICATION_DATE.value: {"type": "date"},
        GroupStatisticUniqueFields.GROUP_NAME.value: {"type": "keyword"},
        GroupStatisticUniqueFields.GROUP_TYPE.value: {"type": "keyword"},
        InventoryMOAdditionalFields.GROUPS.value: {"type": "keyword"},
        InventoryMOProcessedFields.POINT_A_NAME.value: {"type": "keyword"},
        InventoryMOProcessedFields.POINT_B_NAME.value: {"type": "keyword"},
        INVENTORY_PERMISSIONS_FIELD_NAME: {"type": "keyword"},
        InventoryMODefaultFields.LABEL.value: {"type": "keyword"},
        INVENTORY_FUZZY_FIELD_NAME: {
            "type": "object",
            "properties": {
                InventoryFuzzySearchFields.NAME.value: {"type": "text"}
            },
        },
    }
}

INVENTORY_TMO_INDEX_MAPPING = {
    "properties": {
        "id": {"type": "long"},
        "name": {"type": "keyword"},
        "p_id": {"type": "long"},
        "icon": {"type": "keyword"},
        "description": {"type": "keyword"},
        "virtual": {"type": "boolean"},
        "global_uniqueness": {"type": "boolean"},
        "lifecycle_process_definition": {"type": "keyword"},
        "severity_id": {"type": "long"},
        "geometry_type": {"type": "keyword"},
        "materialize": {"type": "boolean"},
        "version": {"type": "long"},
        "latitude": {"type": "long"},
        "longitude": {"type": "long"},
        "status": {"type": "long"},
        "created_by": {"type": "keyword"},
        "modified_by": {"type": "keyword"},
        "creation_date": {"type": "date"},
        "modification_date": {"type": "date"},
        "primary": {"type": "long"},
        "points_constraint_by_tmo": {"type": "long"},
        "minimize": {"type": "boolean"},
        INVENTORY_PERMISSIONS_FIELD_NAME: {"type": "keyword"},
        "label": {"type": "long"},
    }
}

INVENTORY_TPRM_INDEX_MAPPING = {
    "properties": {
        "id": {"type": "long"},
        "name": {"type": "keyword"},
        "description": {"type": "keyword"},
        "val_type": {"type": "keyword"},
        "multiple": {"type": "boolean"},
        "required": {"type": "boolean"},
        "returnable": {"type": "boolean"},
        "constraint": {"type": "keyword"},
        "prm_link_filter": {"type": "keyword"},
        "group": {"type": "keyword"},
        "tmo_id": {"type": "long"},
        "version": {"type": "long"},
        "created_by": {"type": "keyword"},
        "modified_by": {"type": "keyword"},
        "creation_date": {"type": "date"},
        "modification_date": {"type": "date"},
        "line_type": {"type": "keyword"},
        INVENTORY_PERMISSIONS_FIELD_NAME: {"type": "keyword"},
    }
}

INVENTORY_PRM_INDEX_MAPPING = {
    "properties": {
        "id": {"type": "long"},
        "mo_id": {"type": "long"},
        "tprm_id": {"type": "long"},
        "version": {"type": "long"},
        "value": {"type": "keyword"},
    }
}

INVENTORY_PRM_AND_MO_LINK_INDEX_MAPPING = {
    "properties": {
        "id": {"type": "long"},
        "mo_id": {"type": "long"},
        "tprm_id": {"type": "long"},
        "version": {"type": "long"},
        "value": {"type": "long"},
    }
}
