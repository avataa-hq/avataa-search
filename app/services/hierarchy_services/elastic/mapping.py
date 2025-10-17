HIERARCHY_PERMISSIONS_FIELD_NAME = "permissions"

HIERARCHY_HIERARCHIES_INDEX_MAPPING = {
    "properties": {
        "id": {"type": "long"},
        "name": {"type": "keyword"},
        "description": {"type": "keyword"},
        "author": {"type": "keyword"},
        "created": {"type": "date"},
        "change_author": {"type": "keyword"},
        "modified": {"type": "date"},
        "create_empty_nodes": {"type": "boolean"},
        "status": {"type": "keyword"},
        HIERARCHY_PERMISSIONS_FIELD_NAME: {"type": "keyword"},
    }
}

HIERARCHY_OBJ_INDEX_MAPPING = {
    "properties": {
        "id": {"type": "keyword"},
        "hierarchy_id": {"type": "long"},
        "parent_id": {"type": "keyword"},
        "key": {"type": "keyword"},
        "object_id": {"type": "long"},
        "additional_params": {"type": "keyword"},
        "level": {"type": "long"},
        "latitude": {"type": "double"},
        "longitude": {"type": "double"},
        "child_count": {"type": "long"},
        "object_type_id": {"type": "long"},
        "level_id": {"type": "long"},
        "active": {"type": "boolean"},
        "path": {"type": "keyword"},
        "key_is_empty": {"type": "boolean"},
    }
}

HIERARCHY_LEVEL_INDEX_MAPPING = {
    "properties": {
        "id": {"type": "long"},
        "parent_id": {"type": "long"},
        "hierarchy_id": {"type": "long"},
        "level": {"type": "long"},
        "name": {"type": "keyword"},
        "description": {"type": "keyword"},
        "object_type_id": {"type": "long"},
        "is_virtual": {"type": "boolean"},
        "param_type_id": {"type": "long"},
        "additional_params_id": {"type": "long"},
        "latitude_id": {"type": "long"},
        "longitude_id": {"type": "long"},
        "author": {"type": "keyword"},
        "created": {"type": "date"},
        "change_author": {"type": "keyword"},
        "modified": {"type": "date"},
        "show_without_children": {"type": "boolean"},
        "key_attrs": {"type": "keyword"},
    }
}

HIERARCHY_NODE_DATA_INDEX_MAPPING = {
    "properties": {
        "id": {"type": "long"},
        "level_id": {"type": "long"},
        "node_id": {"type": "keyword"},
        "mo_id": {"type": "long"},
        "mo_name": {"type": "keyword"},
        "mo_latitude": {"type": "double"},
        "mo_longitude": {"type": "double"},
        "mo_status": {"type": "keyword"},
        "mo_tmo_id": {"type": "long"},
        "mo_p_id": {"type": "long"},
        "mo_active": {"type": "boolean"},
        "unfolded_key": {"type": "object"},
    }
}
