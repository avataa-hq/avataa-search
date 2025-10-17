INVENTORY_SECURITY_INDEXES_MAPPING = {
    "properties": {
        "id": {"type": "long"},
        "permission": {"type": "keyword"},
        "create": {"type": "boolean"},
        "read": {"type": "boolean"},
        "delete": {"type": "boolean"},
        "update": {"type": "boolean"},
        "root_permission_id": {"type": "long"},
        "permission_name": {"type": "keyword"},
        "admin": {"type": "boolean"},
        "parent_id": {"type": "long"},
        "object_type_id": {"type": "long"},
    }
}
