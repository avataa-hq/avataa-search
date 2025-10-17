import os

INVENTORY_SECURITY_MO_PERMISSION_INDEX = os.environ.get(
    "INVENTORY_SECURITY_MO_PERMISSION_INDEX", "inventory_security_mo_index"
)

INVENTORY_SECURITY_TMO_PERMISSION_INDEX = os.environ.get(
    "INVENTORY_SECURITY_TMO_PERMISSION_INDEX", "inventory_security_tmo_index"
)

INVENTORY_SECURITY_TPRM_PERMISSION_INDEX = os.environ.get(
    "INVENTORY_SECURITY_TPRM_PERMISSION_INDEX", "inventory_security_tprm_index"
)

INVENTORY_SECURITY_PRM_PERMISSION_INDEX = os.environ.get(
    "INVENTORY_SECURITY_PRM_PERMISSION_INDEX", "inventory_security_prm_index"
)

INVENTORY_SECURITY_MO_PERMISSION_SETTINGS = {
    "index.number_of_shards": 1,
    "index.number_of_replicas": 1,
    "index.max_terms_count": 2147483646,
    "index.max_result_window": 2000000,
    "index.mapping.total_fields.limit": 10000,
    "index.store.preload": ["nvd", "dvd"],
}

INVENTORY_SECURITY_TMO_PERMISSION_SETTINGS = {
    "index.number_of_shards": 1,
    "index.number_of_replicas": 1,
    "index.max_terms_count": 2147483646,
    "index.max_result_window": 2000000,
    "index.mapping.total_fields.limit": 10000,
    "index.store.preload": ["nvd", "dvd"],
}

INVENTORY_SECURITY_TPRM_PERMISSION_SETTINGS = {
    "index.number_of_shards": 1,
    "index.number_of_replicas": 1,
    "index.max_terms_count": 2147483646,
    "index.max_result_window": 2000000,
    "index.mapping.total_fields.limit": 10000,
    "index.store.preload": ["nvd", "dvd"],
}

INVENTORY_SECURITY_PRM_PERMISSION_SETTINGS = {
    "index.number_of_shards": 1,
    "index.number_of_replicas": 1,
    "index.max_terms_count": 2147483646,
    "index.max_result_window": 2000000,
    "index.mapping.total_fields.limit": 10000,
    "index.store.preload": ["nvd", "dvd"],
}
