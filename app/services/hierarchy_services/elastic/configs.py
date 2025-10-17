import os

HIERARCHY_HIERARCHIES_INDEX = os.environ.get(
    "HIERARCHY_HIERARCHIES_INDEX", "hierarchy_hierarchies_index"
)
HIERARCHY_LEVELS_INDEX = os.environ.get(
    "HIERARCHY_LEVELS_INDEX", "hierarchy_levels_index"
)
HIERARCHY_NODE_DATA_INDEX = os.environ.get(
    "HIERARCHY_NODE_DATA_INDEX", "hierarchy_node_data_index"
)
HIERARCHY_OBJ_INDEX = os.environ.get(
    "HIERARCHY_OBJ_INDEX", "hierarchy_obj_index"
)


HIERARCHY_OBJ_INDEX_SETTINGS = {
    "index.number_of_shards": 2,
    "index.number_of_replicas": 1,
    "index.max_terms_count": 2147483646,
    "index.max_result_window": 2000000,
    "index.mapping.total_fields.limit": 10000,
    "index.store.preload": ["nvd", "dvd"],
}


HIERARCHY_HIERARCHIES_INDEX_SETTINGS = {
    "index.number_of_shards": 2,
    "index.number_of_replicas": 1,
    "index.max_terms_count": 2147483646,
    "index.max_result_window": 2000000,
    "index.mapping.total_fields.limit": 10000,
    "index.store.preload": ["nvd", "dvd"],
}

HIERARCHY_LEVELS_INDEX_SETTINGS = {
    "index.number_of_shards": 2,
    "index.number_of_replicas": 1,
    "index.max_terms_count": 2147483646,
    "index.max_result_window": 2000000,
    "index.mapping.total_fields.limit": 10000,
    "index.store.preload": ["nvd", "dvd"],
}


HIERARCHY_NODE_DATA_INDEX_SETTINGS = {
    "index.number_of_shards": 2,
    "index.number_of_replicas": 1,
    "index.max_terms_count": 2147483646,
    "index.max_result_window": 2000000,
    "index.mapping.total_fields.limit": 10000,
    "index.store.preload": ["nvd", "dvd"],
}
