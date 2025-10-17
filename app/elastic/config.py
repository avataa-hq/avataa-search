# ELASTICSEARCH
import os

ES_HOST = os.environ.get("ES_HOST", "elasticsearch")
ES_PORT = os.environ.get("ES_PORT", "9200")
ES_USER = os.environ.get("ES_USER", "search_user")
ES_PASS = os.environ.get("ES_PASS", None)
ES_PROTOCOL = os.environ.get("ES_PROTOCOL", "https")

ES_URL = f"{ES_PROTOCOL}://{ES_HOST}"
if ES_PORT:
    ES_URL += f":{ES_PORT}"

# v1
INVENTORY_INDEX = "inventory_index"
PARAMS_INDEX = "params"
TMO_INDEX = "tmo_index"
HIERARCHY_INDEX = "hierarchy_index"
PERMISSION_INDEX = "permission_index"

# v2

INVENTORY_TMO_INDEX_V2 = os.environ.get(
    "INVENTORY_TMO_INDEX_V2", "inventory_tmo_index"
)
INVENTORY_TPRM_INDEX_V2 = os.environ.get(
    "INVENTORY_TPRM_INDEX_V2", "inventory_tprm_index"
)
INVENTORY_OBJ_INDEX_PREFIX = os.environ.get(
    "INVENTORY_OBJ_INDEX_PREFIX", "inventory_obj_tmo_"
)
INVENTORY_MO_LINK_INDEX = os.environ.get(
    "INVENTORY_MO_LINK_INDEX", "inventory_mo_link_index"
)
INVENTORY_PRM_LINK_INDEX = os.environ.get(
    "INVENTORY_PRM_LINK_INDEX", "inventory_prm_link_index"
)
ALL_MO_OBJ_INDEXES_PATTERN = f"{INVENTORY_OBJ_INDEX_PREFIX}*"

INVENTORY_PRM_INDEX = os.environ.get(
    "INVENTORY_PRM_INDEX", "inventory_prm_index"
)

DEFAULT_SETTING_FOR_MO_INDEXES = {
    "index.number_of_shards": 2,
    "index.max_terms_count": 2147483646,
    "index.max_result_window": 2000000,
    "index.mapping.total_fields.limit": 10000,
}

DEFAULT_SETTING_FOR_TPRM_INDEX = {
    "index.number_of_shards": 1,
    "index.max_terms_count": 1000000,
    "index.max_result_window": 1000000,
}

DEFAULT_SETTING_FOR_TMO_INDEX = {
    "index.number_of_shards": 1,
    "index.max_terms_count": 100000,
    "index.max_result_window": 100000,
}

DEFAULT_SETTING_FOR_PRM_INDEX = {
    "index.number_of_shards": 10,
    "index.max_terms_count": 2147483646,
    "index.max_result_window": 2000000,
}
