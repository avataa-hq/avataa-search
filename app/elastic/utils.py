import time

from elasticsearch import Elasticsearch, AsyncElasticsearch, ApiError

from elastic.config import (
    INVENTORY_TMO_INDEX_V2,
    INVENTORY_TPRM_INDEX_V2,
    INVENTORY_MO_LINK_INDEX,
    INVENTORY_PRM_LINK_INDEX,
    INVENTORY_PRM_INDEX,
    DEFAULT_SETTING_FOR_TMO_INDEX,
    DEFAULT_SETTING_FOR_TPRM_INDEX,
    DEFAULT_SETTING_FOR_PRM_INDEX,
)
from indexes_mapping.inventory.mapping import (
    INVENTORY_TMO_INDEX_MAPPING,
    INVENTORY_TPRM_INDEX_MAPPING,
    INVENTORY_PRM_INDEX_MAPPING,
    INVENTORY_PRM_AND_MO_LINK_INDEX_MAPPING,
)
from services.hierarchy_services.elastic.configs import (
    HIERARCHY_HIERARCHIES_INDEX,
    HIERARCHY_HIERARCHIES_INDEX_SETTINGS,
    HIERARCHY_LEVELS_INDEX,
    HIERARCHY_LEVELS_INDEX_SETTINGS,
    HIERARCHY_NODE_DATA_INDEX,
    HIERARCHY_NODE_DATA_INDEX_SETTINGS,
    HIERARCHY_OBJ_INDEX,
    HIERARCHY_OBJ_INDEX_SETTINGS,
)
from services.hierarchy_services.elastic.mapping import (
    HIERARCHY_HIERARCHIES_INDEX_MAPPING,
    HIERARCHY_LEVEL_INDEX_MAPPING,
    HIERARCHY_NODE_DATA_INDEX_MAPPING,
    HIERARCHY_OBJ_INDEX_MAPPING,
)
from services.inventory_services.elastic.security.configs import (
    INVENTORY_SECURITY_MO_PERMISSION_INDEX,
    INVENTORY_SECURITY_MO_PERMISSION_SETTINGS,
    INVENTORY_SECURITY_TMO_PERMISSION_INDEX,
    INVENTORY_SECURITY_TMO_PERMISSION_SETTINGS,
    INVENTORY_SECURITY_TPRM_PERMISSION_INDEX,
    INVENTORY_SECURITY_TPRM_PERMISSION_SETTINGS,
    INVENTORY_SECURITY_PRM_PERMISSION_SETTINGS,
    INVENTORY_SECURITY_PRM_PERMISSION_INDEX,
)
from services.inventory_services.elastic.security.mapping import (
    INVENTORY_SECURITY_INDEXES_MAPPING,
)
from services.loader import load_objects


def convert_int_to_str(value):
    if isinstance(value, int):
        return str(value)

    return value


def init_index(
    client: Elasticsearch, index: str, title: str, mappings: dict | None = None
):
    print(f"{title} clean...")
    if client.indices.exists(index=index):
        client.indices.delete(index=index)
    client.indices.create(index=index, mappings=mappings)

    print(f"{title} loading...")
    start = time.time()
    load_objects(client, index.split("_")[0])
    print(f"loading time: {time.time() - start}")


async def init_all_necessary_indexes(async_client: AsyncElasticsearch):
    """Creates all necessary indexes if they are no exist"""

    elastic_connected = False

    all_indexes = "*"
    search_query = {"match_all": {}}
    print("Check shards")
    while not elastic_connected:
        try:
            res = await async_client.search(
                index=all_indexes, query=search_query, size=0
            )
            time.sleep(5)
        except ApiError as ex:
            print(ex, ex.body["error"]["reason"])
            time.sleep(5)
        except Exception as e:
            print(e)
            time.sleep(5)
        else:
            print(res["_shards"])
            if res["_shards"]["failed"] == 0:
                elastic_connected = True
    print("Get all indexes")
    all_indexes = await async_client.indices.get_alias(index="*")
    all_indexes = set(all_indexes)

    main_idexes_and_configs = {
        INVENTORY_TMO_INDEX_V2: {
            "settings": DEFAULT_SETTING_FOR_TMO_INDEX,
            "mappings": INVENTORY_TMO_INDEX_MAPPING,
        },
        INVENTORY_TPRM_INDEX_V2: {
            "settings": DEFAULT_SETTING_FOR_TPRM_INDEX,
            "mappings": INVENTORY_TPRM_INDEX_MAPPING,
        },
        INVENTORY_PRM_INDEX: {
            "settings": DEFAULT_SETTING_FOR_PRM_INDEX,
            "mappings": INVENTORY_PRM_INDEX_MAPPING,
        },
        INVENTORY_PRM_LINK_INDEX: {
            "settings": DEFAULT_SETTING_FOR_PRM_INDEX,
            "mappings": INVENTORY_PRM_AND_MO_LINK_INDEX_MAPPING,
        },
        INVENTORY_MO_LINK_INDEX: {
            "settings": DEFAULT_SETTING_FOR_PRM_INDEX,
            "mappings": INVENTORY_PRM_AND_MO_LINK_INDEX_MAPPING,
        },
        HIERARCHY_HIERARCHIES_INDEX: {
            "settings": HIERARCHY_HIERARCHIES_INDEX_SETTINGS,
            "mappings": HIERARCHY_HIERARCHIES_INDEX_MAPPING,
        },
        HIERARCHY_LEVELS_INDEX: {
            "settings": HIERARCHY_LEVELS_INDEX_SETTINGS,
            "mappings": HIERARCHY_LEVEL_INDEX_MAPPING,
        },
        HIERARCHY_OBJ_INDEX: {
            "settings": HIERARCHY_OBJ_INDEX_SETTINGS,
            "mappings": HIERARCHY_OBJ_INDEX_MAPPING,
        },
        HIERARCHY_NODE_DATA_INDEX: {
            "settings": HIERARCHY_NODE_DATA_INDEX_SETTINGS,
            "mappings": HIERARCHY_NODE_DATA_INDEX_MAPPING,
        },
        INVENTORY_SECURITY_MO_PERMISSION_INDEX: {
            "settings": INVENTORY_SECURITY_MO_PERMISSION_SETTINGS,
            "mappings": INVENTORY_SECURITY_INDEXES_MAPPING,
        },
        INVENTORY_SECURITY_TMO_PERMISSION_INDEX: {
            "settings": INVENTORY_SECURITY_TMO_PERMISSION_SETTINGS,
            "mappings": INVENTORY_SECURITY_INDEXES_MAPPING,
        },
        INVENTORY_SECURITY_TPRM_PERMISSION_INDEX: {
            "settings": INVENTORY_SECURITY_TPRM_PERMISSION_SETTINGS,
            "mappings": INVENTORY_SECURITY_INDEXES_MAPPING,
        },
        INVENTORY_SECURITY_PRM_PERMISSION_INDEX: {
            "settings": INVENTORY_SECURITY_PRM_PERMISSION_SETTINGS,
            "mappings": INVENTORY_SECURITY_INDEXES_MAPPING,
        },
    }

    for index_name, conf_data in main_idexes_and_configs.items():
        if index_name not in all_indexes:
            print(f"Create index {index_name} - star")
            await async_client.indices.create(
                index=index_name,
                mappings=conf_data["mappings"],
                settings=conf_data["settings"],
            )
            print(f"Create index {index_name} - end")
