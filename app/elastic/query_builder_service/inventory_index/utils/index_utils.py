from typing import List

from elastic.config import INVENTORY_OBJ_INDEX_PREFIX


def get_index_name_by_tmo(tmo_id: int):
    """Returns index name for special tmo_id"""
    return f"{INVENTORY_OBJ_INDEX_PREFIX}{tmo_id}_index"


def get_index_names_fo_list_of_tmo_ids(tmo_ids=List[int]):
    """Returns list of index names for special tmo_ids"""
    return [get_index_name_by_tmo(tmo_id) for tmo_id in tmo_ids]
