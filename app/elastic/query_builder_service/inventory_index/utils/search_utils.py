from typing import Union

from indexes_mapping.inventory.mapping import INVENTORY_PARAMETERS_FIELD_NAME


def get_field_name_for_tprm_id(tprm_id: Union[int, str]) -> str:
    """Returns tprm_field_name for specific tprm_id"""
    return f"{INVENTORY_PARAMETERS_FIELD_NAME}.{tprm_id}"
