from typing import List, Iterable, Set

from dateutil.parser import parse, ParserError

from elastic.enum_models import (
    InventoryFieldValType,
    SearchOperator,
    ElasticFieldValType,
)


def is_integer(value: str):
    try:
        int(value)
    except ValueError:
        return False
    return True


def is_float(value: str):
    try:
        float(value)
    except ValueError:
        return False
    return True


def is_bool(value: str):
    if value.lower() in {"true", "false"}:
        return True
    return False


def is_date(value: str):
    if len(value) > 10:
        return False

    try:
        parse(value)
    except ParserError:
        return False
    return True


def is_datetime(value: str):
    try:
        parse(value)
    except ParserError:
        return False
    return True


check_type_of_str_data_order = {
    "bool": is_bool,
    "int": is_integer,
    "float": is_float,
    "date": is_date,
    "datetime": is_datetime,
}


def find_out_inventory_type_of_inputted_data(input_data: str):
    """Returns type of inputted data"""
    for val_type, validator in check_type_of_str_data_order.items():
        if validator(input_data):
            return val_type
    return "str"


tprm_val_types_check_on_input_data_type = {
    "bool": {
        InventoryFieldValType.STR.value,
        InventoryFieldValType.BOOL.value,
        InventoryFieldValType.FORMULA.value,
        InventoryFieldValType.USER_LINK.value,
    },
    "int": {
        InventoryFieldValType.STR.value,
        InventoryFieldValType.INT.value,
        InventoryFieldValType.SEQUENCE.value,
    },
    "float": {
        InventoryFieldValType.STR.value,
        InventoryFieldValType.FLOAT.value,
    },
    "date": {
        InventoryFieldValType.STR.value,
        InventoryFieldValType.DATE.value,
        InventoryFieldValType.FORMULA.value,
        InventoryFieldValType.USER_LINK.value,
    },
    "datetime": {
        InventoryFieldValType.STR.value,
        InventoryFieldValType.DATETIME.value,
        InventoryFieldValType.FORMULA.value,
        InventoryFieldValType.USER_LINK.value,
    },
    "str": {
        InventoryFieldValType.STR.value,
        InventoryFieldValType.FORMULA.value,
        InventoryFieldValType.USER_LINK.value,
        InventoryFieldValType.MO_LINK.value,
        InventoryFieldValType.TWO_WAY_MO_LINK.value,
        InventoryFieldValType.ENUM.value,
    },
}


def get_tprm_val_types_need_to_check_in_global_search_by_inputted_data(
    input_data: str,
) -> Set[str]:
    """Returns a list of TPRM val_types that should be searched globally."""
    val_type = find_out_inventory_type_of_inputted_data(input_data)
    res = tprm_val_types_check_on_input_data_type.get(val_type)
    if res is None:
        raise NotImplementedError(
            f"Global search is not implemented for val_type '{val_type}' "
        )
    return res


def get_query_to_find_tprms_by_val_types(
    val_types: Iterable[str],
    tmo_ids: List[int] = None,
    only_returnable: bool = True,
    client_permissions: List[str] = None,
) -> dict:
    """Returns a query as a dict to find the required TPRMs."""
    search_conditions = [{"terms": {"val_type": list(val_types)}}]

    if tmo_ids:
        search_conditions.append({"terms": {"tmo_id": tmo_ids}})

    if only_returnable:
        search_conditions.append({"match": {"returnable": True}})

    if client_permissions:
        # TODO : remove comment to enable tprm permissions
        # search_conditions.append({"terms": {INVENTORY_PERMISSIONS_FIELD_NAME: client_permissions}})
        pass

    return {"bool": {"must": search_conditions}}


operator_for_global_search_by_inventory_and_elastic_val_type = {
    InventoryFieldValType.STR.value: SearchOperator.CONTAINS.value,
    InventoryFieldValType.DATE.value: SearchOperator.EQUALS.value,
    InventoryFieldValType.DATETIME.value: SearchOperator.EQUALS.value,
    InventoryFieldValType.FLOAT.value: SearchOperator.EQUALS.value,
    InventoryFieldValType.INT.value: SearchOperator.EQUALS.value,
    InventoryFieldValType.BOOL.value: SearchOperator.EQUALS.value,
    InventoryFieldValType.MO_LINK.value: SearchOperator.CONTAINS.value,
    InventoryFieldValType.TWO_WAY_MO_LINK.value: SearchOperator.CONTAINS.value,
    InventoryFieldValType.ENUM.value: SearchOperator.CONTAINS.value,
    InventoryFieldValType.PRM_LINK.value: SearchOperator.EQUALS.value,
    InventoryFieldValType.USER_LINK.value: SearchOperator.CONTAINS.value,
    InventoryFieldValType.FORMULA.value: SearchOperator.CONTAINS.value,
    ElasticFieldValType.LONG.value: SearchOperator.EQUALS.value,
    ElasticFieldValType.INTEGER.value: SearchOperator.EQUALS.value,
    ElasticFieldValType.BOOLEAN.value: SearchOperator.EQUALS.value,
    ElasticFieldValType.FLOAT.value: SearchOperator.EQUALS.value,
    ElasticFieldValType.DOUBLE.value: SearchOperator.EQUALS.value,
    ElasticFieldValType.DATE.value: SearchOperator.EQUALS.value,
    ElasticFieldValType.KEYWORD.value: SearchOperator.CONTAINS.value,
}


def get_operator_for_global_search_by_inventory_val_type(val_type: str):
    """Returns operator for global search for special val_type"""
    res = operator_for_global_search_by_inventory_and_elastic_val_type.get(
        val_type
    )
    if res is None:
        raise NotImplementedError(
            f"Global search operator is not implemented for val_type '{val_type}' "
        )
    return res
