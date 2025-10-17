from typing import Dict, Callable

from elastic.enum_models import InventoryFieldValType
from elastic.query_builder_service.search_operators.first_depth_fields.boolean_val_type import (
    first_depth_field_boolean_operators,
)
from elastic.query_builder_service.search_operators.first_depth_fields.date_val_type import (
    first_depth_field_date_operators,
)
from elastic.query_builder_service.search_operators.first_depth_fields.datetime_val_type import (
    first_depth_field_datetime_operators,
)
from elastic.query_builder_service.search_operators.first_depth_fields.number_val_type import (
    first_depth_field_number_operators,
)
from elastic.query_builder_service.search_operators.first_depth_fields.str_val_type import (
    first_depth_field_str_operators,
)
from elastic.query_builder_service.search_operators.first_depth_fields.utils import (
    first_depth_field_search_operators_by_val_type,
)

parameter_field_search_operators = dict()
parameter_field_search_operators.update(
    first_depth_field_search_operators_by_val_type
)

parameter_operators_by_inventory_val_type = {
    InventoryFieldValType.STR.value: first_depth_field_str_operators,
    InventoryFieldValType.DATE.value: first_depth_field_date_operators,
    InventoryFieldValType.DATETIME.value: first_depth_field_datetime_operators,
    InventoryFieldValType.FLOAT.value: first_depth_field_number_operators,
    InventoryFieldValType.INT.value: first_depth_field_number_operators,
    InventoryFieldValType.BOOL.value: first_depth_field_boolean_operators,
    InventoryFieldValType.MO_LINK.value: first_depth_field_str_operators,
    InventoryFieldValType.USER_LINK.value: first_depth_field_str_operators,
    InventoryFieldValType.FORMULA.value: first_depth_field_str_operators,
    InventoryFieldValType.SEQUENCE.value: first_depth_field_number_operators,
    InventoryFieldValType.TWO_WAY_MO_LINK.value: first_depth_field_str_operators,
    InventoryFieldValType.ENUM.value: first_depth_field_str_operators,
}

parameter_field_search_operators.update(
    parameter_operators_by_inventory_val_type
)


def get_operators_for_parameter_field_by_inventory_val_type_or_raise_error(
    val_type: str,
) -> Dict[str, Callable]:
    """Returns dict with operators names as key and functions as value if val_type exist for first_depth_fields,
    otherwise raises not implemented error"""
    res = parameter_field_search_operators.get(val_type)
    if res is None:
        raise NotImplementedError(
            f"Value type named {val_type} is not implemented for building a query"
            f" by MO parameters"
        )
    return res


def get_where_condition_function_for_parameter_fields_by_inventory_val_type(
    operator_name: str, val_type: str
) -> Callable:
    """Returns callable function, otherwise raises not implemented error."""
    operators = (
        get_operators_for_parameter_field_by_inventory_val_type_or_raise_error(
            val_type=val_type
        )
    )

    operator_function = operators.get(operator_name)
    if operator_function is None:
        raise NotImplementedError(
            f"Operator named {operator_name} is not implemented for"
            f"building a query by MO parameters with val_type = {val_type}"
        )
    return operator_function


def get_available_operators_for_inventory_val_types():
    """Returns dict with inventory val typse as key and lists of available operators names as values"""
    return {
        v_t: list(op.keys())
        for v_t, op in parameter_operators_by_inventory_val_type.items()
    }
