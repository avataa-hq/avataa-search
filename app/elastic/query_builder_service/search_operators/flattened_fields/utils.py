from typing import Dict, Callable

from elastic.query_builder_service.search_operators.flattened_fields.boolean_val_type import (
    flattened_field_boolean_operators,
)
from elastic.query_builder_service.search_operators.flattened_fields.float_val_type import (
    flattened_field_float_operators,
)
from elastic.query_builder_service.search_operators.flattened_fields.int_val_type import (
    flattened_field_int_operators,
)
from elastic.query_builder_service.search_operators.flattened_fields.str_val_type import (
    flattened_field_str_operators,
)


flattened_field_search_operators_by_python_val_type = {
    "str": flattened_field_str_operators,
    "date": flattened_field_str_operators,
    "datetime": flattened_field_str_operators,
    "float": flattened_field_float_operators,
    "int": flattened_field_int_operators,
    "bool": flattened_field_boolean_operators,
}


flattened_field_search_operators_by_val_type = dict()
flattened_field_search_operators_by_val_type.update(
    flattened_field_search_operators_by_python_val_type
)


def get_where_condition_functions_for_flattened_fields_by_val_type_or_raise_error(
    val_type: str,
) -> Dict[str, Callable]:
    """Returns dict with operators names as key and functions as value if val_type exist for mo_prms,
    otherwise raises not implemented error"""
    res = flattened_field_search_operators_by_val_type.get(val_type)
    if res is None:
        raise NotImplementedError(
            f"Value type named {val_type} is not implemented for building a query"
            f" by MO TPRMs"
        )
    return res


def get_where_condition_function_for_flattened_fields(
    operator_name: str, val_type: str
) -> Callable:
    """Returns callable function, otherwise raises not implemented error."""
    operators = get_where_condition_functions_for_flattened_fields_by_val_type_or_raise_error(
        val_type=val_type
    )

    operator_function = operators.get(operator_name)
    if operator_function is None:
        raise NotImplementedError(
            f"Operator named {operator_name} is not implemented for "
            f"building a query by MO TPRMs with val_type = {val_type}"
        )
    return operator_function
