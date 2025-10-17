from typing import Dict, Callable

from elastic.enum_models import ElasticFieldValType
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

first_depth_field_search_operators_by_elastic_val_type = {
    ElasticFieldValType.LONG.value: first_depth_field_number_operators,
    ElasticFieldValType.INTEGER.value: first_depth_field_number_operators,
    ElasticFieldValType.FLOAT.value: first_depth_field_number_operators,
    ElasticFieldValType.DOUBLE.value: first_depth_field_number_operators,
    ElasticFieldValType.BOOLEAN.value: first_depth_field_boolean_operators,
    ElasticFieldValType.KEYWORD.value: first_depth_field_str_operators,
}

first_depth_field_search_operators_by_python_val_type = {
    "str": first_depth_field_str_operators,
    "date": first_depth_field_date_operators,
    "datetime": first_depth_field_datetime_operators,
    "float": first_depth_field_number_operators,
    "int": first_depth_field_number_operators,
    "bool": first_depth_field_boolean_operators,
}


first_depth_field_search_operators_by_val_type = dict()
first_depth_field_search_operators_by_val_type.update(
    first_depth_field_search_operators_by_elastic_val_type
)
first_depth_field_search_operators_by_val_type.update(
    first_depth_field_search_operators_by_python_val_type
)


def get_where_condition_functions_for_first_depth_fields_by_val_type_or_raise_error(
    val_type: str,
) -> Dict[str, Callable]:
    """Returns dict with operators names as key and functions as value if val_type exist for first_depth_fields,
    otherwise raises not implemented error"""
    res = first_depth_field_search_operators_by_val_type.get(val_type)
    if res is None:
        raise NotImplementedError(
            f"Value type named {val_type} is not implemented for building a query"
            f" by MO attributes"
        )
    return res


def get_where_condition_function_for_first_depth_fields(
    operator_name: str, val_type: str
) -> Callable:
    """Returns callable function, otherwise raises not implemented error."""
    operators = get_where_condition_functions_for_first_depth_fields_by_val_type_or_raise_error(
        val_type=val_type
    )

    operator_function = operators.get(operator_name)
    if operator_function is None:
        raise NotImplementedError(
            f"Operator named {operator_name} is not implemented for"
            f"building a query by MO attributes with val_type = {val_type}"
        )
    return operator_function
