from typing import List

from elastic.enum_models import SearchOperator


def first_depth_field_boolean_equals_where_condition(
    column_name: str, value: bool
) -> List[dict]:
    """Returns 'where' condition where values of mo attr 'column_name' equal 'value'."""
    condition = [
        {"exists": {"field": column_name}},
        {"match": {column_name: value}},
    ]

    return condition


def first_depth_field_boolean_not_equals_where_condition(
    column_name: str, value: bool
) -> List[dict]:
    """Returns 'where' condition where values of mo attr 'column_name' not equal 'value'."""
    condition = [{"match": {column_name: value}}]

    return condition


def first_depth_field_boolean_is_empty_where_condition(
    column_name, value: bool
) -> List[dict]:
    """Returns 'where' condition where values of mo attr 'column_name' are empty."""

    condition = [{"exists": {"field": column_name}}]

    return condition


def first_depth_field_boolean_is_not_empty_where_condition(
    column_name: str, value: bool
) -> List[dict]:
    """Returns 'where' condition where values of mo attr 'column_name' are not empty."""

    condition = [{"exists": {"field": column_name}}]

    return condition


first_depth_field_boolean_operators = {
    SearchOperator.EQUALS.value: first_depth_field_boolean_equals_where_condition,
    SearchOperator.NOT_EQUALS.value: first_depth_field_boolean_not_equals_where_condition,
    SearchOperator.IS_EMPTY: first_depth_field_boolean_is_empty_where_condition,
    SearchOperator.IS_NOT_EMPTY: first_depth_field_boolean_is_not_empty_where_condition,
}
