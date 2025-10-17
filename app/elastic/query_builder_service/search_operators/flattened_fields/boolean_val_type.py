from typing import List

from elastic.enum_models import SearchOperator


def flattened_field_boolean_equals_where_condition(
    column_name: str, value: bool
) -> List[dict]:
    """Returns 'where' condition where values of 'column_name' equal 'value'."""

    condition = [
        {"exists": {"field": column_name}},
        {"match": {column_name: value}},
    ]

    return condition


def flattened_field_boolean_not_equals_where_condition(
    column_name: int, value: bool
) -> List[dict]:
    """Returns 'where' condition where values of 'column_name' not equal 'value'."""

    condition = [{"match": {column_name: value}}]

    return condition


def flattened_field_boolean_is_empty_where_condition(
    column_name: int, value: bool
) -> List[dict]:
    """Returns 'where' condition where values of 'column_name' are empty."""

    condition = [{"exists": {"field": column_name}}]

    return condition


def flattened_field_boolean_is_not_empty_where_condition(
    column_name: int, value: bool
) -> List[dict]:
    """Returns 'where' condition where values of 'column_name' are not empty."""

    condition = [{"exists": {"field": column_name}}]

    return condition


flattened_field_boolean_operators = {
    SearchOperator.EQUALS.value: flattened_field_boolean_equals_where_condition,
    SearchOperator.NOT_EQUALS.value: flattened_field_boolean_not_equals_where_condition,
    SearchOperator.IS_EMPTY.value: flattened_field_boolean_is_empty_where_condition,
    SearchOperator.IS_NOT_EMPTY.value: flattened_field_boolean_is_not_empty_where_condition,
}
