from typing import List

from elastic.enum_models import SearchOperator


def first_depth_field_str_contains_where_condition(column_name, value):
    """Returns 'where' condition where values of 'column_name' contains 'value'."""
    condition = [
        {"exists": {"field": column_name}},
        {
            "wildcard": {
                column_name: {"value": f"*{value}*", "case_insensitive": True}
            }
        },
    ]
    return condition


def first_depth_field_str_not_contains_where_condition(column_name, value):
    """Returns 'where' condition where values of 'column_name' not contains 'value'."""
    condition = [
        {"exists": {"field": column_name}},
        {
            "wildcard": {
                column_name: {"value": f"*{value}*", "case_insensitive": True}
            }
        },
    ]
    return condition


def first_depth_field_str_equals_where_condition(column_name, value):
    """Returns 'where' condition where values of 'db_column' equal 'value'."""
    condition = [
        {"exists": {"field": column_name}},
        {"match": {column_name: value}},
    ]

    return condition


def first_depth_field_str_not_equals_where_condition(column_name, value):
    """Returns 'where' condition where values of 'db_column' not equal 'value'."""
    condition = [{"match": {column_name: value}}]

    return condition


def first_depth_field_str_starts_with_where_condition(column_name, value):
    """Returns 'where' condition where values of 'db_column' start with 'value'."""

    condition = [
        {"exists": {"field": column_name}},
        {
            "wildcard": {
                column_name: {"value": f"{value}*", "case_insensitive": True}
            }
        },
    ]

    return condition


def first_depth_field_str_ends_with_where_condition(column_name, value):
    """Returns 'where' condition where values of 'db_column' end with 'value'."""

    condition = [
        {"exists": {"field": column_name}},
        {
            "wildcard": {
                column_name: {"value": f"*{value}", "case_insensitive": True}
            }
        },
    ]

    return condition


def first_depth_field_str_is_empty_where_condition(column_name, value):
    """Returns 'where' condition where values of 'db_column' are empty."""

    condition = [{"exists": {"field": column_name}}]

    return condition


def first_depth_field_str_is_not_empty_where_condition(column_name, value):
    """Returns 'where' condition where values of 'db_column' are not empty."""

    condition = [{"exists": {"field": column_name}}]

    return condition


def first_depth_field_str_is_any_of_where_condition(
    column_name, value: List[str]
):
    """Returns 'where' condition where values of 'db_column' equals to any of values in 'value'."""
    if not isinstance(value, list):
        value = [value]
    condition = [
        {"exists": {"field": column_name}},
        {"terms": {column_name: value}},
    ]

    return condition


def first_depth_field_str_is_not_any_of_where_condition(
    column_name, value: List[str]
):
    """Returns 'where' condition where values of 'db_column' not equals to any of values in 'value'."""
    if not isinstance(value, list):
        value = [value]
    condition = [
        {"exists": {"field": column_name}},
        {"terms": {column_name: value}},
    ]

    return condition


def first_depth_field_str_more_where_condition(column_name, value):
    condition = [
        {"exists": {"field": column_name}},
        {"range": {column_name: {"gt": value}}},
    ]
    return condition


def first_depth_field_str_more_or_eq_where_condition(column_name, value):
    condition = [
        {"exists": {"field": column_name}},
        {"range": {column_name: {"gte": value}}},
    ]

    return condition


def first_depth_field_str_less_where_condition(column_name, value):
    condition = [
        {"exists": {"field": column_name}},
        {"range": {column_name: {"lt": value}}},
    ]

    return condition


def first_depth_field_str_less_or_eq_where_condition(column_name, value):
    condition = [
        {"exists": {"field": column_name}},
        {"range": {column_name: {"lte": value}}},
    ]

    return condition


first_depth_field_str_operators = {
    SearchOperator.CONTAINS.value: first_depth_field_str_contains_where_condition,
    SearchOperator.NOT_CONTAINS.value: first_depth_field_str_not_contains_where_condition,
    SearchOperator.EQUALS.value: first_depth_field_str_equals_where_condition,
    SearchOperator.NOT_EQUALS.value: first_depth_field_str_not_equals_where_condition,
    SearchOperator.STARTS_WITH.value: first_depth_field_str_starts_with_where_condition,
    SearchOperator.ENDS_WITH.value: first_depth_field_str_ends_with_where_condition,
    SearchOperator.IS_EMPTY.value: first_depth_field_str_is_empty_where_condition,
    SearchOperator.IS_NOT_EMPTY.value: first_depth_field_str_is_not_empty_where_condition,
    SearchOperator.IS_ANY_OF.value: first_depth_field_str_is_any_of_where_condition,
    SearchOperator.IS_NOT_ANY_OF.value: first_depth_field_str_is_not_any_of_where_condition,
    SearchOperator.MORE.value: first_depth_field_str_more_where_condition,
    SearchOperator.MORE_OR_EQ.value: first_depth_field_str_more_or_eq_where_condition,
    SearchOperator.LESS.value: first_depth_field_str_less_where_condition,
    SearchOperator.LESS_OR_EQ.value: first_depth_field_str_less_or_eq_where_condition,
}
