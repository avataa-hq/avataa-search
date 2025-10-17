from typing import Union

from elastic.enum_models import SearchOperator


def first_depth_field_number_equals_where_condition(column_name, value):
    """Returns 'where' condition where values of 'db_column' equal 'value'."""
    condition = [
        {"exists": {"field": column_name}},
        {"match": {column_name: value}},
    ]

    return condition


def first_depth_field_number_not_equals_where_condition(column_name, value):
    """Returns 'where' condition where values of 'db_column' not equal 'value'."""
    condition = [{"match": {column_name: value}}]

    return condition


def first_depth_field_number_is_empty_where_condition(column_name, value):
    """Returns 'where' condition where values of 'db_column' are empty."""

    condition = [{"exists": {"field": column_name}}]

    return condition


def first_depth_field_number_is_not_empty_where_condition(column_name, value):
    """Returns 'where' condition where values of 'db_column' are not empty."""

    condition = [{"exists": {"field": column_name}}]

    return condition


def first_depth_field_number_is_any_of_where_condition(
    column_name, value: Union[str, list]
):
    """Returns 'where' condition where values of 'db_column' equals to any of values in 'value'."""
    if not isinstance(value, list):
        value = [value]
    condition = [
        {"exists": {"field": column_name}},
        {"terms": {column_name: value}},
    ]

    return condition


def first_depth_field_number_is_not_any_of_where_condition(
    column_name, value: Union[str, list]
):
    """Returns 'where' condition where values of 'db_column' not equals to any of values in 'value'."""
    if not isinstance(value, list):
        value = [value]
    condition = [
        {"exists": {"field": column_name}},
        {"terms": {column_name: value}},
    ]

    return condition


def first_depth_field_number_more_where_condition(column_name, value):
    condition = [
        {"exists": {"field": column_name}},
        {"range": {column_name: {"gt": value}}},
    ]

    return condition


def first_depth_field_number_more_or_eq_where_condition(column_name, value):
    condition = [
        {"exists": {"field": column_name}},
        {"range": {column_name: {"gte": value}}},
    ]

    return condition


def first_depth_field_number_less_where_condition(column_name, value):
    condition = [
        {"exists": {"field": column_name}},
        {"range": {column_name: {"lt": value}}},
    ]

    return condition


def first_depth_field_number_less_or_eq_where_condition(column_name, value):
    condition = [
        {"exists": {"field": column_name}},
        {"range": {column_name: {"lte": value}}},
    ]

    return condition


first_depth_field_number_operators = {
    SearchOperator.EQUALS.value: first_depth_field_number_equals_where_condition,
    SearchOperator.NOT_EQUALS.value: first_depth_field_number_not_equals_where_condition,
    SearchOperator.IS_EMPTY.value: first_depth_field_number_is_empty_where_condition,
    SearchOperator.IS_NOT_EMPTY.value: first_depth_field_number_is_not_empty_where_condition,
    SearchOperator.IS_ANY_OF.value: first_depth_field_number_is_any_of_where_condition,
    SearchOperator.IS_NOT_ANY_OF.value: first_depth_field_number_is_not_any_of_where_condition,
    SearchOperator.MORE.value: first_depth_field_number_more_where_condition,
    SearchOperator.MORE_OR_EQ.value: first_depth_field_number_more_or_eq_where_condition,
    SearchOperator.LESS.value: first_depth_field_number_less_where_condition,
    SearchOperator.LESS_OR_EQ.value: first_depth_field_number_less_or_eq_where_condition,
}
