from elastic.enum_models import SearchOperator


def first_depth_field_date_equals_where_condition(column_name: str, value):
    """Returns 'where' condition where values of 'db_column' equal 'value'."""
    condition = [
        {"exists": {"field": column_name}},
        {"match": {column_name: value}},
    ]

    return condition


def first_depth_field_date_not_equals_where_condition(column_name, value):
    """Returns 'where' condition where values of 'db_column' not equal 'value'."""
    condition = [{"match": {column_name: value}}]

    return condition


def first_depth_field_date_is_empty_where_condition(column_name, value):
    """Returns 'where' condition where values of 'db_column' are empty."""

    condition = [{"exists": {"field": column_name}}]

    return condition


def first_depth_field_date_is_not_empty_where_condition(column_name, value):
    """Returns 'where' condition where values of 'db_column' are not empty."""

    condition = [{"exists": {"field": column_name}}]

    return condition


def first_depth_field_date_more_where_condition(column_name, value):
    condition = [
        {"exists": {"field": column_name}},
        {"range": {column_name: {"gt": value}}},
    ]

    return condition


def first_depth_field_date_more_or_eq_where_condition(column_name, value):
    condition = [
        {"exists": {"field": column_name}},
        {"range": {column_name: {"gte": value}}},
    ]

    return condition


def first_depth_field_date_less_where_condition(column_name, value):
    condition = [
        {"exists": {"field": column_name}},
        {"range": {column_name: {"lt": value}}},
    ]

    return condition


def first_depth_field_date_less_or_eq_where_condition(column_name, value):
    condition = [
        {"exists": {"field": column_name}},
        {"range": {column_name: {"lte": value}}},
    ]

    return condition


def first_depth_field_date_in_period_where_condition(column_name, value):
    condition = [
        {"exists": {"field": column_name}},
        {"range": {column_name: {"gte": value}}},
    ]

    return condition


first_depth_field_date_operators = {
    SearchOperator.EQUALS.value: first_depth_field_date_equals_where_condition,
    SearchOperator.NOT_EQUALS.value: first_depth_field_date_not_equals_where_condition,
    SearchOperator.IS_EMPTY.value: first_depth_field_date_is_empty_where_condition,
    SearchOperator.IS_NOT_EMPTY.value: first_depth_field_date_is_not_empty_where_condition,
    SearchOperator.MORE.value: first_depth_field_date_more_where_condition,
    SearchOperator.MORE_OR_EQ.value: first_depth_field_date_more_or_eq_where_condition,
    SearchOperator.LESS.value: first_depth_field_date_less_where_condition,
    SearchOperator.LESS_OR_EQ.value: first_depth_field_date_less_or_eq_where_condition,
    SearchOperator.IN_PERIOD.value: first_depth_field_date_in_period_where_condition,
}
