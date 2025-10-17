from typing import Union, List

from elastic.enum_models import SearchOperator


def flattened_field_float_equals_where_condition(
    column_name: str, value: float
) -> List[dict]:
    """Returns 'where' condition where values of 'column_name' equal 'value'."""
    condition = [
        {"exists": {"field": column_name}},
        {"match": {column_name: value}},
    ]

    return condition


def flattened_field_float_not_equals_where_condition(
    column_name: str, value: float
) -> List[dict]:
    """Returns 'where' condition where values of 'column_name' not equal 'value'."""
    condition = [{"match": {column_name: value}}]

    return condition


def flattened_field_float_is_empty_where_condition(
    column_name: str, value: float
) -> List[dict]:
    """Returns 'where' condition where values of 'column_name' are empty."""

    condition = [{"exists": {"field": column_name}}]

    return condition


def flattened_field_float_is_not_empty_where_condition(
    column_name: str, value: float
) -> List[dict]:
    """Returns 'where' condition where values of 'column_name' are not empty."""

    condition = [{"exists": {"field": column_name}}]

    return condition


def flattened_field_float_is_any_of_where_condition(
    column_name: str, value: Union[float, list]
) -> List[dict]:
    """Returns 'where' condition where values of 'column_name' equals to any of values in 'value'."""
    if not isinstance(value, list):
        value = [value]

    condition = [
        {"exists": {"field": column_name}},
        {"terms": {column_name: value}},
    ]

    return condition


def flattened_field_float_is_not_any_of_where_condition(
    column_name: str, value: Union[float, list]
) -> List[dict]:
    """Returns 'where' condition where values of 'column_name' not equals to any of values in 'value'."""
    if not isinstance(value, list):
        value = [value]
    condition = [
        {"exists": {"field": column_name}},
        {"terms": {column_name: value}},
    ]

    return condition


def flattened_field_float_more_where_condition(column_name: str, value: float):
    """Returns 'where' condition where values of 'column_name' more than 'value'."""
    condition = [
        {"exists": {"field": column_name}},
        {
            "script": {
                "script": """
                try{float v = Float.parseFloat(doc["%(column_name)s"].value);
                if (v > %(field_value)s)
                {return true;}
                }
                catch (Exception ignore){}
                """
                % {"column_name": column_name, "field_value": value}
            }
        },
    ]

    return condition


def flattened_field_float_more_or_eq_where_condition(
    column_name: str, value: float
):
    """Returns 'where' condition where values of 'column_name' more than or equal 'value'."""
    condition = [
        {"exists": {"field": column_name}},
        {
            "script": {
                "script": """
                    try{float v = Float.parseFloat(doc["%(column_name)s"].value);
                    if (v >= %(field_value)s)
                    {return true;}
                    }
                    catch (Exception ignore){}
                    """
                % {"column_name": column_name, "field_value": value}
            }
        },
    ]

    return condition


def flattened_field_float_less_where_condition(column_name: str, value: float):
    """Returns 'where' condition where values of 'column_name' less than 'value'."""
    condition = [
        {"exists": {"field": column_name}},
        {
            "script": {
                "script": """
                    try{float v = Float.parseFloat(doc["%(column_name)s"].value);
                    if (v < %(field_value)s)
                    {return true;}
                    }
                    catch (Exception ignore){}
                    """
                % {"column_name": column_name, "field_value": value}
            }
        },
    ]

    return condition


def flattened_field_float_less_or_eq_where_condition(
    column_name: str, value: float
):
    """Returns 'where' condition where values of 'column_name' less than or equal 'value'."""
    condition = [
        {"exists": {"field": column_name}},
        {
            "script": {
                "script": """
                    try{float v = Float.parseFloat(doc["%(column_name)s"].value);
                    if (v <= %(field_value)s)
                    {return true;}
                    }
                    catch (Exception ignore){}
                    """
                % {"column_name": column_name, "field_value": value}
            }
        },
    ]

    return condition


flattened_field_float_operators = {
    SearchOperator.EQUALS.value: flattened_field_float_equals_where_condition,
    SearchOperator.NOT_EQUALS.value: flattened_field_float_not_equals_where_condition,
    SearchOperator.IS_EMPTY.value: flattened_field_float_is_empty_where_condition,
    SearchOperator.IS_NOT_EMPTY.value: flattened_field_float_is_not_empty_where_condition,
    SearchOperator.IS_ANY_OF.value: flattened_field_float_is_any_of_where_condition,
    SearchOperator.IS_NOT_ANY_OF.value: flattened_field_float_is_not_any_of_where_condition,
    SearchOperator.MORE.value: flattened_field_float_more_where_condition,
    SearchOperator.MORE_OR_EQ.value: flattened_field_float_more_or_eq_where_condition,
    SearchOperator.LESS.value: flattened_field_float_less_where_condition,
    SearchOperator.LESS_OR_EQ.value: flattened_field_float_less_or_eq_where_condition,
}
