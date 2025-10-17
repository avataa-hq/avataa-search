from typing import List

from elastic.enum_models import SearchOperator


def flattened_field_str_contains_where_condition(
    column_name: str, value: str
) -> List[dict]:
    """Returns 'where' condition where values of 'column_name' contains 'value'."""
    value = value.lower()
    condition = [
        {"exists": {"field": column_name}},
        {
            "script": {
                "script": """
                            try{String v = doc["%(column_name)s"].value.toLowerCase();
                            return v.contains("%(field_value)s");
                            }
                            catch (Exception ignore){}
                            """
                % {"column_name": column_name, "field_value": value}
            }
        },
    ]
    return condition


def flattened_field_str_not_contains_where_condition(
    column_name: str, value: str
) -> List[dict]:
    """Returns 'where' condition where values of 'column_name' not contains 'value'."""
    value = value.lower()
    condition = [
        {
            "script": {
                "script": """
                                try{String v = doc["%(column_name)s"].value.toLowerCase();
                                return v.contains("%(field_value)s");
                                }
                                catch (Exception ignore){}
                                """
                % {"column_name": column_name, "field_value": value}
            }
        }
    ]
    return condition


def flattened_field_str_equals_where_condition(
    column_name: str, value: str
) -> List[dict]:
    """Returns 'where' condition where values of 'db_column' equal 'value'."""
    condition = [
        {"exists": {"field": column_name}},
        {"match": {column_name: value}},
    ]

    return condition


def flattened_field_str_not_equals_where_condition(
    column_name: str, value: str
) -> List[dict]:
    """Returns 'where' condition where values of 'db_column' not equal 'value'."""
    condition = [{"match": {column_name: value}}]

    return condition


def flattened_field_str_starts_with_where_condition(
    column_name: str, value: str
) -> List[dict]:
    """Returns 'where' condition where values of 'db_column' start with 'value'."""

    condition = [
        {"exists": {"field": column_name}},
        {"prefix": {column_name: {"case_insensitive": True, "value": value}}},
    ]

    return condition


def flattened_field_str_ends_with_where_condition(
    column_name: str, value: str
) -> List[dict]:
    """Returns 'where' condition where values of 'db_column' end with 'value'."""
    value = value.lower()
    condition = [
        {"exists": {"field": column_name}},
        {
            "script": {
                "script": """
            try{String v = doc["%(column_name)s"].value.toLowerCase();
            return v.endsWith("%(field_value)s");
            }
            catch (Exception ignore){}
            """
                % {"column_name": column_name, "field_value": value}
            }
        },
    ]

    return condition


def flattened_field_str_is_empty_where_condition(
    column_name: str, value: str
) -> List[dict]:
    """Returns 'where' condition where values of 'db_column' are empty."""

    condition = [{"exists": {"field": column_name}}]

    return condition


def flattened_field_str_is_not_empty_where_condition(
    column_name: str, value: str
) -> List[dict]:
    """Returns 'where' condition where values of 'db_column' are not empty."""

    condition = [{"exists": {"field": column_name}}]

    return condition


def flattened_field_str_is_any_of_where_condition(
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


def flattened_field_str_is_not_any_of_where_condition(
    column_name, value: List[str]
):
    """Returns 'where' condition where values of 'db_column' not equals to any of values in 'value'."""
    if not isinstance(value, list):
        value = [value]
    condition = [{"terms": {column_name: value}}]

    return condition


def flattened_field_str_more_where_condition(
    column_name: str, value: str
) -> List[dict]:
    condition = [
        {"exists": {"field": column_name}},
        {
            "script": {
                "script": """
            try{String v = doc["%(column_name)s"].value;
            if (v.compareToIgnoreCase("%(field_value)s") > 0)
            {return true;}
            }
            catch (Exception ignore){}
            """
                % {"column_name": column_name, "field_value": value}
            }
        },
    ]
    return condition


def flattened_field_str_more_or_eq_where_condition(
    column_name: str, value: str
) -> List[dict]:
    condition = [
        {"exists": {"field": column_name}},
        {
            "script": {
                "script": """
                try{String v = doc["%(column_name)s"].value;
                if (v.compareToIgnoreCase("%(field_value)s") >= 0)
                {return true;}
                }
                catch (Exception ignore){}
                """
                % {"column_name": column_name, "field_value": value}
            }
        },
    ]

    return condition


def flattened_field_str_less_where_condition(
    column_name: str, value: str
) -> List[dict]:
    condition = [
        {"exists": {"field": column_name}},
        {
            "script": {
                "script": """
                try{String v = doc["%(column_name)s"].value;
                if (v.compareToIgnoreCase("%(field_value)s") < 0)
                {return true;}
                }
                catch (Exception ignore){}
                """
                % {"column_name": column_name, "field_value": value}
            }
        },
    ]

    return condition


def flattened_field_str_less_or_eq_where_condition(
    column_name: str, value: str
) -> List[dict]:
    condition = [
        {"exists": {"field": column_name}},
        {
            "script": {
                "script": """
            try{String v = doc["%(column_name)s"].value;
            if (v.compareToIgnoreCase("%(field_value)s") <= 0)
            {return true;}
            }
            catch (Exception ignore){}
            """
                % {"column_name": column_name, "field_value": value}
            }
        },
    ]

    return condition


flattened_field_str_operators = {
    SearchOperator.CONTAINS.value: flattened_field_str_contains_where_condition,
    SearchOperator.NOT_CONTAINS.value: flattened_field_str_not_contains_where_condition,
    SearchOperator.EQUALS.value: flattened_field_str_equals_where_condition,
    SearchOperator.NOT_EQUALS.value: flattened_field_str_not_equals_where_condition,
    SearchOperator.STARTS_WITH.value: flattened_field_str_starts_with_where_condition,
    SearchOperator.ENDS_WITH.value: flattened_field_str_ends_with_where_condition,
    SearchOperator.IS_EMPTY.value: flattened_field_str_is_empty_where_condition,
    SearchOperator.IS_NOT_EMPTY.value: flattened_field_str_is_not_empty_where_condition,
    SearchOperator.IS_ANY_OF.value: flattened_field_str_is_any_of_where_condition,
    SearchOperator.IS_NOT_ANY_OF.value: flattened_field_str_is_not_any_of_where_condition,
    SearchOperator.MORE.value: flattened_field_str_more_where_condition,
    SearchOperator.MORE_OR_EQ.value: flattened_field_str_more_or_eq_where_condition,
    SearchOperator.LESS.value: flattened_field_str_less_where_condition,
    SearchOperator.LESS_OR_EQ.value: flattened_field_str_less_or_eq_where_condition,
}
