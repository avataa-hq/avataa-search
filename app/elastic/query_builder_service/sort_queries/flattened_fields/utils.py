from elastic.enum_models import ElasticFieldValType


def sort_flattened_field_str_type(column_name: str, ascending: bool) -> dict:
    """Returns sorting query as dict for str val_type"""
    sort_item = {column_name: {"order": "asc" if ascending else "desc"}}
    return sort_item


def sort_flattened_field_int_type(column_name: str, ascending: bool) -> dict:
    """Returns sorting query as dict for int val_type"""
    sort_item = {
        "_script": {
            "script": """
            try{int v = Integer.parseInt(doc["%(column_name)s"].value);
            return v;}
            catch (Exception ignore){}
            """
            % {"column_name": column_name},
            "type": "number",
            "order": "asc" if ascending else "desc",
        }
    }
    return sort_item


def sort_flattened_field_float_type(column_name: str, ascending: bool) -> dict:
    """Returns sorting query as dict for float val_type"""
    sort_item = {
        "_script": {
            "script": """
            try{float v = Float.parseFloat(doc["%(column_name)s"].value);
            return v;}
            catch (Exception ignore){}
            """
            % {"column_name": column_name},
            "type": "number",
            "order": "asc" if ascending else "desc",
        }
    }
    return sort_item


sort_functions_for_flattened_field_by_python_val_type = {
    "str": sort_flattened_field_str_type,
    "float": sort_flattened_field_float_type,
    "int": sort_flattened_field_int_type,
    "bool": sort_flattened_field_str_type,
}

sort_functions_for_flattened_field_by_by_elastic_val_type = {
    ElasticFieldValType.LONG.value: sort_flattened_field_int_type,
    ElasticFieldValType.TEXT.value: sort_flattened_field_str_type,
    ElasticFieldValType.INTEGER.value: sort_flattened_field_int_type,
    ElasticFieldValType.FLOAT.value: sort_flattened_field_float_type,
}


sort_functions_for_flattened_field_by_val_type = dict()
sort_functions_for_flattened_field_by_val_type.update(
    sort_functions_for_flattened_field_by_python_val_type
)
sort_functions_for_flattened_field_by_val_type.update(
    sort_functions_for_flattened_field_by_by_elastic_val_type
)


def get_sort_query_function_for_flattened_fields_by_val_type(val_type: str):
    """Returns conversion function for particular val_type"""

    sourt_query_builder_function = (
        sort_functions_for_flattened_field_by_val_type.get(val_type, None)
    )
    if sourt_query_builder_function is None:
        raise NotImplementedError(
            f"Sort function for flattened field does not implemented for val_type - {val_type}"
        )

    return sourt_query_builder_function
