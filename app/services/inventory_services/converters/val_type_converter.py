import pickle
from typing import Any, Callable, Iterable
from dateutil.parser import parse

from elastic.enum_models import InventoryFieldValType, ElasticFieldValType


def to_str(value: Any):
    return str(value)


def to_int(value: Any):
    return int(value)


def to_float(value: Any):
    return float(value)


def to_bool(value: Any):
    value = str(value).lower()
    if value in {"1", "true", "yes"}:
        return True
    return False


def to_date(value):
    return str(parse(str(value)).date().__format__("%Y-%m-%d"))


def to_datetime(value):
    return parse(str(value)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


convert_functions_by_inventory_val_type = {
    InventoryFieldValType.STR.value: to_str,
    InventoryFieldValType.INT.value: to_int,
    InventoryFieldValType.FLOAT.value: to_float,
    InventoryFieldValType.BOOL.value: to_bool,
    InventoryFieldValType.FORMULA.value: to_str,
    InventoryFieldValType.USER_LINK.value: to_str,
    InventoryFieldValType.PRM_LINK.value: to_str,
    InventoryFieldValType.MO_LINK.value: to_str,
    InventoryFieldValType.DATE.value: to_date,
    InventoryFieldValType.DATETIME.value: to_datetime,
    InventoryFieldValType.SEQUENCE.value: to_int,
    InventoryFieldValType.TWO_WAY_MO_LINK.value: to_str,
    InventoryFieldValType.ENUM.value: to_str,
}

convert_functions_by_elastic_val_type = {
    ElasticFieldValType.LONG.value: to_int,
    ElasticFieldValType.KEYWORD.value: to_str,
    ElasticFieldValType.INTEGER.value: to_int,
    ElasticFieldValType.BOOLEAN.value: to_bool,
    ElasticFieldValType.FLOAT.value: to_float,
    ElasticFieldValType.DOUBLE.value: to_float,
}

convert_functions_by_val_type = dict()
convert_functions_by_val_type.update(convert_functions_by_inventory_val_type)
convert_functions_by_val_type.update(convert_functions_by_elastic_val_type)


def get_convert_function_by_val_type(val_type: str) -> Callable:
    converter = convert_functions_by_val_type.get(val_type, None)
    if converter is not None:
        return converter
    raise NotImplementedError(
        f"Conversion function not implemented for val_type - {val_type}"
    )


def get_converted_value_by_val_type(value: Any, val_type: str):
    """Tries to convert a value. Doesn't check the value before converting it"""
    converter = get_convert_function_by_val_type(val_type)
    return converter(value)


def convert_array_of_values_by_val_type(
    array_of_values: Iterable, val_type: str
):
    """Tries to convert all items in array_of_values. Doesn't check the value before converting it"""
    converter = get_convert_function_by_val_type(val_type)
    return [converter(item) for item in array_of_values]


def get_convert_function_by_val_type_for_multiple_values(
    val_type: str,
) -> Callable:
    converter = convert_functions_by_val_type.get(val_type, None)
    if converter is None:
        raise NotImplementedError(
            f"Conversion function not implemented for val_type - {val_type}"
        )

    def convert_multiply_values(array_of_values: Iterable):
        return [converter(item) for item in array_of_values]

    return convert_multiply_values


def get_convert_function_by_val_type_for_multiple_pickled_values(
    val_type: str,
) -> Callable:
    """Returns convert functions for multiple values. Value must be hex formatted string"""
    converter = convert_functions_by_val_type.get(val_type, None)
    if converter is None:
        raise NotImplementedError(
            f"Conversion function not implemented for val_type - {val_type}"
        )

    def convert_multiply_values(hex_value_as_str: str):
        values = pickle.loads(bytes.fromhex(hex_value_as_str))
        return [converter(item) for item in values]

    return convert_multiply_values


def get_converted_func_by_val_type_and_multiple_from_prm_index(
    value: str, val_type: str, multiple: bool
):
    """Returns converted value for parameter saved in INVENTORY_PRM_INDEX"""
    conv_func = None
    if multiple:
        conv_func = (
            get_convert_function_by_val_type_for_multiple_pickled_values(
                val_type
            )
        )
    else:
        conv_func = get_convert_function_by_val_type(val_type)

    return conv_func(value)


corresponding_python_val_type_for_elastic_val_type = {
    ElasticFieldValType.LONG.value: InventoryFieldValType.INT.value,
    ElasticFieldValType.TEXT.value: InventoryFieldValType.STR.value,
    ElasticFieldValType.INTEGER.value: InventoryFieldValType.INT.value,
    ElasticFieldValType.BOOLEAN.value: InventoryFieldValType.BOOL.value,
    ElasticFieldValType.FLOAT.value: InventoryFieldValType.FLOAT.value,
    ElasticFieldValType.KEYWORD.value: InventoryFieldValType.STR.value,
    ElasticFieldValType.DATE.value: InventoryFieldValType.DATETIME.value,
    ElasticFieldValType.DOUBLE.value: InventoryFieldValType.FLOAT.value,
}


def get_corresponding_python_val_type_for_elastic_val_type(
    elastic_val_type: str,
):
    """Returns corresponding python val type name as str for special elastic_val_type"""
    corresp_val_type = corresponding_python_val_type_for_elastic_val_type.get(
        elastic_val_type
    )
    if not corresp_val_type:
        raise NotImplementedError(
            f"Conversion function not implemented for Elasticsearch val_type "
            f"- {elastic_val_type}"
        )
    return corresp_val_type
