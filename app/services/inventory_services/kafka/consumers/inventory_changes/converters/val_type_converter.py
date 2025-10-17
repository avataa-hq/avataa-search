import pickle
from typing import Any, Callable
from dateutil.parser import parse

from elastic.enum_models import InventoryFieldValType


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


inventory_kafka_mag_convert_functions_by_inventory_val_type = {
    InventoryFieldValType.STR.value: to_str,
    InventoryFieldValType.INT.value: to_int,
    InventoryFieldValType.FLOAT.value: to_float,
    InventoryFieldValType.BOOL.value: to_bool,
    InventoryFieldValType.FORMULA.value: to_str,
    InventoryFieldValType.USER_LINK.value: to_str,
    InventoryFieldValType.PRM_LINK.value: to_int,
    InventoryFieldValType.MO_LINK.value: to_int,
    InventoryFieldValType.DATE.value: to_date,
    InventoryFieldValType.DATETIME.value: to_datetime,
    InventoryFieldValType.SEQUENCE.value: to_int,
    InventoryFieldValType.TWO_WAY_MO_LINK.value: to_int,
    InventoryFieldValType.ENUM.value: to_str,
}


convert_functions_for_inventory_kafka_msg_by_val_type = dict()
convert_functions_for_inventory_kafka_msg_by_val_type.update(
    inventory_kafka_mag_convert_functions_by_inventory_val_type
)


def get_convert_func_for_inventory_kafka_msg_for_not_multiple_tprm(
    val_type: str,
) -> Callable:
    converter = convert_functions_for_inventory_kafka_msg_by_val_type.get(
        val_type, None
    )
    if converter is not None:
        return converter
    raise NotImplementedError(
        f"Conversion function not implemented for inventory kafka msg with "
        f"val_type - {val_type}"
    )


def get_convert_func_for_inventory_kafka_msg_for_multiple_pickled_values(
    val_type: str,
) -> Callable:
    """Returns convert functions for multiple values. Value must be hex formatted string"""
    converter = get_convert_func_for_inventory_kafka_msg_for_not_multiple_tprm(
        val_type
    )

    def convert_multiply_values(hex_value_as_str: str):
        values = pickle.loads(bytes.fromhex(hex_value_as_str))
        return [converter(item) for item in values]

    return convert_multiply_values
