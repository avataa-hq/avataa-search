from elastic.enum_models import InventoryFieldValType
from elastic.query_builder_service.sort_queries.flattened_fields.utils import (
    sort_flattened_field_float_type,
    sort_flattened_field_str_type,
    sort_flattened_field_int_type,
    sort_functions_for_flattened_field_by_val_type,
)

sort_functions_for_parameters_field = dict()
sort_functions_for_parameters_field.update(
    sort_functions_for_flattened_field_by_val_type
)

sort_functions_for_parameters_field_by_inventory_val_type = {
    InventoryFieldValType.STR.value: sort_flattened_field_str_type,
    InventoryFieldValType.FLOAT.value: sort_flattened_field_float_type,
    InventoryFieldValType.INT.value: sort_flattened_field_int_type,
    InventoryFieldValType.BOOL.value: sort_flattened_field_str_type,
    InventoryFieldValType.ENUM.value: sort_flattened_field_str_type,
    InventoryFieldValType.MO_LINK.value: sort_flattened_field_str_type,
    InventoryFieldValType.USER_LINK.value: sort_flattened_field_str_type,
    InventoryFieldValType.FORMULA.value: sort_flattened_field_float_type,
    InventoryFieldValType.SEQUENCE.value: sort_flattened_field_int_type,
}

sort_functions_for_parameters_field.update(
    sort_functions_for_parameters_field_by_inventory_val_type
)


def get_sort_query_function_for_parameter_fields_by_inventory_val_type(
    val_type: str,
):
    """Returns conversion function for particular val_type"""

    sourt_query_builder_function = sort_functions_for_parameters_field.get(
        val_type, None
    )
    if sourt_query_builder_function is None:
        raise NotImplementedError(
            f"Sort function for MO parameter field does not implemented for val_type - "
            f"{val_type}"
        )

    return sourt_query_builder_function
