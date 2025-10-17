from elastic.enum_models import ElasticFieldValType
from indexes_mapping.inventory.mapping import (
    INVENTORY_OBJ_INDEX_MAPPING,
    INVENTORY_PERMISSIONS_FIELD_NAME,
)
from v2.routers.inventory.utils.helpers import (
    find_out_inventory_type_of_inputted_data,
)

date_datetime_dict = dict()
mo_attrs_val_types_check_on_input_data_type = {
    "bool": dict(),
    "int": dict(),
    "float": dict(),
    "date": date_datetime_dict,
    "datetime": date_datetime_dict,
    "str": dict(),
}


corresponding_elastic_types_for_mo_attr_search = {
    ElasticFieldValType.LONG.value: mo_attrs_val_types_check_on_input_data_type[
        "int"
    ],
    ElasticFieldValType.INTEGER.value: mo_attrs_val_types_check_on_input_data_type[
        "int"
    ],
    ElasticFieldValType.BOOLEAN.value: mo_attrs_val_types_check_on_input_data_type[
        "bool"
    ],
    ElasticFieldValType.FLOAT.value: mo_attrs_val_types_check_on_input_data_type[
        "float"
    ],
    ElasticFieldValType.DATE.value: mo_attrs_val_types_check_on_input_data_type[
        "date"
    ],
    ElasticFieldValType.KEYWORD.value: mo_attrs_val_types_check_on_input_data_type[
        "str"
    ],
    ElasticFieldValType.DOUBLE.value: mo_attrs_val_types_check_on_input_data_type[
        "float"
    ],
}

NOT_SEARCHABLE_MO_ATTR_ELASTIC_TYPES = {
    ElasticFieldValType.OBJECT.value,
    ElasticFieldValType.FLATTENED.value,
}

for mo_attr_name, data in INVENTORY_OBJ_INDEX_MAPPING["properties"].items():
    if mo_attr_name == INVENTORY_PERMISSIONS_FIELD_NAME:
        continue
    data_type = data["type"]
    if data_type not in NOT_SEARCHABLE_MO_ATTR_ELASTIC_TYPES:
        special_dict = corresponding_elastic_types_for_mo_attr_search.get(
            data_type
        )

        if special_dict is None:
            raise NotImplementedError(
                f"{data_type} does not implemented in MO attr search."
            )

        special_dict[mo_attr_name] = data_type


mo_attr_val_types_check_on_input_data_type = {
    "bool": [
        mo_attrs_val_types_check_on_input_data_type["str"],
        mo_attrs_val_types_check_on_input_data_type["bool"],
    ],
    "int": [
        mo_attrs_val_types_check_on_input_data_type["str"],
        mo_attrs_val_types_check_on_input_data_type["int"],
    ],
    "float": [
        mo_attrs_val_types_check_on_input_data_type["str"],
        mo_attrs_val_types_check_on_input_data_type["float"],
    ],
    "date": [
        mo_attrs_val_types_check_on_input_data_type["str"],
        mo_attrs_val_types_check_on_input_data_type["date"],
    ],
    "datetime": [
        mo_attrs_val_types_check_on_input_data_type["str"],
        mo_attrs_val_types_check_on_input_data_type["date"],
    ],
    "str": [mo_attrs_val_types_check_on_input_data_type["str"]],
}


def get_mo_type_and_attr_names_need_to_check_in_global_search_by_inputted_data(
    input_data: str,
) -> dict:
    """Returns a dict of mo attr names anr their val_types that should be searched globally."""
    val_type = find_out_inventory_type_of_inputted_data(input_data)
    list_of_dicts_of_attr_names = (
        mo_attr_val_types_check_on_input_data_type.get(val_type)
    )
    if list_of_dicts_of_attr_names is None:
        raise NotImplementedError(
            f"Global search is not implemented for MO attrs with val_type '{val_type}' "
        )
    res = dict()
    [res.update(d) for d in list_of_dicts_of_attr_names]
    return res
