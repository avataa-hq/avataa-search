from elastic.enum_models import InventoryFieldValType, ElasticFieldValType


dict_of_matching_inventory_and_elastic_data_types = {
    InventoryFieldValType.STR.value: ElasticFieldValType.KEYWORD.value,
    InventoryFieldValType.DATE.value: ElasticFieldValType.DATE.value,
    InventoryFieldValType.DATETIME.value: ElasticFieldValType.DATE.value,
    InventoryFieldValType.FLOAT.value: ElasticFieldValType.DOUBLE.value,
    InventoryFieldValType.INT.value: ElasticFieldValType.LONG.value,
    InventoryFieldValType.BOOL.value: ElasticFieldValType.BOOLEAN.value,
    InventoryFieldValType.ENUM.value: ElasticFieldValType.KEYWORD.value,
    InventoryFieldValType.MO_LINK.value: ElasticFieldValType.KEYWORD.value,
    InventoryFieldValType.USER_LINK.value: ElasticFieldValType.KEYWORD.value,
    InventoryFieldValType.FORMULA.value: ElasticFieldValType.KEYWORD.value,
    InventoryFieldValType.SEQUENCE.value: ElasticFieldValType.LONG.value,
}


def get_corresponding_elastic_data_type(inventory_data_type: str):
    """Returns the corresponding elasticsearch data type for the entered inventory data type, otherwise raises error"""
    elastic_data_type = dict_of_matching_inventory_and_elastic_data_types.get(
        inventory_data_type
    )
    if elastic_data_type is None:
        raise NotImplementedError(
            f"Where are no implemented elasticsearch data types for inventory data "
            f"'{inventory_data_type}' type."
        )
    return elastic_data_type
