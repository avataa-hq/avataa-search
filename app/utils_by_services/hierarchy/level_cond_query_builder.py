from typing import List
from elasticsearch import AsyncElasticsearch

from elastic.enum_models import SearchOperator, LogicalOperator
from elastic.pydantic_models import FilterItem, FilterColumn
from elastic.query_builder_service.inventory_index.mo_object.utils import (
    get_dict_of_inventory_attr_and_params_types,
    get_search_query_for_inventory_obj_index,
)
from indexes_mapping.inventory.mapping import INVENTORY_PERMISSIONS_FIELD_NAME
from security.security_data_models import UserPermission
from services.hierarchy_services.models.dto import NodeDTO
from services.inventory_services.models import InventoryMODefaultFields
from utils_by_services.hierarchy.level_condition_grouper import LevelConditions
from v2.routers.inventory.utils.search_by_value_utils import (
    get_query_for_search_by_value_in_tmo_scope,
)


class LevelConditionsQueryBuilder:
    def __init__(
        self,
        levels_conditions: List[LevelConditions],
        elastic_client: AsyncElasticsearch,
        parent_node_dto: NodeDTO = None,
        user_permissions: UserPermission = None,
    ):
        self.levels_conditions = levels_conditions
        self.parent_node_dto = parent_node_dto
        self.elastic_client = elastic_client
        self.user_permissions = user_permissions

        self.__parent_path_must_condition = None

    async def get_list_of_should_conditions(self) -> List[dict]:
        """Returns a list of 'should' conditions for inventory_tmo_obj index"""
        user_permissions = None
        permission_filter_column = None
        if self.user_permissions and self.user_permissions.is_admin is False:
            user_permissions = self.user_permissions.user_permissions
            filter_item = FilterItem(
                operator=SearchOperator.IS_ANY_OF,
                value=self.user_permissions.user_permissions,
            )
            filter_column = FilterColumn(
                columnName=INVENTORY_PERMISSIONS_FIELD_NAME,
                rule=LogicalOperator.AND.value,
                filters=[filter_item],
            )
            permission_filter_column = filter_column

        # create filter conditions
        level_id_filter_columns = {}
        level_id_search_by_value = {}
        all_inventory_filter_column_names = set()
        inventory_should_conditions = []

        level_ids_to_process = set()

        for level_cond in self.levels_conditions:
            # create inventory conditions
            if level_cond.filter_condition:
                hierarchy_filter = level_cond.filter_condition
                additional_filters = []
                # add tmo_id condition
                filter_item = FilterItem(
                    operator=SearchOperator.EQUALS,
                    value=hierarchy_filter.tmo_id,
                )
                filter_column = FilterColumn(
                    columnName=InventoryMODefaultFields.TMO_ID.value,
                    rule=LogicalOperator.AND.value,
                    filters=[filter_item],
                )
                additional_filters.append(filter_column)
                all_inventory_filter_column_names.add(
                    InventoryMODefaultFields.TMO_ID.value
                )

                # add active True
                filter_item = FilterItem(
                    operator=SearchOperator.EQUALS, value=True
                )
                filter_column = FilterColumn(
                    columnName=InventoryMODefaultFields.ACTIVE.value,
                    rule=LogicalOperator.AND.value,
                    filters=[filter_item],
                )
                additional_filters.append(filter_column)
                all_inventory_filter_column_names.add(
                    InventoryMODefaultFields.ACTIVE.value
                )

                if permission_filter_column:
                    additional_filters.append(permission_filter_column)
                    all_inventory_filter_column_names.add(
                        INVENTORY_PERMISSIONS_FIELD_NAME
                    )

                if hierarchy_filter.filter_columns:
                    level_id_filter_columns[level_cond.level.id] = (
                        hierarchy_filter.filter_columns + additional_filters
                    )

                    all_inventory_filter_column_names.update(
                        {c.column_name for c in hierarchy_filter.filter_columns}
                    )
                else:
                    level_id_filter_columns[level_cond.level.id] = (
                        additional_filters
                    )

                level_ids_to_process.add(level_cond.level.id)

                if hierarchy_filter.search_by_value:
                    search_by_value_query = (
                        await get_query_for_search_by_value_in_tmo_scope(
                            elastic_client=self.elastic_client,
                            tmo_ids=[hierarchy_filter.tmo_id],
                            search_value=hierarchy_filter.search_by_value,
                            client_permissions=user_permissions,
                        )
                    )

                    if search_by_value_query:
                        level_id_search_by_value[level_cond.level.id] = (
                            search_by_value_query
                        )

        if level_ids_to_process:
            dict_of_types = await get_dict_of_inventory_attr_and_params_types(
                array_of_attr_and_params_names=list(
                    all_inventory_filter_column_names
                ),
                elastic_client=self.elastic_client,
            )

            for level_id in level_ids_to_process:
                filter_columns = level_id_filter_columns.get(level_id)
                filter_search_query = {}
                if filter_columns:
                    filter_search_query = (
                        get_search_query_for_inventory_obj_index(
                            filter_columns=filter_columns,
                            dict_of_types=dict_of_types,
                        )
                    )

                search_by_value_query = level_id_search_by_value.get(level_id)

                if search_by_value_query:
                    if not filter_search_query:
                        inventory_should_conditions.append(
                            [search_by_value_query]
                        )
                    else:
                        filter_search_query_must = filter_search_query[
                            "bool"
                        ].get("must")

                        if filter_search_query_must:
                            filter_search_query_must.append(
                                search_by_value_query
                            )
                        else:
                            filter_search_query["bool"]["must"] = (
                                search_by_value_query
                            )

                if filter_search_query:
                    inventory_should_conditions.append(filter_search_query)
        else:
            inventory_should_conditions.append({"match_all": {}})

        return inventory_should_conditions
