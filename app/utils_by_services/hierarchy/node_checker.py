import copy
from typing import List

from elasticsearch import AsyncElasticsearch
from pydantic import BaseModel

from elastic.enum_models import LogicalOperator, SearchOperator
from elastic.pydantic_models import HierarchyFilter, FilterColumn, FilterItem
from elastic.query_builder_service.inventory_index.mo_object.utils import (
    get_search_query_for_inventory_obj_index,
    get_dict_of_inventory_attr_and_params_types,
)
from elastic.query_builder_service.inventory_index.utils.index_utils import (
    get_index_name_by_tmo,
)
from indexes_mapping.inventory.mapping import INVENTORY_PERMISSIONS_FIELD_NAME
from security.security_data_models import UserPermission

from services.hierarchy_services.features.dependet_level_factory import (
    DependentLevelsChain,
)
from services.hierarchy_services.features.node_features import (
    get_chain_of_parent_nodes,
    get_all_mo_ids_of_special_node_id,
)
from services.hierarchy_services.models.dto import (
    HierarchyDTO,
    NodeDTO,
    LevelDTO,
)
from services.inventory_services.models import InventoryMODefaultFields


class LevelFilterStepRes(BaseModel):
    full_match_filter_condition: bool
    mo_ids_matched_conditions: List[int | str] | None = None


class ItemOfFilterQueue(BaseModel):
    node: NodeDTO
    filter: HierarchyFilter | None = None
    level: LevelDTO
    parent_level: LevelDTO | None = None


class NodeCheckerWithFilters:
    def __init__(
        self,
        node: NodeDTO,
        filters: List[HierarchyFilter],
        hierarchy: HierarchyDTO,
        elastic_client: AsyncElasticsearch,
        dependent_levels_chain: DependentLevelsChain,
        user_permissions: UserPermission = None,
    ):
        self.node = node
        self.filters = filters
        self.hierarchy = hierarchy
        self.elastic_client = elastic_client
        self.dependent_levels_chain = dependent_levels_chain
        self.user_permissions = user_permissions

        self.__node_parents = None

    async def __get_node_parents(self):
        if self.node.path:
            self.__node_parents = await get_chain_of_parent_nodes(
                self.elastic_client, self.node.path
            )

    def __create_filter_queue(self):
        """Groups corresponding filters, levels and nodes into queue of ItemOfFilterQueue"""
        order_of_filter_statements = []

        # get list of all levels including level of current node
        chain_with_current_level = list()

        if self.dependent_levels_chain.current_level:
            if self.dependent_levels_chain.upper_levels_ordered_by_depth_asc:
                chain_with_current_level.extend(
                    self.dependent_levels_chain.upper_levels_ordered_by_depth_asc
                )

            chain_with_current_level.append(
                self.dependent_levels_chain.current_level
            )

        if not chain_with_current_level:
            return order_of_filter_statements

        filters_by_tmo_id = dict()
        if self.filters:
            filters_by_tmo_id = {
                f_item.tmo_id: f_item for f_item in self.filters
            }
        #
        # level_tmo_ids = {level.object_type_id for level in chain_with_current_level}
        #
        # if not level_tmo_ids.intersection(filters_by_tmo_id):
        #     return order_of_filter_statements

        parent_nodes_by_level_id = dict()
        if self.__node_parents:
            parent_nodes_by_level_id = {
                p_node.level_id: p_node for p_node in self.__node_parents
            }

        parent_nodes_by_level_id[self.node.level_id] = self.node

        parent_level: LevelDTO | None = None
        for level in chain_with_current_level:
            corresp_node = parent_nodes_by_level_id.get(level.id)
            corresp_filter = filters_by_tmo_id.get(level.object_type_id, None)

            # if parent_level and parent_level.object_type_id == level.object_type_id:
            #     parent_filter_item = order_of_filter_statements[-1]
            #     parent_filter_item.filter = None

            filter_queue_item = ItemOfFilterQueue(
                node=corresp_node,
                filter=corresp_filter,
                level=level,
                parent_level=parent_level,
            )
            order_of_filter_statements.append(filter_queue_item)
            parent_level = level

        return order_of_filter_statements

    async def __process_one_filter_queue_item(
        self,
        filter_queue_item: ItemOfFilterQueue,
        parent_node_mo_ids: List[str] | None = None,
    ) -> LevelFilterStepRes:
        """If all mods of which the node consists satisfy the filtering - returns None.
        Otherwise, it returns the list of mo id of only those objects that satisfy the filtering condition"""
        default_resp = LevelFilterStepRes(full_match_filter_condition=True)

        hierarchy_filter = filter_queue_item.filter
        if not parent_node_mo_ids and not hierarchy_filter:
            return default_resp

        level = filter_queue_item.level
        node = filter_queue_item.node
        parent_level = filter_queue_item.parent_level

        # add hierarchy_condition
        if not self.hierarchy.create_empty_nodes:
            if node.key_is_empty:
                return LevelFilterStepRes(
                    full_match_filter_condition=False,
                    mo_ids_matched_conditions=list(),
                )

        if level.is_virtual:
            # get all mo_ids of virtual node
            node_mo_ids = await get_all_mo_ids_of_special_node_id(
                elastic_client=self.elastic_client, node_id=node.id
            )
        else:
            # if level is real
            node_mo_ids = [node.object_id]

        # get filter query
        list_of_filter_columns = list()
        if hierarchy_filter and hierarchy_filter.filter_columns:
            list_of_filter_columns = copy.deepcopy(
                hierarchy_filter.filter_columns
            )

        # add user permissions
        if self.user_permissions and self.user_permissions.is_admin is False:
            filter_item = FilterItem(
                operator=SearchOperator.IS_ANY_OF,
                value=self.user_permissions.user_permissions,
            )
            filter_column = FilterColumn(
                columnName=INVENTORY_PERMISSIONS_FIELD_NAME,
                rule=LogicalOperator.AND.value,
                filters=[filter_item],
            )
            list_of_filter_columns.append(filter_column)

        # add parent node mo_ids to filter list

        # add current node mo_ids to filter list
        filter_item = FilterItem(
            operator=SearchOperator.IS_ANY_OF, value=node_mo_ids
        )
        filter_column = FilterColumn(
            columnName="id",
            rule=LogicalOperator.AND.value,
            filters=[filter_item],
        )
        list_of_filter_columns.append(filter_column)

        # add active = True
        filter_item = FilterItem(operator=SearchOperator.EQUALS, value=True)
        filter_column = FilterColumn(
            columnName=InventoryMODefaultFields.ACTIVE.value,
            rule=LogicalOperator.AND.value,
            filters=[filter_item],
        )
        list_of_filter_columns.append(filter_column)

        if parent_node_mo_ids:
            filter_item = FilterItem(
                operator=SearchOperator.IS_ANY_OF, value=parent_node_mo_ids
            )
            # if parent level was with same object_type id  - behavior by mo_id, else by p_id
            if parent_level.object_type_id != level.object_type_id:
                filter_column = FilterColumn(
                    columnName="p_id",
                    rule=LogicalOperator.AND.value,
                    filters=[filter_item],
                )
            else:
                filter_column = FilterColumn(
                    columnName="id",
                    rule=LogicalOperator.AND.value,
                    filters=[filter_item],
                )

            list_of_filter_columns.append(filter_column)

        if not list_of_filter_columns:
            return default_resp

        column_names = {
            filter_column.column_name
            for filter_column in list_of_filter_columns
        }
        dict_of_types = await get_dict_of_inventory_attr_and_params_types(
            array_of_attr_and_params_names=column_names,
            elastic_client=self.elastic_client,
        )

        filter_search_query = get_search_query_for_inventory_obj_index(
            filter_columns=list_of_filter_columns, dict_of_types=dict_of_types
        )

        size_per_step = 100_000
        search_body = {
            "query": filter_search_query,
            "track_total_hits": True,
            "size": size_per_step,
            "sort": {"_doc": {"order": "asc"}},
            "_source": {"includes": ["id"]},
        }

        inventory_index_name = get_index_name_by_tmo(level.object_type_id)

        len_of_mo_ids = len(node_mo_ids)
        all_in = False
        mo_ids_match_filter_cond = list()
        while True:
            search_res = await self.elastic_client.search(
                index=inventory_index_name,
                body=search_body,
                ignore_unavailable=True,
            )

            total_hits = search_res["hits"]["total"]["value"]
            if total_hits == len_of_mo_ids:
                all_in = True
                break

            search_res = search_res["hits"]["hits"]

            if not mo_ids_match_filter_cond:
                mo_ids_match_filter_cond = [
                    item["_source"]["id"] for item in search_res
                ]
            else:
                mo_ids_match_filter_cond.extend(
                    [item["_source"]["id"] for item in search_res]
                )

            if total_hits < size_per_step:
                break

            if len(search_res) == size_per_step:
                search_after = search_res[-1]["sort"]
                search_body["search_after"] = search_after
            else:
                break
        if all_in:
            default_resp.full_match_filter_condition = True
        else:
            default_resp.full_match_filter_condition = False

        default_resp.mo_ids_matched_conditions = mo_ids_match_filter_cond
        return default_resp

    async def check(self) -> LevelFilterStepRes:
        """If the node fully satisfies the filtering - returns instance of LevelFilterStepRes with:
        full_match_filter_condition: True
        mo_ids_matched_conditions: None,
        otherwise:
        full_match_filter_condition: False
        mo_ids_matched_conditions: list of mo identifiers matching the filter conditions"""
        await self.__get_node_parents()

        filter_queue = self.__create_filter_queue()

        prev_res = LevelFilterStepRes(full_match_filter_condition=True)
        for filter_queue_item in filter_queue:
            if prev_res.full_match_filter_condition:
                prev_res = await self.__process_one_filter_queue_item(
                    filter_queue_item
                )
            else:
                if prev_res.mo_ids_matched_conditions:
                    prev_res = await self.__process_one_filter_queue_item(
                        filter_queue_item, prev_res.mo_ids_matched_conditions
                    )
                else:
                    prev_res = LevelFilterStepRes(
                        full_match_filter_condition=False,
                        mo_ids_matched_conditions=list(),
                    )
        return prev_res
