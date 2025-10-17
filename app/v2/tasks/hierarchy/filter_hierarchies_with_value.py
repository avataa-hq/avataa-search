import itertools
from asyncio import TaskGroup
from collections import deque
from typing import AsyncIterator

from elasticsearch import AsyncElasticsearch

from elastic.enum_models import SearchOperator, LogicalOperator
from elastic.pydantic_models import FilterColumn, FilterItem, SearchModel
from elastic.query_builder_service.inventory_index.mo_object.utils import (
    get_search_query_for_inventory_obj_index,
)
from elastic.query_builder_service.inventory_index.search_query_builder import (
    InventoryIndexQueryBuilder,
)
from elastic.query_builder_service.inventory_index.utils.index_utils import (
    get_index_name_by_tmo,
)
from indexes_mapping.inventory.mapping import (
    INVENTORY_OBJ_INDEX_MAPPING,
    INVENTORY_PERMISSIONS_FIELD_NAME,
)
from security.security_data_models import UserPermission
from services.hierarchy_services.elastic.configs import (
    HIERARCHY_OBJ_INDEX,
    HIERARCHY_NODE_DATA_INDEX,
)
from services.hierarchy_services.models.dto import NodeDTO
from services.inventory_services.converters.val_type_converter import (
    get_corresponding_python_val_type_for_elastic_val_type,
)
from v2.routers.inventory.utils.helpers import (
    get_operator_for_global_search_by_inventory_val_type,
    get_tprm_val_types_need_to_check_in_global_search_by_inputted_data,
)
from v2.routers.inventory.utils.mo_attr_search_helper import (
    get_mo_type_and_attr_names_need_to_check_in_global_search_by_inputted_data,
)
from v2.tasks.hierarchy.dtos.levels import LevelHierarchyWithTmoTprmsDto
from v2.tasks.utils.exceptions import ElementFound


class GetHierarchiesWithValue:
    EXCLUDED_TYPES = {"object", "flattened"}
    STEP_SIZE = 100_000

    def __init__(
        self,
        hierarchy_levels: list[LevelHierarchyWithTmoTprmsDto],
        parent_node: NodeDTO | None,
        search_by_value: str,
        elastic_client: AsyncElasticsearch,
        user_permission: UserPermission,
    ):
        self._hierarchy_levels = hierarchy_levels
        self._parent_node = parent_node
        self._search_by_value = search_by_value
        self._elastic_client = elastic_client
        self._user_permission = user_permission

        self.__level_by_level_id = ...
        self.__tprm_val_types = ...

    async def search_iterator(
        self,
        body: dict,
        index: str,
        ignore_unavailable: bool = True,
        size: int | None = None,
    ) -> AsyncIterator:
        step_size = size or self.STEP_SIZE
        last_response_size = step_size
        body["size"] = step_size
        while last_response_size >= step_size:
            search_res = await self._elastic_client.search(
                index=index, body=body, ignore_unavailable=ignore_unavailable
            )
            result = search_res["hits"]["hits"]

            for res in result:
                yield res

            last_response_size = len(result)
            if len(result):
                search_after = result[-1]["sort"]
                body["search_after"] = search_after

    async def get_child_obj_ids(
        self, parent_id: str | None, child_level_id: int
    ) -> list[str]:
        parent_id = parent_id or ""

        query = {
            "bool": {
                "must": [
                    {"term": {"parent_id": parent_id}},
                    {"term": {"level_id": child_level_id}},
                ]
            }
        }
        body = {
            "query": query,
            "sort": {"id": {"order": "asc"}},
            "track_total_hits": True,
            "_source": {"includes": ["id"]},
        }
        index = HIERARCHY_OBJ_INDEX
        cursor = self.search_iterator(
            body=body, index=index, ignore_unavailable=True
        )

        results = []
        async for row in cursor:
            results.append(row["_source"]["id"])
        return results

    async def __get_obj_id_by_mo_ids(
        self, obj_ids: list[str]
    ) -> dict[int, str]:
        results = {}  # mo_id: node_id
        if not obj_ids:
            return results

        query = {"bool": {"must": [{"terms": {"node_id": obj_ids}}]}}
        body = {
            "query": query,
            "sort": {"mo_id": {"order": "asc"}},
            "track_total_hits": True,
            "_source": {"includes": ["mo_id", "node_id"]},
        }
        index = HIERARCHY_NODE_DATA_INDEX
        cursor = self.search_iterator(
            body=body, index=index, ignore_unavailable=True
        )

        async for row in cursor:
            source = row["_source"]
            results[source["mo_id"]] = source["node_id"]
        return results

    def __create_dict_of_types(
        self, level: LevelHierarchyWithTmoTprmsDto
    ) -> dict[str, dict]:
        result = {}

        # static
        for attr_name, data in INVENTORY_OBJ_INDEX_MAPPING[
            "properties"
        ].items():
            if data["type"] not in self.EXCLUDED_TYPES:
                corresp_type = (
                    get_corresponding_python_val_type_for_elastic_val_type(
                        data["type"]
                    )
                )
                result[attr_name] = {
                    "val_type": corresp_type,
                    "multiple": False,
                }

        # by parameter id
        for tprm in level.current_level_tprms_by_tprm_id.values():
            result[str(tprm.id)] = {
                "val_type": tprm.val_type,
                "multiple": tprm.multiple,
            }
        return result

    @property
    def tprm_val_types(self) -> set[str]:
        if self.__tprm_val_types is ...:
            self.__tprm_val_types = get_tprm_val_types_need_to_check_in_global_search_by_inputted_data(
                self._search_by_value
            )
        return self.__tprm_val_types

    def _add_should_value_condition(
        self, query: dict, dict_of_types: dict[str, dict]
    ) -> dict:
        should_parts = []
        for id, tprm_data in dict_of_types.items():
            if not id.isdigit():
                continue
            column_type = tprm_data["val_type"]
            if column_type not in self.tprm_val_types:
                continue
            search_operator = (
                get_operator_for_global_search_by_inventory_val_type(
                    column_type
                )
            )
            search_model = SearchModel(
                column_name=id,
                operator=search_operator,
                column_type=column_type,
                multiple=False,
                value=self._search_by_value,
            )

            should_parts.append(search_model)

        mo_attrs = get_mo_type_and_attr_names_need_to_check_in_global_search_by_inputted_data(
            self._search_by_value
        )

        for mo_attr_name, mo_attr_val_type in mo_attrs.items():
            search_operator = (
                get_operator_for_global_search_by_inventory_val_type(
                    mo_attr_val_type
                )
            )
            search_model = SearchModel(
                column_name=mo_attr_name,
                operator=search_operator,
                column_type=mo_attr_val_type,
                multiple=False,
                value=self._search_by_value,
            )
            should_parts.append(search_model)
        should_query = InventoryIndexQueryBuilder(
            logical_operator=LogicalOperator.OR.value,
            search_list_order=should_parts,
        )
        query["bool"].update(
            should_query.create_query_as_dict()["bool"]["must"][0]["bool"]
        )
        return query

    async def __check_existing_data_by_filters(
        self,
        level_filters: list[FilterColumn],
        level: LevelHierarchyWithTmoTprmsDto,
    ) -> bool:
        dict_of_types = self.__create_dict_of_types(level=level)

        # can't be should condition in query down
        query = get_search_query_for_inventory_obj_index(
            filter_columns=level_filters, dict_of_types=dict_of_types
        )
        query = self._add_should_value_condition(
            query=query, dict_of_types=dict_of_types
        )
        body = {
            "query": query,
            "sort": {"id": {"order": "asc"}},
            "track_total_hits": True,
            "_source": {"includes": ["id"]},
        }
        index = get_index_name_by_tmo(tmo_id=level.current_level.object_type_id)
        cursor = self.search_iterator(
            body=body, index=index, ignore_unavailable=True, size=1
        )
        async for _ in cursor:
            return True
        return False

    def __filter_by_mo_ids(self, mo_ids: list[int]) -> FilterColumn | None:
        if not mo_ids:
            return None
        filter_item = FilterItem(
            operator=SearchOperator.IS_ANY_OF, value=mo_ids
        )
        filter_column = FilterColumn(
            columnName="id",
            rule=LogicalOperator.AND.value,
            filters=[filter_item],
        )
        return filter_column

    def __filter_add_permissions(
        self, filters: list[FilterColumn]
    ) -> list[FilterColumn] | None:
        filters = filters.copy()
        if self._user_permission.is_admin:
            return filters
        if not self._user_permission.user_permissions:
            return
        filter_item = FilterItem(
            operator=SearchOperator.IS_ANY_OF,
            value=self._user_permission.user_permissions,
        )
        filter_column = FilterColumn(
            columnName=INVENTORY_PERMISSIONS_FIELD_NAME,
            rule=LogicalOperator.AND.value,
            filters=[filter_item],
        )
        filters.append(filter_column)
        return filters

    async def _check_current_level(
        self, obj_ids: list[str], level: LevelHierarchyWithTmoTprmsDto
    ) -> bool:
        if not obj_ids:
            return False
        obj_id_by_mo_id = await self.__get_obj_id_by_mo_ids(obj_ids=obj_ids)
        mo_ids: list[int] = list(obj_id_by_mo_id.keys())
        filter_by_mo_ids = self.__filter_by_mo_ids(mo_ids=mo_ids)
        if filter_by_mo_ids is None:
            return False
        filters = [filter_by_mo_ids]
        filters = self.__filter_add_permissions(filters=filters)
        if filters is None:
            return False
        return await self.__check_existing_data_by_filters(
            level_filters=filters, level=level
        )

    def filter_out_top_levels(
        self, parent_node: NodeDTO | None
    ) -> list[LevelHierarchyWithTmoTprmsDto]:
        level_id = parent_node.level_id if parent_node else None
        if level_id is None:
            return self._hierarchy_levels

        results = []
        queue = deque(self._hierarchy_levels)
        while queue:
            level: LevelHierarchyWithTmoTprmsDto = queue.popleft()
            if level.current_level.id == level_id:
                results.extend(level.children)
                break
            queue.extend(level.children)
        return results

    async def _check_value_in_this_branch_recursive(
        self,
        parent_obj_id: str | None,
        level: LevelHierarchyWithTmoTprmsDto,
        level_obj_ids: list[str] | None,
    ):
        level_obj_ids = level_obj_ids or await self.get_child_obj_ids(
            parent_id=parent_obj_id, child_level_id=level.current_level.id
        )
        current_level_result = await self._check_current_level(
            obj_ids=level_obj_ids, level=level
        )
        if current_level_result:
            raise ElementFound()

        children_levels = level.children
        products = itertools.product(level_obj_ids, children_levels)

        async with TaskGroup() as task_group:
            for level_obj_id, children_level in products:
                task_group.create_task(
                    self._check_value_in_this_branch_recursive(
                        parent_obj_id=level_obj_id,
                        level=children_level,
                        level_obj_ids=None,
                    )
                )

    async def _check_value_in_this_branch(
        self, parent_obj_id: str | None, level: LevelHierarchyWithTmoTprmsDto
    ) -> bool:
        try:
            await self._check_value_in_this_branch_recursive(
                parent_obj_id=None, level=level, level_obj_ids=[parent_obj_id]
            )
        except ElementFound:
            return True
        return False

    async def execute(self):
        parent_id = self._parent_node.id if self._parent_node else None
        levels = self.filter_out_top_levels(parent_node=self._parent_node)
        task_children_nodes = []
        async with TaskGroup() as task_group:
            for level in levels:
                task_children_node = task_group.create_task(
                    self.get_child_obj_ids(
                        parent_id=parent_id,
                        child_level_id=level.current_level.id,
                    )
                )
                task_children_nodes.append((level, task_children_node))
        level_obj_ids = []
        for level, task_children_node in task_children_nodes:
            result = task_children_node.result()
            if not result:
                continue
            level_obj_ids.append((level, result))

        task_check_obj_ids = []
        async with TaskGroup() as task_group:
            for level, obj_ids in level_obj_ids:
                for obj_id in obj_ids:
                    task_check_obj_id = task_group.create_task(
                        self._check_value_in_this_branch(
                            parent_obj_id=obj_id, level=level
                        )
                    )
                    task_check_obj_ids.append((obj_id, task_check_obj_id))

        results = []
        for obj_id, task_result in task_check_obj_ids:
            if task_result.result():
                results.append(obj_id)

        return results
