from asyncio import TaskGroup
from collections import defaultdict, deque
from typing import AsyncIterator

from elasticsearch import AsyncElasticsearch

from elastic.enum_models import SearchOperator, LogicalOperator
from elastic.pydantic_models import HierarchyFilter, FilterColumn, FilterItem
from elastic.query_builder_service.inventory_index.mo_object.utils import (
    get_search_query_for_inventory_obj_index,
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
    HIERARCHY_NODE_DATA_INDEX,
    HIERARCHY_OBJ_INDEX,
)
from services.hierarchy_services.models.dto import NodeDTO
from services.inventory_services.converters.val_type_converter import (
    get_corresponding_python_val_type_for_elastic_val_type,
)
from v2.tasks.hierarchy.dtos.levels import LevelHierarchyWithTmoTprmsDto


class GetFilteredHierarchies:
    EXCLUDED_TYPES = {"object", "flattened"}
    STEP_SIZE = 100_000

    def __init__(
        self,
        hierarchy_levels: list[LevelHierarchyWithTmoTprmsDto],
        parent_node: NodeDTO | None,
        filters: list[HierarchyFilter] | None,
        elastic_client: AsyncElasticsearch,
        user_permission: UserPermission,
    ):
        self._hierarchy_levels = hierarchy_levels
        self._filters = filters
        self._parent_node = parent_node
        self._elastic_client = elastic_client
        self._user_permission = user_permission

        self.__grouped_filters_by_tmo_id = ...
        self.__parent_level_id = ...

    async def search_iterator(
        self, body: dict, index: str, ignore_unavailable: bool = True
    ) -> AsyncIterator:
        last_response_size = self.STEP_SIZE
        body["size"] = self.STEP_SIZE
        while last_response_size >= self.STEP_SIZE:
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

    @property
    def grouped_filters_by_tmo_id(self) -> dict[int, list[FilterColumn]]:
        if self.__grouped_filters_by_tmo_id is ...:
            grouped_filters_by_tmo_id = defaultdict(list)
            for tmo_filter in self._filters:
                if tmo_filter.filter_columns:
                    grouped_filters_by_tmo_id[tmo_filter.tmo_id].extend(
                        tmo_filter.filter_columns
                    )
            self.__grouped_filters_by_tmo_id = grouped_filters_by_tmo_id
        return self.__grouped_filters_by_tmo_id

    @property
    def parent_level_id(self) -> int:
        if self.__parent_level_id is ...:
            self.__parent_level_id = (
                self._parent_node.level_id if self._parent_node else None
            )
        return self.__parent_level_id

    def __create_dict_of_types(self, level: LevelHierarchyWithTmoTprmsDto):
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

    async def __get_level_obj_ids(
        self, parent_ids: set[str] | None, level_id: int
    ) -> list[str]:
        results = []
        if isinstance(parent_ids, set) and len(parent_ids) == 0:
            return results
        elif parent_ids is None:
            parent_ids = {""}

        query = {
            "bool": {
                "must": [
                    {"terms": {"parent_id": list(parent_ids)}},
                    {"term": {"level_id": level_id}},
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
            results[int(source["mo_id"])] = source["node_id"]
        return results

    async def __filter_level_data_by_mo_ids(
        self,
        mo_ids: list[int],
        level_filters: list[FilterColumn],
        level: LevelHierarchyWithTmoTprmsDto,
    ) -> list[int]:
        if not level_filters:
            return mo_ids
        dict_of_types = self.__create_dict_of_types(level=level)

        query = get_search_query_for_inventory_obj_index(
            filter_columns=level_filters, dict_of_types=dict_of_types
        )
        body = {
            "query": query,
            "sort": {"id": {"order": "asc"}},
            "track_total_hits": True,
            "_source": {"includes": ["id"]},
        }
        index = get_index_name_by_tmo(tmo_id=level.current_level.object_type_id)
        cursor = self.search_iterator(
            body=body, index=index, ignore_unavailable=True
        )
        results = []
        async for row in cursor:
            results.append(row["_source"]["id"])
        return results

    @staticmethod
    def __convert_mo_ids_to_obj_ids(
        obj_id_by_mo_ids: dict[int, str], mo_ids: list[int]
    ) -> set[str]:
        return {obj_id_by_mo_ids[i] for i in mo_ids}

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

    def is_child_level(self, level: LevelHierarchyWithTmoTprmsDto) -> bool:
        parent_level = level.parent
        parent_id = parent_level.current_level.id if parent_level else None
        return parent_id == self.parent_level_id

    def _filter_levels_to_child_of_level_id(
        self,
    ) -> list[LevelHierarchyWithTmoTprmsDto]:
        if not self.parent_level_id:
            return self._hierarchy_levels

        queue = deque(self._hierarchy_levels)
        while queue:
            level = queue.popleft()
            if level.current_level.id == self.parent_level_id:
                return level.children
            queue.extend(level.children)
        return []

    async def __children_filter_recursive_to_parent_values(
        self,
        level: LevelHierarchyWithTmoTprmsDto,
        filtered_obj_ids: set[str],
        filtered_tmo_ids: set[int],
    ) -> list[str]:
        result = []
        if level.children:
            is_child_level = self.is_child_level(level=level)
            task_results = []
            async with TaskGroup() as task_group:
                for child_level in level.children:
                    task_results.append(
                        task_group.create_task(
                            self.__filter_recursive_to_parent_values(
                                level=child_level,
                                parent_ids=filtered_obj_ids,
                                filtered_tmo_ids=filtered_tmo_ids.copy(),
                            )
                        )
                    )
            have_children_result = False
            for task_result in task_results:
                task_result_data = task_result.result()
                if not task_result_data:
                    continue
                for result_level in task_result_data:  # type: str
                    if is_child_level:
                        have_children_result = True
                        break
                    result.append(result_level)
                else:
                    continue
                break
            if have_children_result:
                result.extend(filtered_obj_ids)
        else:
            result.extend(filtered_obj_ids)
        return result

    async def __filter_recursive_to_parent_values(
        self,
        level: LevelHierarchyWithTmoTprmsDto,
        parent_ids: set[str] | None,
        filtered_tmo_ids: set[int] | None,
    ) -> list[str] | None:
        result: list[str] = []
        tmo_id = level.current_level.object_type_id
        level_filters = self.grouped_filters_by_tmo_id.get(tmo_id, [])
        level_obj_ids = await self.__get_level_obj_ids(
            parent_ids=parent_ids, level_id=level.current_level.id
        )
        if not level_obj_ids:
            return result
        obj_id_by_mo_id = await self.__get_obj_id_by_mo_ids(
            obj_ids=level_obj_ids
        )
        del level_obj_ids
        if not obj_id_by_mo_id:
            return result
        mo_ids = list(obj_id_by_mo_id.keys())

        if not filtered_tmo_ids:
            filtered_tmo_ids = set()
        if tmo_id not in filtered_tmo_ids:
            level_filters = self.__filter_add_permissions(filters=level_filters)
            if level_filters is None:
                mo_ids = []
            elif level_filters:
                filter_item = FilterItem(
                    operator=SearchOperator.IS_ANY_OF, value=mo_ids
                )
                filter_column = FilterColumn(
                    columnName="id",
                    rule=LogicalOperator.AND.value,
                    filters=[filter_item],
                )
                level_filters.append(filter_column)
                print(level_filters)
                mo_ids = await self.__filter_level_data_by_mo_ids(
                    level=level, level_filters=level_filters, mo_ids=mo_ids
                )

            filtered_tmo_ids.add(tmo_id)
        if not mo_ids:
            return result

        filtered_obj_ids = self.__convert_mo_ids_to_obj_ids(
            mo_ids=mo_ids, obj_id_by_mo_ids=obj_id_by_mo_id
        )
        del obj_id_by_mo_id
        del mo_ids

        result = await self.__children_filter_recursive_to_parent_values(
            filtered_tmo_ids=filtered_tmo_ids,
            filtered_obj_ids=filtered_obj_ids,
            level=level,
        )
        return result

    async def execute(self) -> list[str]:
        task_results = []
        levels = self._filter_levels_to_child_of_level_id()
        parent_ids = {self._parent_node.id} if self._parent_node else None
        async with TaskGroup() as task_group:
            for level in levels:
                task_results.append(
                    task_group.create_task(
                        self.__filter_recursive_to_parent_values(
                            level=level,
                            parent_ids=parent_ids,
                            filtered_tmo_ids=None,
                        )
                    )
                )
        results = []
        for task_result in task_results:
            result = task_result.result()
            if result:
                results.extend(result)
        return results
