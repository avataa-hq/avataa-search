import uuid
from asyncio import TaskGroup
from functools import reduce
from typing import AsyncIterator

from elasticsearch import AsyncElasticsearch

from elastic.config import INVENTORY_TPRM_INDEX_V2
from elastic.pydantic_models import HierarchyFilter
from indexes_mapping.inventory.mapping import INVENTORY_PERMISSIONS_FIELD_NAME
from security.security_data_models import UserPermission
from services.hierarchy_services.elastic.configs import HIERARCHY_OBJ_INDEX
from services.hierarchy_services.models.dto import NodeDTO
from services.inventory_services.utils.security.filter_by_realm import (
    get_only_available_to_read_tmo_ids_for_special_client,
)
from v2.tasks.hierarchy.dtos.levels import LevelHierarchyWithTmoTprmsDto
from v2.tasks.hierarchy.filter_hierarchies_with_value import (
    GetHierarchiesWithValue,
)
from v2.tasks.hierarchy.filtered_hierarchies import GetFilteredHierarchies
from v2.tasks.hierarchy.level_filter import LevelFilter
from v2.tasks.hierarchy.utils.get_node_or_raise_error import (
    get_node_or_raise_error,
)
from v2.tasks.utils.exceptions import (
    SearchPermissionException,
    SearchValueError,
    ElementNotFound,
)


class FilterHierarchiesCurrentLevel:
    STEP_SIZE = 100_000

    def __init__(
        self,
        elastic_client: AsyncElasticsearch,
        user_permissions: UserPermission,
        hierarchy_id: int,
        filters: list[HierarchyFilter] = None,
        parent_node_id: str = None,
    ):
        self._elastic_client = elastic_client
        self._user_permissions = user_permissions
        self._hierarchy_id = hierarchy_id
        self._filters = filters
        self._parent_node_id = parent_node_id

        self.__search_by_values: list[str] = ...
        self.__parent_node_id: uuid.UUID | None = ...
        self.__parent_node: NodeDTO | None = ...

    @property
    def search_by_values(self) -> list[str]:
        if self.__search_by_values is ...:
            search_by_values = set()
            for f in self._filters:
                if not f.search_by_value:
                    continue
                if f.search_by_value in search_by_values:
                    continue
                for v in search_by_values.copy():
                    if len(v) > len(f):
                        if f in v:
                            search_by_values.remove(v)
                            search_by_values.add(f)
                        continue
                    else:
                        if v in f:
                            continue
                        search_by_values.add(f)
            self.__search_by_values = list(search_by_values)
        return self.__search_by_values

    async def check(
        self,
    ):
        await self.check_permissions(user_permission=self._user_permissions)

    async def check_permissions(self, user_permission: UserPermission) -> None:
        if user_permission.is_admin:
            return
        all_tprms = set()
        all_tmos = set()
        if self._filters:
            for h_filter in self._filters:
                if not h_filter.filter_columns:
                    continue
                f_tprms = {
                    int(f_c.column_name)
                    for f_c in h_filter.filter_columns
                    if f_c.column_name.isdigit()
                }
                all_tprms.update(f_tprms)
                all_tmos.add(h_filter.tmo_id)
        if all_tprms:
            search_body = {
                "query": {
                    "bool": {
                        "must": [
                            {"terms": {"id": list(all_tprms)}},
                            {
                                "terms": {
                                    INVENTORY_PERMISSIONS_FIELD_NAME: user_permission.user_permissions
                                }
                            },
                        ]
                    }
                },
                "size": len(all_tprms),
                "_source": {"includes": "id"},
            }

            search_res = await self._elastic_client.search(
                index=INVENTORY_TPRM_INDEX_V2,
                body=search_body,
                ignore_unavailable=True,
            )

            search_res = search_res["hits"]["hits"]
            available_tprms = {item["_source"]["id"] for item in search_res}

            difference = all_tprms.difference(available_tprms)
            if difference:
                raise SearchPermissionException(
                    f"User has no permissions for tprms: {difference}"
                )

        if all_tmos:
            available_tmos = (
                await get_only_available_to_read_tmo_ids_for_special_client(
                    client_permissions=user_permission.user_permissions,
                    elastic_client=self._elastic_client,
                    tmo_ids=list(all_tmos),
                )
            )
            difference = set(all_tmos).difference(available_tmos)
            if difference:
                raise SearchPermissionException(
                    f"User has no permissions for tmos: {difference}"
                )

    @property
    def parent_node_id(self) -> uuid.UUID | None:
        if self.__parent_node_id is ...:
            if not self._parent_node_id:
                parent_node_id = None
            else:
                if self._parent_node_id.upper() in ("ROOT", "NONE", "NULL"):
                    parent_node_id = None
                else:
                    try:
                        parent_node_id = uuid.UUID(self._parent_node_id)
                    except ValueError:
                        raise SearchValueError(
                            "parent_node_id must be instance of UUID or be one of values: root, none, null"
                        )
            self.__parent_node_id = parent_node_id
        return self.__parent_node_id

    async def get_parent_node(self) -> NodeDTO | None:
        if self.__parent_node is ...:
            parent_node_id = self.parent_node_id
            parent_node = None
            if isinstance(parent_node_id, uuid.UUID):
                parent_node = await get_node_or_raise_error(
                    node_id=parent_node_id, elastic_client=self._elastic_client
                )
                node_hierarchy_id_as_int = int(parent_node["hierarchy_id"])
                if node_hierarchy_id_as_int != self._hierarchy_id:
                    raise SearchValueError(
                        "Parent node hierarchy id does not match hierarchy_id "
                        f"({node_hierarchy_id_as_int} != {self._hierarchy_id})"
                    )
                is_active = parent_node.get("active")
                if not is_active:
                    raise SearchValueError("Parent node is not active")
            self.__parent_node = (
                NodeDTO.model_validate(parent_node) if parent_node else None
            )
        return self.__parent_node

    async def get_object_ids_by_filters(
        self,
        levels: list[LevelHierarchyWithTmoTprmsDto],
        parent_node: NodeDTO | None,
    ) -> list[str]:
        task = GetFilteredHierarchies(
            hierarchy_levels=levels,
            parent_node=parent_node,
            filters=self._filters,
            elastic_client=self._elastic_client,
            user_permission=self._user_permissions,
        )
        return await task.execute()

    async def get_object_ids_by_value(
        self,
        levels: list[LevelHierarchyWithTmoTprmsDto],
        parent_node: NodeDTO | None,
        search_by_value: str,
    ) -> list[str]:
        task = GetHierarchiesWithValue(
            hierarchy_levels=levels,
            parent_node=parent_node,
            search_by_value=search_by_value,
            elastic_client=self._elastic_client,
            user_permission=self._user_permissions,
        )
        result = await task.execute()
        if not result:
            raise ElementNotFound()
        return result

    async def get_levels(self) -> list[LevelHierarchyWithTmoTprmsDto]:
        task = LevelFilter(
            elastic_client=self._elastic_client,
            hierarchy_id=self._hierarchy_id,
            filters=self._filters,
        )
        return await task.execute()

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

    async def replace_id_with_values(self, obj_ids: list[str]) -> list[NodeDTO]:
        if not obj_ids:
            return []
        query = {"bool": {"must": [{"terms": {"id": obj_ids}}]}}
        body = {
            "query": query,
            "sort": {"id": {"order": "asc"}},
            "track_total_hits": True,
        }
        index = HIERARCHY_OBJ_INDEX
        cursor = self.search_iterator(
            body=body, index=index, ignore_unavailable=True
        )
        results = []
        async for row in cursor:
            source = row["_source"]
            result = NodeDTO.model_validate(source)
            results.append(result)
        return results

    async def execute(self):
        await self.check()

        results = []
        levels = await self.get_levels()
        if not levels:
            return results

        parent_node = await self.get_parent_node()
        obj_ids: list[str] = await self.get_object_ids_by_filters(
            levels=levels, parent_node=parent_node
        )
        if not obj_ids:
            return results

        if self.search_by_values:
            search_by_value_tasks = []
            try:
                async with TaskGroup() as task_group:
                    for search_by_value in self.search_by_values:
                        search_by_value_task = task_group.create_task(
                            self.get_object_ids_by_value(
                                levels=levels,
                                parent_node=parent_node,
                                search_by_value=search_by_value,
                            )
                        )
                        search_by_value_tasks.append(search_by_value_task)
            except ElementNotFound:
                return results
            search_by_value_results: list[set[str]] = [
                set(i.result()) for i in search_by_value_tasks
            ]
            search_by_value_results.append(set(obj_ids))
            obj_ids = list(
                reduce(lambda x, y: y.intersection(x), search_by_value_results)
            )
        return await self.replace_id_with_values(obj_ids=obj_ids)
