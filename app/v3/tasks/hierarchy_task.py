from asyncio import TaskGroup
from collections import defaultdict
from copy import deepcopy
from typing import TYPE_CHECKING, Type, get_origin

from elasticsearch import AsyncElasticsearch

from v3.models.dto.hierarchy.hierarchy_recursive_response import (
    HierarchyRecursiveResponse,
    HierarchyRecursiveFilterDto,
)
from v3.models.input.operators.field import field
from v3.models.input.operators.field_operators.comparison import In, Eq
from v3.models.input.operators.logical_operators.logical import And

if TYPE_CHECKING:
    from v3.models.input.hierarchy.hierarchy_filter import LevelFilter  # noqa
    from v3.models.input.operators.input_union import base_operators_union
    from v3.db.base_db import BaseTable
    from v3.models.dto.hierarchy.level_way import LevelWay
    from v3.models.input.hierarchy.hierarchy_filter import HierarchyFilterModel


field_origin = get_origin(field)


class InvByHierarchyTask:
    """
    Traverses a route of layers, performs data filtering at each layer, passes the parents to the layer below.
    When the lowest level is reached, forwards responses upwards until it reaches the target level.
    This way we get only valid data for the target level
    """

    def __init__(
        self,
        query: "HierarchyFilterModel",
        level_way: "LevelWay",
        hierarchy_obj_table_class: Type["BaseTable"],
        node_table_class: Type["BaseTable"],
        mo_table: Type["BaseTable"],
        connection: AsyncElasticsearch,
        is_admin: bool = True,
        permissions: list[str] | None = None,
    ):
        self.query = query
        self.level_way = level_way
        self.mo_table_class = mo_table
        self.hierarchy_obj_table_class = hierarchy_obj_table_class
        self.node_table_class = node_table_class
        self.connection = connection
        self.is_admin = is_admin
        self.permissions = permissions

    @property
    def hierarchy_id(self):
        return self.query.filters.hierarchy_id

    def _get_hierarchy_filters_by_level_ids(
        self,
    ) -> dict[int | None, "base_operators_union"]:
        """
        Create a dictionary of filters for the hierarchy for each level
        """
        # None is default filter
        filters_by_level_ids: dict[int | None, "base_operators_union"] = {}

        base_and_filter = [
            field(hierarchy_id=Eq(value=self.query.filters.hierarchy_id))
        ]
        if self.query.filters.active is not None:
            base_and_filter.append(
                field(active=Eq(value=self.query.filters.active))
            )
        filters_by_level_ids[None] = (
            base_and_filter[0]
            if len(base_and_filter) == 1
            else And(value=base_and_filter)
        )

        for level in self.query.filters.level_filters:  # type: LevelFilter
            level_and_filter = deepcopy(base_and_filter)
            if level.path:
                level_and_filter.append(field(path=level.path))
            if level.id:
                level_and_filter.append(field(level_id=level.id))
            if level.parent_id:
                level_and_filter.append(field(parent_id=level.parent_id))
            if level.key:
                level_and_filter.append(field(key=level.key))

            if len(level_and_filter) == 1:
                filters_by_level_ids[level.level_id] = level_and_filter[0]
            else:
                filters_by_level_ids[level.level_id] = And(
                    value=level_and_filter
                )

        return filters_by_level_ids

    def _get_inventory_filters_by_level_ids(
        self,
    ) -> dict[int | None, "base_operators_union"]:
        """
        Create a dictionary of filters for the inventory for each level
        """
        filters_by_level_ids: dict[int | None, "base_operators_union"] = {}

        for level in self.query.filters.level_filters:  # type: LevelFilter
            if not level.mo_filters:
                continue
            filters_by_level_ids[level.level_id] = level.mo_filters

        return filters_by_level_ids

    @staticmethod
    async def _filter_level_data(
        parent_ids: list[str] | None,
        hierarchy_filters_by_level_ids: dict[
            int | None, "base_operators_union"
        ],
        level_way: "LevelWay",
        obj_table: "BaseTable",
        node_table: "BaseTable",
    ) -> HierarchyRecursiveFilterDto:
        level_id = level_way.level_data.id

        # filter hierarchy obj
        hier_level_filt = hierarchy_filters_by_level_ids.get(level_id, None)
        if not hier_level_filt:
            # default filter
            hier_level_filt = deepcopy(
                hierarchy_filters_by_level_ids.get(None, None)
            )

            # appending filter by level_id
            if hier_level_filt:
                level_filter_part = field(level_id=Eq(value=level_id))
                if isinstance(hier_level_filt, And):
                    hier_level_filt.value.append(level_filter_part)
                elif isinstance(hier_level_filt, field_origin):
                    hier_level_filt = And(
                        value=[hier_level_filt, level_filter_part]
                    )
                else:
                    hier_level_filt = And(
                        value=[And(value=level_filter_part), hier_level_filt]
                    )

        if isinstance(parent_ids, list):
            if len(parent_ids) == 0:
                return HierarchyRecursiveFilterDto(
                    node_ids=[],
                    mo_ids_by_node_id=[],
                    node_mo_ids=[],
                    hier_response=[],
                    hier_without_children=[],
                )
            if isinstance(hier_level_filt, And):
                query = deepcopy(hier_level_filt)
                query.value.append(field(parent_id=In(value=parent_ids)))
            else:
                query = And(
                    value=[
                        And(value=[field(parent_id=In(value=parent_ids))]),
                        hier_level_filt,
                    ]
                )
        else:
            query = hier_level_filt
        hier_without_children = []
        hier_response = {}
        async for elem in obj_table.find_by_query(
            query=query, includes=["id", "parent_id", "child_count"]
        ):
            if int(elem.get("child_count", 0)) == 0:
                hier_without_children.append(elem["id"])
            hier_response[elem["id"]] = elem["parent_id"]

        # filter hierarchy node
        query = field(node_id=In(value=list(hier_response.keys())))
        node_response_iter = node_table.find_by_query(
            query=query, includes=["mo_id", "node_id"]
        )
        mo_ids_by_node_id = defaultdict(set)
        async for elem in node_response_iter:
            mo_ids_by_node_id[elem["node_id"]].add(elem["mo_id"])

        return HierarchyRecursiveFilterDto(
            mo_ids_by_node_id=mo_ids_by_node_id,
            hier_response=hier_response,
            hier_without_children=hier_without_children,
        )

    async def _collect_children_responses(
        self,
        level_way: "LevelWay",
        hierarchy_filters_by_level_ids: dict[
            int | None, "base_operators_union"
        ],
        inventory_filters_by_level_ids: dict[
            int | None, "base_operators_union"
        ],
        node_ids: set[str],
    ):
        children_tasks = []
        async with TaskGroup() as tg:
            for child_level in level_way.children:
                task = tg.create_task(
                    self._recursive_find(
                        hierarchy_filters_by_level_ids=hierarchy_filters_by_level_ids,
                        inventory_filters_by_level_ids=inventory_filters_by_level_ids,
                        parent_ids=list(node_ids),
                        level_way=child_level,
                    )
                )
                children_tasks.append(task)
        target_results = []
        child_node_ids = set()
        for child_task in children_tasks:
            child_result: HierarchyRecursiveResponse = child_task.result()
            if child_result.is_target_response:
                target_results.extend(child_result.data)
            else:
                child_node_ids.update(child_result.parent_ids)
        return target_results, child_node_ids

    async def _prepare_level_response_children_target(
        self,
        level_way: "LevelWay",
        inventory_filters_by_level_ids: dict[
            int | None, "base_operators_union"
        ],
        mo_ids_by_node_id: dict[str, set[int]],
        mo_table: "BaseTable",
        obj_table: "BaseTable",
        level_id: int,
        filtered_node_ids: set[str],
    ):
        filtered_mo_ids = [
            j for i in filtered_node_ids for j in mo_ids_by_node_id[i]
        ]
        inventory_level_filt = inventory_filters_by_level_ids.get(
            level_id, None
        )
        if not inventory_level_filt:
            inventory_level_filt = field(id=In(value=filtered_mo_ids))
        else:
            inventory_level_filt = And(
                value=[
                    field(id=In(value=filtered_mo_ids)),
                    inventory_level_filt,
                ]
            )
        inv_response = [
            i async for i in mo_table.find_by_query(query=inventory_level_filt)
        ]
        return HierarchyRecursiveResponse(
            is_target_response=level_way.is_target, data=inv_response
        )

    async def _prepare_level_response_children_not_target(
        self,
        level_way: "LevelWay",
        hier_response: dict[str, str],  # dict[id, parent_id]
        filtered_node_ids: set[str],
    ):
        filtered_parent_ids = set([hier_response[i] for i in filtered_node_ids])
        return HierarchyRecursiveResponse(
            is_target_response=level_way.is_target,
            parent_ids=filtered_parent_ids,
        )

    async def _prepare_level_response_children(
        self,
        level_way: "LevelWay",
        hierarchy_filters_by_level_ids: dict[
            int | None, "base_operators_union"
        ],
        inventory_filters_by_level_ids: dict[
            int | None, "base_operators_union"
        ],
        mo_ids_by_node_id: dict[str, set[int]],
        mo_table: "BaseTable",
        obj_table: "BaseTable",
        hier_response: dict[str, str],  # dict[id, parent_id]
        hier_without_children: list[str],
        node_ids: set[str],
        level_id: int,
    ):
        target_results, child_node_ids = await self._collect_children_responses(
            level_way=level_way,
            hierarchy_filters_by_level_ids=hierarchy_filters_by_level_ids,
            inventory_filters_by_level_ids=inventory_filters_by_level_ids,
            node_ids=node_ids,
        )
        if target_results:
            return HierarchyRecursiveResponse(
                is_target_response=True, data=target_results
            )

        filtered_node_ids = node_ids.intersection(child_node_ids)
        filtered_node_ids.update(hier_without_children)
        if level_way.is_target:
            return await self._prepare_level_response_children_target(
                level_way=level_way,
                level_id=level_id,
                mo_table=mo_table,
                obj_table=obj_table,
                mo_ids_by_node_id=mo_ids_by_node_id,
                inventory_filters_by_level_ids=inventory_filters_by_level_ids,
                filtered_node_ids=filtered_node_ids,
            )
        else:
            return await self._prepare_level_response_children_not_target(
                level_way=level_way,
                hier_response=hier_response,
                filtered_node_ids=filtered_node_ids,
            )

    async def _prepare_level_response_without_children_target(
        self,
        level_way: "LevelWay",
        inventory_filters_by_level_ids: dict[
            int | None, "base_operators_union"
        ],
        mo_ids_by_node_id: dict[str, set[int]],
        mo_table: "BaseTable",
        obj_table: "BaseTable",  # noqa
        level_id: int,
        extended_node_ids: set[str],
    ):
        mo_ids = set()
        for node_id in extended_node_ids:
            mo_ids.update(mo_ids_by_node_id[node_id])
        inventory_level_filt = inventory_filters_by_level_ids.get(
            level_id, None
        )
        if not inventory_level_filt:
            inventory_level_filt = field(id=In(value=list(mo_ids)))
        else:
            inventory_level_filt = And(
                value=[field(id=In(value=list(mo_ids))), inventory_level_filt]
            )
        inv_response = [
            i async for i in mo_table.find_by_query(query=inventory_level_filt)
        ]
        return HierarchyRecursiveResponse(
            is_target_response=level_way.is_target, data=inv_response
        )

    async def _prepare_level_response_without_children_not_target(
        self,
        level_way: "LevelWay",
        extended_node_ids: set[str],
        hier_response: dict[str, str],  # dict[id, parent_id]
    ):
        filtered_parent_ids = set([hier_response[i] for i in extended_node_ids])
        return HierarchyRecursiveResponse(
            is_target_response=level_way.is_target,
            parent_ids=filtered_parent_ids,
        )

    async def _prepare_level_response_without_children(
        self,
        level_way: "LevelWay",
        inventory_filters_by_level_ids: dict[
            int | None, "base_operators_union"
        ],
        mo_ids_by_node_id: dict[str, set[int]],
        mo_table: "BaseTable",
        obj_table: "BaseTable",
        hier_response: dict[str, str],  # dict[id, parent_id]
        hier_without_children: list[str],
        node_ids: set[str],
        level_id: int,
    ):
        extended_node_ids = set(hier_without_children)
        extended_node_ids.update(node_ids)
        if level_way.is_target:
            return await self._prepare_level_response_without_children_target(
                level_way=level_way,
                level_id=level_id,
                mo_table=mo_table,
                obj_table=obj_table,
                mo_ids_by_node_id=mo_ids_by_node_id,
                inventory_filters_by_level_ids=inventory_filters_by_level_ids,
                extended_node_ids=extended_node_ids,
            )
        else:
            return (
                await self._prepare_level_response_without_children_not_target(
                    level_way=level_way,
                    extended_node_ids=extended_node_ids,
                    hier_response=hier_response,
                )
            )

    async def _prepare_level_response(
        self,
        level_way: "LevelWay",
        hierarchy_filters_by_level_ids: dict[
            int | None, "base_operators_union"
        ],
        inventory_filters_by_level_ids: dict[
            int | None, "base_operators_union"
        ],
        mo_ids_by_node_id: dict[str, set[int]],
        mo_table: "BaseTable",
        obj_table: "BaseTable",
        hier_response: dict[str, str],  # dict[id, parent_id]
        hier_without_children: list[str],
    ):
        level_id = level_way.level_data.id
        node_ids = set(mo_ids_by_node_id.keys())

        if level_way.children:
            return await self._prepare_level_response_children(
                level_way=level_way,
                level_id=level_id,
                mo_table=mo_table,
                obj_table=obj_table,
                mo_ids_by_node_id=mo_ids_by_node_id,
                hier_response=hier_response,
                hier_without_children=hier_without_children,
                inventory_filters_by_level_ids=inventory_filters_by_level_ids,
                hierarchy_filters_by_level_ids=hierarchy_filters_by_level_ids,
                node_ids=node_ids,
            )
        else:
            return await self._prepare_level_response_without_children(
                level_way=level_way,
                level_id=level_id,
                mo_table=mo_table,
                obj_table=obj_table,
                mo_ids_by_node_id=mo_ids_by_node_id,
                hier_response=hier_response,
                hier_without_children=hier_without_children,
                inventory_filters_by_level_ids=inventory_filters_by_level_ids,
                node_ids=node_ids,
            )

    async def _recursive_find(
        self,
        hierarchy_filters_by_level_ids: dict[
            int | None, "base_operators_union"
        ],
        inventory_filters_by_level_ids: dict[
            int | None, "base_operators_union"
        ],
        parent_ids: list[str] | None,
        level_way: "LevelWay",
    ) -> HierarchyRecursiveResponse:
        mo_table = self.mo_table_class(
            connection=self.connection,
            table_id=level_way.level_data.object_type_id,
            is_admin=self.is_admin,
            permissions=self.permissions,
        )
        node_table = self.node_table_class(
            connection=self.connection,
            mo_table=mo_table,
            is_admin=self.is_admin,
            permissions=self.permissions,
        )
        obj_table = self.hierarchy_obj_table_class(
            connection=self.connection,
            nodes_table=node_table,
            is_admin=self.is_admin,
            permissions=self.permissions,
        )

        # filter
        filter_response: HierarchyRecursiveFilterDto = (
            await self._filter_level_data(
                parent_ids=parent_ids,
                hierarchy_filters_by_level_ids=hierarchy_filters_by_level_ids,
                level_way=level_way,
                obj_table=obj_table,
                node_table=node_table,
            )
        )
        mo_ids_by_node_id = filter_response.mo_ids_by_node_id
        hier_response = filter_response.hier_response
        hier_without_children = filter_response.hier_without_children
        if not mo_ids_by_node_id:
            return HierarchyRecursiveResponse(
                is_target_response=level_way.is_target
            )

        # same level recursion
        if level_way.level_data.attr_as_parent:
            same_level_way_children = deepcopy(level_way)
            same_level_way_children.children = []
            same_level_way_children.is_target = False

            same_level_parent_ids = (
                set(mo_ids_by_node_id.keys()) if mo_ids_by_node_id else None
            )
            while same_level_parent_ids:
                # child filter loop
                same_level_filter_response: HierarchyRecursiveFilterDto = await self._filter_level_data(
                    parent_ids=list(same_level_parent_ids),
                    hierarchy_filters_by_level_ids=hierarchy_filters_by_level_ids,
                    level_way=same_level_way_children,
                    obj_table=obj_table,
                    node_table=node_table,
                )
                same_level_parent_ids = (
                    same_level_filter_response.node_parent_ids
                )
                hier_without_children.extend(
                    same_level_filter_response.hier_without_children
                )
                for (
                    key,
                    value,
                ) in same_level_filter_response.node_mo_ids_by_parent_id:
                    mo_ids_by_node_id[key].update(value)

        response = await self._prepare_level_response(
            level_way=level_way,
            mo_table=mo_table,
            obj_table=obj_table,
            mo_ids_by_node_id=mo_ids_by_node_id,
            hierarchy_filters_by_level_ids=hierarchy_filters_by_level_ids,
            inventory_filters_by_level_ids=inventory_filters_by_level_ids,
            hier_response=hier_response,
            hier_without_children=hier_without_children,
        )
        return response

    async def execute(self) -> list[dict]:
        hierarchy_filters_by_level_ids = (
            self._get_hierarchy_filters_by_level_ids()
        )
        inventory_filters_by_level_ids = (
            self._get_inventory_filters_by_level_ids()
        )

        response = await self._recursive_find(
            hierarchy_filters_by_level_ids=hierarchy_filters_by_level_ids,
            inventory_filters_by_level_ids=inventory_filters_by_level_ids,
            parent_ids=None,
            level_way=self.level_way,
        )
        if not response.is_target_response:
            return []
        return response.data


class Hierarchy(InvByHierarchyTask):
    """
    It does the same thing as the InvByHierarchyTask class,
    but returns hierarchy objects instead of inventory objects in the response
    """

    async def _prepare_level_response_children_target(
        self,
        level_way: "LevelWay",
        inventory_filters_by_level_ids: dict[
            int | None, "base_operators_union"
        ],
        mo_ids_by_node_id: dict[str, set[int]],
        mo_table: "BaseTable",
        obj_table: "BaseTable",
        level_id: int,
        filtered_node_ids: set[str],
    ):
        hier_filter = field(id=In(value=list(filtered_node_ids)))
        inv_response = [
            i async for i in obj_table.find_by_query(query=hier_filter)
        ]
        return HierarchyRecursiveResponse(
            is_target_response=level_way.is_target, data=inv_response
        )

    async def _prepare_level_response_without_children_target(
        self,
        level_way: "LevelWay",
        inventory_filters_by_level_ids: dict[
            int | None, "base_operators_union"
        ],
        mo_ids_by_node_id: dict[str, set[int]],
        mo_table: "BaseTable",
        obj_table: "BaseTable",  # noqa
        level_id: int,
        extended_node_ids: set[str],
    ):
        hier_filter = field(id=In(value=list(extended_node_ids)))
        inv_response = [
            i async for i in obj_table.find_by_query(query=hier_filter)
        ]
        return HierarchyRecursiveResponse(
            is_target_response=level_way.is_target, data=inv_response
        )
