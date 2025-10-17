from collections import defaultdict
from typing import TYPE_CHECKING

from v3.custom_exceptions.not_found import LevelNotFound, HierarchyNotFound
from v3.models.dto.hierarchy.level_way import LevelWay, LevelDto
from v3.models.input.operators.field import field
from v3.models.input.operators.field_operators.comparison import Eq

if TYPE_CHECKING:
    from v3.db.base_db import BaseTable


class LevelWayTask:
    """
    Plots the optimal route to the target level and screens out levels that do not need to be passed through
    """

    def __init__(
        self,
        hierarchy_id: int,
        level_id: int,
        hierarchy_table: "BaseTable",
        level_table: "BaseTable",
    ):
        self._hierarchy_id = hierarchy_id
        self._level_id = level_id
        self._hierarchy_table = hierarchy_table
        self._level_table = level_table

    async def get_hierarchy(self) -> dict | None:
        query = field(id=Eq(value=self._hierarchy_id))
        response = self._hierarchy_table.find_by_query(query=query)
        try:
            return anext(response)
        except StopAsyncIteration:
            return None

    async def get_hierarchy_levels(self) -> list[LevelDto]:
        query = field(hierarchy_id=Eq(value=self._hierarchy_id))
        response = self._level_table.find_by_query(query=query)
        result = []
        async for element in response:
            result.append(LevelDto.model_validate(element))
        return result

    def convert_levels_to_tree(self, levels: list[LevelDto]) -> LevelWay:
        tree: dict[int | None, list[int]] = defaultdict(
            list
        )  # parent_level: list[level_id]
        inverted_tree: dict[int, int | None] = {}  # level_id: parent_level
        levels_by_id = {}
        level_data: LevelDto | None = None
        for level in levels:
            levels_by_id[level.id] = level
            tree[level.parent_id].append(level.id)
            inverted_tree[level.id] = level.parent_id
            if level.id == self._level_id:
                level_data = level
        if not level_data:
            raise LevelNotFound("No target level found")
        target_way = LevelWay(level_data=level_data, is_target=True)

        # up
        way_for_return = target_way
        level_id = level_data.id
        while True:
            parent_way_id = inverted_tree[level_id]
            if parent_way_id is None or parent_way_id == 0:
                break
            parent_level_data = levels_by_id[parent_way_id]
            parent_way = LevelWay(
                level_data=parent_level_data,
                is_target=False,
                children=[way_for_return],
            )
            way_for_return = parent_way
            level_id = parent_way_id

        # down
        children_ways = [target_way]
        while children_ways:
            current_level: LevelWay = children_ways.pop()
            for child_level_id in tree.get(current_level.level_data.id, []):
                child_level_data = levels_by_id[child_level_id]
                child_way = LevelWay(
                    level_data=child_level_data, is_target=False
                )
                current_level.children.append(child_way)
                children_ways.append(child_way)

        return way_for_return

    async def execute(self):
        hierarchy = await self.get_hierarchy()
        if not hierarchy:
            raise HierarchyNotFound("No hierarchy found")
        levels = await self.get_hierarchy_levels()
        if not levels:
            raise LevelNotFound("No levels found")
        return self.convert_levels_to_tree(levels)
