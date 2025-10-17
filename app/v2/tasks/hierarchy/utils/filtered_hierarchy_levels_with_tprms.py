from collections import defaultdict, deque
from typing import AsyncIterator

from elasticsearch import AsyncElasticsearch

from elastic.config import INVENTORY_TPRM_INDEX_V2
from elastic.pydantic_models import HierarchyFilter
from services.hierarchy_services.elastic.configs import HIERARCHY_LEVELS_INDEX
from services.hierarchy_services.models.dto import LevelDTO
from v2.tasks.hierarchy.dtos.levels import (
    LevelHierarchyDto,
    LevelHierarchyWithTmoTprmsDto,
    TprmDto,
    FilteredLevelsDto,
)
from v2.tasks.utils.exceptions import SearchValueError


class LevelFilter:
    STEP_SIZE = 100_000

    def __init__(
        self,
        elastic_client: AsyncElasticsearch,
        hierarchy_id: int,
        filters: list[HierarchyFilter] | None = None,
    ):
        self._elastic_client = elastic_client
        self._hierarchy_id = hierarchy_id
        self._filters: list[HierarchyFilter] = filters or []

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

    async def get_level_hierarchy(self) -> list[LevelHierarchyDto]:
        hierarchical_level_dependency: dict[int | None, list[int]] = (
            defaultdict(list)
        )
        hierarchy_levels: dict[int, LevelDTO] = {}

        search_body = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"hierarchy_id": self._hierarchy_id}},
                        {"exists": {"field": "level"}},
                    ]
                }
            },
            "sort": {"level": {"order": "desc"}},
            "track_total_hits": True,
        }

        cursor = self.search_iterator(
            body=search_body,
            index=HIERARCHY_LEVELS_INDEX,
            ignore_unavailable=True,
        )
        async for level_data in cursor:
            level_dto = LevelDTO.model_validate(level_data["_source"])
            hierarchy_levels[level_dto.id] = level_dto
            hierarchical_level_dependency[level_dto.parent_id].append(
                level_dto.id
            )

        top_level_ids = (
            hierarchical_level_dependency.get(0)
            or hierarchical_level_dependency.get(None)
            or None
        )
        results = []
        for top_level_id in top_level_ids:
            hierarchy_level = hierarchy_levels.get(top_level_id, None)
            if hierarchy_level is None:
                continue
            level_hierarchy_dto = LevelHierarchyDto(
                current_level=hierarchy_level
            )
            results.append(level_hierarchy_dto)

            queue = deque([level_hierarchy_dto])
            while queue:
                current_level_hierarchy_dto: LevelHierarchyDto = queue.popleft()
                current_level_dto: LevelDTO = (
                    current_level_hierarchy_dto.current_level
                )

                children_ids = hierarchical_level_dependency.get(
                    current_level_dto.id, []
                )
                children_level_hierarchy_dtos = []
                for child_id in children_ids:
                    child_level = hierarchy_levels.get(child_id, None)
                    if child_level is None:
                        continue
                    child_level_hierarchy_dto = LevelHierarchyDto(
                        current_level=child_level,
                        parent=current_level_hierarchy_dto,
                    )
                    children_level_hierarchy_dtos.append(
                        child_level_hierarchy_dto
                    )
                current_level_hierarchy_dto.children.extend(
                    children_level_hierarchy_dtos
                )
                queue.extend(children_level_hierarchy_dtos)
        return results

    async def get_tprms_by_tmo_ids(
        self, tmo_ids: list[int]
    ) -> dict[int, dict[int, TprmDto]]:
        results: dict[int, dict[int, TprmDto]] = defaultdict(dict)
        if not tmo_ids:
            return results

        body = {
            "query": {
                "bool": {
                    "must": [
                        {"terms": {"tmo_id": tmo_ids}},
                    ]
                }
            },
            "sort": {"id": {"order": "desc"}},
            "track_total_hits": True,
        }
        cursor = self.search_iterator(
            body=body, index=INVENTORY_TPRM_INDEX_V2, ignore_unavailable=True
        )
        async for tprm in cursor:
            tprm_dto = TprmDto.model_validate(tprm["_source"])
            results[tprm_dto.tmo_id][tprm_dto.id] = tprm_dto
        return results

    async def saturate_levels_with_tprms(
        self, levels: list[LevelHierarchyDto]
    ) -> list[LevelHierarchyWithTmoTprmsDto]:
        unique_tmo_ids: set[int] = set()
        levels_by_id: dict[int, LevelHierarchyDto] = {}

        queue = deque(levels)
        while queue:
            current_level_hierarchy_dto: LevelHierarchyDto = queue.popleft()
            levels_by_id[current_level_hierarchy_dto.current_level.id] = (
                current_level_hierarchy_dto
            )
            unique_tmo_ids.add(
                current_level_hierarchy_dto.current_level.object_type_id
            )
            queue.extend(current_level_hierarchy_dto.children)

        tprms_by_tprm_id_by_tmo_id = await self.get_tprms_by_tmo_ids(
            tmo_ids=list(unique_tmo_ids)
        )

        results: list[LevelHierarchyWithTmoTprmsDto] = []
        for level in levels:
            tprms_by_tprm_id = tprms_by_tprm_id_by_tmo_id.get(
                level.current_level.object_type_id, {}
            )
            result = LevelHierarchyWithTmoTprmsDto(
                current_level=level.current_level,
                current_level_tprms_by_tprm_id=tprms_by_tprm_id,
            )
            results.append(result)

            queue = deque([result])
            while queue:
                current_child: LevelHierarchyWithTmoTprmsDto = queue.popleft()
                current_child_level_dto: LevelDTO = current_child.current_level
                current_level: LevelHierarchyDto = levels_by_id.get(
                    current_child_level_dto.id, None
                )
                if not current_level:
                    continue
                for child in current_level.children:  # type: LevelHierarchyDto
                    child_tprms_by_tprm_id = tprms_by_tprm_id_by_tmo_id.get(
                        child.current_level.object_type_id, {}
                    )
                    child_result = LevelHierarchyWithTmoTprmsDto(
                        current_level=child.current_level,
                        current_level_tprms_by_tprm_id=child_tprms_by_tprm_id,
                        parent=current_child,
                    )
                    current_child.children.append(child_result)
                    queue.append(child_result)

        return results

    @staticmethod
    def collect_tprm_ids_from_filters(filters: list[HierarchyFilter]):
        all_tprm_ids = set()
        for h_filter in filters:
            if not h_filter.filter_columns:
                continue
            f_tprms = {
                int(f_c.column_name)
                for f_c in h_filter.filter_columns
                if f_c.column_name.isdigit()
            }
            all_tprm_ids.update(f_tprms)
        return all_tprm_ids

    def _filter_saturated_level_recursive(
        self,
        level_data: FilteredLevelsDto,
    ) -> FilteredLevelsDto:
        if not level_data.left_tprm_ids:
            return level_data
        if not level_data.level.children:
            return level_data

        left_tprm_ids = set(level_data.left_tprm_ids).difference(
            level_data.level.current_level_tprms_by_tprm_id.keys()
        )

        results_by_left_tprm_ids_count: dict[int, list[FilteredLevelsDto]] = (
            defaultdict(list)
        )
        for child_level in level_data.level.children:
            child_level_data = FilteredLevelsDto(
                level=child_level, left_tprm_ids=left_tprm_ids
            )
            child_level_result = self._filter_saturated_level_recursive(
                level_data=child_level_data
            )
            results_by_left_tprm_ids_count[
                len(child_level_result.left_tprm_ids)
            ].append(child_level_result)
        min_key = min(results_by_left_tprm_ids_count.keys())
        level = LevelHierarchyWithTmoTprmsDto(
            current_level=level_data.level.current_level,
            current_level_tprms_by_tprm_id=level_data.level.current_level_tprms_by_tprm_id,
            parent=level_data.level.parent,
            children=[i.level for i in results_by_left_tprm_ids_count[min_key]],
        )
        left_tprm_ids = set(
            j
            for i in results_by_left_tprm_ids_count[min_key]
            for j in i.left_tprm_ids
        )
        return FilteredLevelsDto(level=level, left_tprm_ids=left_tprm_ids)

    def filter_saturated_levels(
        self,
        levels: list[LevelHierarchyWithTmoTprmsDto],
    ) -> list[LevelHierarchyWithTmoTprmsDto]:
        tprm_ids = self.collect_tprm_ids_from_filters(self._filters)
        if not tprm_ids:
            return levels

        results_by_left_tprm_ids_count: dict[int, list[FilteredLevelsDto]] = (
            defaultdict(list)
        )
        for level in levels:
            level_data = FilteredLevelsDto(level=level, left_tprm_ids=tprm_ids)
            level_result = self._filter_saturated_level_recursive(
                level_data=level_data
            )
            results_by_left_tprm_ids_count[
                len(level_result.left_tprm_ids)
            ].append(level_result)
        min_key = min(results_by_left_tprm_ids_count.keys())
        left_tprm_ids = set(
            j
            for i in results_by_left_tprm_ids_count[min_key]
            for j in i.left_tprm_ids
        )
        if min_key != 0:
            raise SearchValueError(
                f"Filtering by the following TPRM_ID is not compatible with other filters."
                f"Tprm_ids: {list(left_tprm_ids)}"
            )
        return [i.level for i in results_by_left_tprm_ids_count[min_key]]

    async def execute(self) -> list[LevelHierarchyWithTmoTprmsDto]:
        level_hierarchy = await self.get_level_hierarchy()
        saturated_levels = await self.saturate_levels_with_tprms(
            levels=level_hierarchy
        )
        filtered_levels = self.filter_saturated_levels(levels=saturated_levels)
        return filtered_levels
