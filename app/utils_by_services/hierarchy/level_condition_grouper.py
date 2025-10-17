from collections import defaultdict
from typing import List, Dict

from pydantic import BaseModel

from common_utils.dto_models.models import HierarchyAggregateByTMO

from elastic.pydantic_models import HierarchyFilter
from services.hierarchy_services.features.dependet_level_factory import (
    DependentLevelsChain,
)
from services.hierarchy_services.models.dto import HierarchyDTO, LevelDTO


class LevelConditions(BaseModel):
    level: LevelDTO
    hierarchy_create_empty_nodes: bool = False
    level_show_without_children: bool = False
    filter_condition: HierarchyFilter | None = None
    aggregation: HierarchyAggregateByTMO = None
    return_mo_ids_for_this_level: bool = False


class LevelsGroupedByDepth(BaseModel):
    depth: int
    levels_conditions: list[LevelConditions]
    inventory_filter_conditions_exist: bool = False
    aggregation_conditions_exist: bool = False
    level_filter_conditions_exist: bool = False
    level_for_inventory_results_exists: bool = False


class InventoryLevelConditionGrouper:
    """Groups levels by level.show_without_children"""

    def __init__(
        self,
        depends_level_chain: DependentLevelsChain,
        hierarchy: HierarchyDTO,
        hierarchy_filters: List[HierarchyFilter] = None,
        aggregation: HierarchyAggregateByTMO = None,
    ):
        self.depends_level_chain = depends_level_chain
        self.hierarchy = hierarchy
        self.hierarchy_filters = hierarchy_filters
        self.aggregation = aggregation

    def __group_aggregation_and_filters_by_depth(
        self,
    ) -> Dict[int, List[LevelConditions]]:
        """Groups levels by depth and conditions"""
        filters_grouped_by_tmo_id = dict()
        if self.hierarchy_filters:
            filters_grouped_by_tmo_id = {
                h_filter.tmo_id: h_filter for h_filter in self.hierarchy_filters
            }

        # level p_ids with inventory aggregation conditions by tmo_id
        level_p_ids_w_aggr_cond_by_tmo_id = defaultdict(set)

        # level_id, whose MO objects we should return
        level_for_inventory_results_exists = False

        level_conditions_by_level_id = dict()

        depth_and_level_cond = defaultdict(list)

        for level in self.depends_level_chain.lower_levels_ordered_by_depth_asc:
            # inventory_filter_conditions_exist = False
            # aggregation_conditions_exist = False
            # level_filter_conditions_exist = False

            # if not level.show_without_children:
            #     level_filter_conditions_exist = True

            level_cond = LevelConditions(
                level=level,
                hierarchy_create_empty_nodes=self.hierarchy.create_empty_nodes,
                level_show_without_children=level.show_without_children,
            )

            filter_for_spec_tmo = filters_grouped_by_tmo_id.get(
                level.object_type_id
            )
            level_conditions_by_level_id[level.id] = level_cond

            # get first real level - to returns mo_ids on handling
            if (
                not level.is_virtual
                and level_for_inventory_results_exists is False
            ):
                level_for_inventory_results_exists = True
                level_cond.return_mo_ids_for_this_level = True

            if filter_for_spec_tmo:
                level_cond.filter_condition = filter_for_spec_tmo

            if self.aggregation:
                if level.object_type_id == self.aggregation.tmo_id:
                    # check if direct_parent_with this aggregation cond

                    if (
                        level.object_type_id
                        in level_p_ids_w_aggr_cond_by_tmo_id
                    ):
                        l_p_ids_with_a_cond = (
                            level_p_ids_w_aggr_cond_by_tmo_id.get(
                                level.object_type_id
                            )
                        )

                        if level.parent_id in l_p_ids_with_a_cond:
                            parent_level_conditions = (
                                level_conditions_by_level_id[level.parent_id]
                            )
                            parent_level_conditions.aggregation = None

                        l_p_ids_with_a_cond.add(level.id)
                        level_cond.aggregation = self.aggregation
                        # aggregation_conditions_exist = True
                    else:
                        level_p_ids_w_aggr_cond_by_tmo_id[
                            level.object_type_id
                        ].add(level.id)
                        level_cond.aggregation = self.aggregation
                        # aggregation_conditions_exist = True
            level_depth = level.level
            depth_and_level_cond[level_depth].append(level_cond)

        return depth_and_level_cond

    def get_order_to_process(self) -> Dict[int, List[LevelConditions]]:
        # check first two depth
        grouped_by_depth = self.__group_aggregation_and_filters_by_depth()

        reverse_order = list(grouped_by_depth)[::-1]
        return {depth: grouped_by_depth[depth] for depth in reverse_order}
