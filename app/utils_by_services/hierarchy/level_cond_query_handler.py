from collections import defaultdict
from typing import Union

from elasticsearch import AsyncElasticsearch
from pydantic import BaseModel

from common_utils.dto_models.models import HierarchyAggregateByTMO
from elastic.query_builder_service.inventory_index.utils.index_utils import (
    get_index_name_by_tmo,
)
from indexes_mapping.inventory.mapping import INVENTORY_PARAMETERS_FIELD_NAME
from security.security_data_models import UserPermission
from services.hierarchy_services.elastic.configs import (
    HIERARCHY_NODE_DATA_INDEX,
    HIERARCHY_OBJ_INDEX,
)
from services.hierarchy_services.features.node_features import (
    create_path_for_children_node_by_parent_node,
)
from services.hierarchy_services.models.dto import NodeDTO, LevelDTO
from services.inventory_services.mo_link.common import async_timing_decorator
from utils_by_services.hierarchy.level_cond_query_builder import (
    LevelConditionsQueryBuilder,
)
from utils_by_services.hierarchy.level_condition_grouper import LevelConditions
from itertools import chain

from utils_by_services.hierarchy.models import HierarchyModel, InventoryModel
from utils_by_services.inventory.common import search_after_generator_with_total
from v2.routers.hierarchy.utils.response_models import (
    HierarchyFilterHierarchyItem,
    HierarchyFilterInventoryItem,
)


class LevelConditionsOrderResults(BaseModel):
    nodes: list = list()
    level: LevelDTO | None = None
    mo_ids: list = list()


class LevelConditionsOrderHandler:
    def __init__(
        self,
        level_cond_order: dict[int, list[LevelConditions]],
        elastic_client: AsyncElasticsearch,
        parent_node_dto: NodeDTO = None,
        user_permissions: UserPermission = None,
    ):
        self.level_cond_order = level_cond_order
        self.parent_node_dto = parent_node_dto
        self.elastic_client = elastic_client
        self.user_permissions = user_permissions
        # cache data

        self.__applied_filter_at_prev_depth = False
        self.__applied_aggr_at_prev_depth = False
        self.__depth_cache_of_level_p_ids_node_parent_ids = defaultdict(set)
        self.__depth_cache_of_level_parent_id_child_node_ids = defaultdict(list)
        # self.step_parent_node_ids = set()  # use only for temp data

        self.__parent_path_must_condition = None

        # results
        self.__hierarchy_level_ids_result = list()
        self.__hierarchy_node_ids_node_result = dict()
        self.__result_level_ids_with_show_without_children_false = set()
        self.result_hierarchy_nodes = list()

        # aggregation result
        self.__result_aggregation_by_node_id = defaultdict(list)
        self.__prev_step_aggregation_by_parent_node_id = defaultdict(list)
        self.__aggregation_instance: Union[HierarchyAggregateByTMO, None] = None

        # inventory_results
        self.__inventory_results_by_node_id = defaultdict(list)
        self.__prev_step_inventory_results_by_parent_node_id = defaultdict(list)
        self.__tmo_id_of_level_to_return = None
        self.level_to_return_mo_data = None

    def __create_parent_condition_if_parent_exist(self):
        if self.parent_node_dto:
            parent_path = create_path_for_children_node_by_parent_node(
                parent_node=self.parent_node_dto
            )
            if parent_path:
                self.__parent_path_must_condition = [
                    {"exists": {"field": "path"}},
                    {
                        "wildcard": {
                            "path": {
                                "value": f"{parent_path}*",
                                "case_insensitive": False,
                            }
                        }
                    },
                ]

    @staticmethod
    def group_depth_level_conditions_by_parent_level_id(
        list_of_level_cond: list[LevelConditions],
    ) -> dict[str, list[LevelConditions]]:
        """Returns dict of parent_level_id as key and list of LevelConditions as value"""
        res = defaultdict(list)
        for level_cond in list_of_level_cond:
            res[level_cond.level.parent_id].append(level_cond)
        return res

    async def process(self) -> LevelConditionsOrderResults:
        self.__create_parent_condition_if_parent_exist()
        count_of_depths = len(self.level_cond_order)
        depth_counter = 1
        is_last_depth = False
        for depth, level_conditions in self.level_cond_order.items():
            # find last depth
            if depth_counter == count_of_depths:
                is_last_depth = True
            depth_counter += 1

            was_aggregation_on_depth = False

            filter_on_level = False

            for one_level_cond in level_conditions:
                print("one_level_cond")
                print(one_level_cond)
                level_p_id = one_level_cond.level.parent_id
                # get data from hierarchy obj index

                node_ids_node = await self.__process_hierarchy_obj_conditions(
                    one_level_cond=one_level_cond
                )
                print("hierarchy results ")
                print(len(node_ids_node))
                # get data from inventory
                # if on previous level was filter - no need to get all inventory objects

                if self.__applied_filter_at_prev_depth:
                    mo_ids = list()
                    if node_ids_node:
                        mo_ids_from_nodes = await self.__get_mo_ids_from_hierarchy_node_data_filter_conditions(
                            node_ids=list(node_ids_node)
                        )
                        mo_ids = (
                            await self.__process_inventory_filter_conditions(
                                one_level_cond=one_level_cond,
                                mo_ids=mo_ids_from_nodes,
                            )
                        )
                else:
                    mo_ids = await self.__process_inventory_filter_conditions(
                        one_level_cond=one_level_cond
                    )
                print("inventory results")
                print(len(mo_ids))
                node_ids_mo_ids = (
                    await self.__process_hierarchy_node_data_filter_conditions(
                        one_level_cond=one_level_cond,
                        mo_ids=list(mo_ids),
                        node_ids=list(node_ids_node),
                    )
                )
                print("inersection")
                print(len(node_ids_mo_ids))
                # aggregation - start
                # if prev step aggregation
                # combine by parent ids
                if self.__applied_aggr_at_prev_depth:
                    intersection_results = (
                        node_ids_mo_ids.keys()
                        & self.__prev_step_aggregation_by_parent_node_id.keys()
                    )
                    if is_last_depth:
                        for node_id in intersection_results:
                            aggr_mo_ids = self.__prev_step_aggregation_by_parent_node_id.get(
                                node_id
                            )
                            self.__result_aggregation_by_node_id[
                                node_id
                            ].extend(aggr_mo_ids)
                    else:
                        for node_id in intersection_results:
                            # get_parent_node
                            node = node_ids_node.get(node_id)
                            if node:
                                parent_id = node.get("parent_id")
                                aggr_mo_ids = self.__prev_step_aggregation_by_parent_node_id.get(
                                    node_id
                                )
                                if parent_id:
                                    self.__prev_step_aggregation_by_parent_node_id[
                                        parent_id
                                    ].extend(aggr_mo_ids)

                # if aggregation and last level
                if one_level_cond.aggregation:
                    self.__aggregation_instance = one_level_cond.aggregation

                    was_aggregation_on_depth = True
                    if is_last_depth:
                        self.__result_aggregation_by_node_id.update(
                            node_ids_mo_ids
                        )
                    else:
                        for node_id, mo_ids in node_ids_mo_ids.items():
                            node = node_ids_node.get(node_id)
                            if node:
                                parent_id = node.get("parent_id")
                                if parent_id:
                                    self.__prev_step_aggregation_by_parent_node_id[
                                        parent_id
                                    ].extend(mo_ids)

                # if one_level_cond cond aggregation - end
                # aggregation - end

                # if level_mo_data to return - start
                if self.__prev_step_inventory_results_by_parent_node_id:
                    intersection_results = (
                        node_ids_mo_ids.keys()
                        & self.__prev_step_inventory_results_by_parent_node_id.keys()
                    )
                    if is_last_depth:
                        for node_id in intersection_results:
                            aggr_mo_ids = self.__prev_step_inventory_results_by_parent_node_id.get(
                                node_id
                            )
                            self.__inventory_results_by_node_id[node_id].extend(
                                aggr_mo_ids
                            )
                    else:
                        for node_id in intersection_results:
                            # get_parent_node
                            node = node_ids_node.get(node_id)
                            if node:
                                parent_id = node.get("parent_id")
                                aggr_mo_ids = self.__prev_step_inventory_results_by_parent_node_id.get(
                                    node_id
                                )
                                if parent_id:
                                    self.__prev_step_inventory_results_by_parent_node_id[
                                        parent_id
                                    ].extend(aggr_mo_ids)

                # level_mo_data if level to return  and last level
                if one_level_cond.return_mo_ids_for_this_level:
                    self.__tmo_id_of_level_to_return = (
                        one_level_cond.level.object_type_id
                    )
                    self.level_to_return_mo_data = one_level_cond.level
                    if is_last_depth:
                        self.__inventory_results_by_node_id.update(
                            node_ids_mo_ids
                        )
                    else:
                        for node_id, mo_ids in node_ids_mo_ids.items():
                            node = node_ids_node.get(node_id)
                            if node:
                                parent_id = node.get("parent_id")
                                if parent_id:
                                    self.__prev_step_inventory_results_by_parent_node_id[
                                        parent_id
                                    ].extend(mo_ids)

                # if level_mo_data to return - end

                # hierarchy results - start
                if is_last_depth:
                    if one_level_cond.level.show_without_children is False:
                        self.__result_level_ids_with_show_without_children_false.add(
                            one_level_cond.level.id
                        )
                    self.__hierarchy_level_ids_result.append(
                        one_level_cond.level.id
                    )

                    intersection_results = (
                        node_ids_mo_ids.keys() & node_ids_node.keys()
                    )
                    intersection_results = {
                        node_id: node_ids_node[node_id]
                        for node_id in intersection_results
                    }
                    self.__hierarchy_node_ids_node_result.update(
                        intersection_results
                    )

                else:
                    filter_on_level = True
                    parent_node_ids = set()

                    for node_id in node_ids_mo_ids:
                        node = node_ids_node.get(node_id)
                        if node:
                            parent_id = node.get("parent_id")
                            if parent_id:
                                parent_node_ids.add(parent_id)

                    self.__depth_cache_of_level_p_ids_node_parent_ids[
                        level_p_id
                    ].update(parent_node_ids)
                    self.__depth_cache_of_level_parent_id_child_node_ids[
                        level_p_id
                    ].extend(list(node_ids_mo_ids))
                # hierarchy results - end

            if filter_on_level:
                self.__applied_filter_at_prev_depth = True

            if was_aggregation_on_depth:
                self.__applied_aggr_at_prev_depth = was_aggregation_on_depth

        # process results
        await self.__process_base_result_for_hierarchy_nodes()
        await self.__process_aggregation_results()
        # await self.__process_level_to_return_results()

        res = LevelConditionsOrderResults(
            nodes=self.result_hierarchy_nodes,
            level=self.level_to_return_mo_data,
            mo_ids=list(chain(*self.__inventory_results_by_node_id.values())),
        )
        print("search res")
        print(res)
        return res

    # CREATE QUERY BLOCK - Start
    def __create_search_query_for_list_level_conditions(
        self, list_level_conditions: list[LevelConditions]
    ):
        """Returns list of dicts ('should' conditions) based on list_level_conditions"""
        hierarchy_obj_should_conditions = list()
        if self.__applied_filter_at_prev_depth is False:
            for level_cond in list_level_conditions:
                # create hierarchy conditions
                must_cond = [{"match": {"level_id": level_cond.level.id}}]
                must_not_cond = []

                if self.__parent_path_must_condition:
                    must_cond.extend(self.__parent_path_must_condition)

                if level_cond.hierarchy_create_empty_nodes is False:
                    must_not_cond.extend([{"match": {"key_is_empty": True}}])

                if level_cond.level_show_without_children is False:
                    must_cond.extend(
                        [
                            {"exists": {"field": "child_count"}},
                            {"range": {"child_count": {"gt": 0}}},
                        ]
                    )

                # if level has parent_id all objects also must have p_id
                if level_cond.level.parent_id:
                    must_cond.append({"exists": {"field": "parent_id"}})

                hierarchy_should_cond_for_level = dict()
                if must_cond:
                    hierarchy_should_cond_for_level["must"] = must_cond
                if must_not_cond:
                    hierarchy_should_cond_for_level["must_not"] = must_not_cond

                if hierarchy_should_cond_for_level:
                    hierarchy_should_cond_for_level = {
                        "bool": hierarchy_should_cond_for_level
                    }
                    hierarchy_obj_should_conditions.append(
                        hierarchy_should_cond_for_level
                    )

        else:
            for level_cond in list_level_conditions:
                use_level_in_search = True
                # create hierarchy conditions
                must_cond = [{"match": {"level_id": level_cond.level.id}}]
                must_not_cond = []

                if self.__parent_path_must_condition:
                    must_cond.extend(self.__parent_path_must_condition)

                if level_cond.hierarchy_create_empty_nodes is False:
                    must_not_cond.extend([{"match": {"key_is_empty": True}}])

                if level_cond.level_show_without_children is False:
                    node_ids_from_prev_depth = (
                        self.__depth_cache_of_level_p_ids_node_parent_ids.get(
                            level_cond.level.id
                        )
                    )
                    if node_ids_from_prev_depth:
                        must_cond.extend(
                            [
                                {"exists": {"field": "child_count"}},
                                {"range": {"child_count": {"gt": 0}}},
                                {
                                    "terms": {
                                        "id": list(node_ids_from_prev_depth)
                                    }
                                },
                            ]
                        )
                    else:
                        use_level_in_search = False
                        # delete level from order
                if use_level_in_search:
                    hierarchy_should_cond_for_level = dict()
                    if must_cond:
                        hierarchy_should_cond_for_level["must"] = must_cond
                    if must_not_cond:
                        hierarchy_should_cond_for_level["must_not"] = (
                            must_not_cond
                        )

                    if hierarchy_should_cond_for_level:
                        hierarchy_should_cond_for_level = {
                            "bool": hierarchy_should_cond_for_level
                        }
                        hierarchy_obj_should_conditions.append(
                            hierarchy_should_cond_for_level
                        )
        return hierarchy_obj_should_conditions

    # CREATE QUERY BLOCK - End

    # PROCESS FILTER COND BLOCK - Start

    async def __process_inventory_filter_conditions(
        self,
        one_level_cond: LevelConditions,
        mo_ids: list[str | int] | None = None,
    ) -> set[int]:
        """Returns set of inventory mo ids that match search_body"""
        # get objects from inventory

        query_builder = LevelConditionsQueryBuilder(
            levels_conditions=[one_level_cond],
            elastic_client=self.elastic_client,
            parent_node_dto=self.parent_node_dto,
            user_permissions=self.user_permissions,
        )
        inventory_filter_should_conditions = (
            await query_builder.get_list_of_should_conditions()
        )

        size_per_step = 500_000
        bool_conditions = {
            "should": inventory_filter_should_conditions,
            "minimum_should_match": 1,
        }
        if mo_ids:
            bool_conditions["must"] = [{"terms": {"id": mo_ids}}]

        search_body = {
            "query": {"bool": bool_conditions},
            "sort": {"_doc": {"order": "asc"}},
            "track_total_hits": True,
            "size": size_per_step,
            "_source": {"includes": ["id"]},
        }

        step_mo_ids = set()
        index_name = get_index_name_by_tmo(one_level_cond.level.object_type_id)

        while True:
            search_res = await self.elastic_client.search(
                index=index_name, body=search_body, ignore_unavailable=True
            )

            total_hits = search_res["hits"]["total"]["value"]

            search_res = search_res["hits"]["hits"]
            if not step_mo_ids:
                step_mo_ids = {
                    item_id
                    for item in search_res
                    if (item_id := item["_source"].get("id"))
                }
            else:
                mo_ids = {
                    item_id
                    for item in search_res
                    if (item_id := item["_source"].get("id"))
                }
                step_mo_ids.update(mo_ids)

            if total_hits < size_per_step:
                break

            if len(search_res) == size_per_step:
                search_after = search_res[-1]["sort"]
                search_body["search_after"] = search_after
            else:
                break
        return step_mo_ids

    async def __process_hierarchy_node_data_filter_conditions(
        self,
        one_level_cond: LevelConditions,
        mo_ids: list[int],
        node_ids: list[str],
    ) -> dict[str, list[str]]:
        """Returns a dictionary with node_ids as keys and lists of mo_ids as values that match search_body"""
        if not mo_ids or not node_ids:
            return dict()

        node_data_must_conds = [
            {"match": {"level_id": one_level_cond.level.id}}
        ]
        if mo_ids:
            mo_ids_must_cond = [{"terms": {"mo_id": mo_ids}}]
            node_data_must_conds = node_data_must_conds + mo_ids_must_cond

        if node_ids:
            node_ids_must_cond = [{"terms": {"node_id": list(node_ids)}}]
            node_data_must_conds = node_data_must_conds + node_ids_must_cond

        size_per_step = 500_000

        search_body = {
            "query": {"bool": {"must": node_data_must_conds}},
            "sort": {"_doc": {"order": "asc"}},
            "track_total_hits": True,
            "size": size_per_step,
            "_source": {"includes": ["node_id", "mo_id"]},
        }

        node_ids_mo_ids = defaultdict(list)
        if search_body:
            while True:
                search_res = await self.elastic_client.search(
                    index=HIERARCHY_NODE_DATA_INDEX,
                    body=search_body,
                    ignore_unavailable=True,
                )

                total_hits = search_res["hits"]["total"]["value"]

                search_res = search_res["hits"]["hits"]

                for item in search_res:
                    item = item["_source"]
                    node_ids_mo_ids[item["node_id"]].append(item["mo_id"])

                if total_hits < size_per_step:
                    break

                if len(search_res) == size_per_step:
                    search_after = search_res[-1]["sort"]
                    search_body["search_after"] = search_after
                else:
                    break

        return node_ids_mo_ids

    async def __get_mo_ids_from_hierarchy_node_data_filter_conditions(
        self, node_ids: list[str]
    ) -> list[str]:
        """Returns a list with mo_ids"""

        size_per_step = 500_000

        search_body = {
            "query": {"bool": {"must": {"terms": {"node_id": node_ids}}}},
            "sort": {"_doc": {"order": "asc"}},
            "track_total_hits": True,
            "size": size_per_step,
            "_source": {"includes": ["node_id", "mo_id"]},
        }

        mo_ids = list()
        if search_body:
            while True:
                search_res = await self.elastic_client.search(
                    index=HIERARCHY_NODE_DATA_INDEX,
                    body=search_body,
                    ignore_unavailable=True,
                )

                total_hits = search_res["hits"]["total"]["value"]

                search_res = search_res["hits"]["hits"]

                if mo_ids:
                    mo_ids.extend(
                        [item["_source"]["mo_id"] for item in search_res]
                    )
                else:
                    mo_ids = [item["_source"]["mo_id"] for item in search_res]

                if total_hits < size_per_step:
                    break

                if len(search_res) == size_per_step:
                    search_after = search_res[-1]["sort"]
                    search_body["search_after"] = search_after
                else:
                    break

        return mo_ids

    async def __process_hierarchy_obj_conditions(
        self, one_level_cond: LevelConditions
    ) -> dict:
        """Returns a dictionary with node_ids as keys and node info as values, that match search body"""

        # get data from hierarchy obj index - start

        hierarchy_obj_should_conditions = (
            self.__create_search_query_for_list_level_conditions(
                list_level_conditions=[one_level_cond]
            )
        )

        node_id_node = dict()

        if hierarchy_obj_should_conditions:
            bool_cond = {
                "should": hierarchy_obj_should_conditions,
                "minimum_should_match": 1,
            }

            size_per_step = 500_000

            search_body = {
                "query": {"bool": bool_cond},
                "sort": {"_doc": {"order": "asc"}},
                "track_total_hits": True,
                "size": size_per_step,
                # '_source': {'includes': ['id', 'parent_id', 'level_id']}
            }

            if search_body:
                while True:
                    search_res = await self.elastic_client.search(
                        index=HIERARCHY_OBJ_INDEX,
                        body=search_body,
                        ignore_unavailable=True,
                    )

                    total_hits = search_res["hits"]["total"]["value"]

                    search_res = search_res["hits"]["hits"]

                    if not node_id_node:
                        node_id_node = {
                            item["_source"]["id"]: item["_source"]
                            for item in search_res
                        }
                    else:
                        node_id_node.update(
                            {
                                item["_source"]["id"]: item["_source"]
                                for item in search_res
                            }
                        )

                    if total_hits < size_per_step:
                        break

                    if len(search_res) == size_per_step:
                        search_after = search_res[-1]["sort"]
                        search_body["search_after"] = search_after
                    else:
                        break
        return node_id_node

    # PROCESS FILTER COND BLOCK - END
    # PROCESS RESULTS BLOCK - START

    async def __process_base_result_for_hierarchy_nodes(self):
        """Fills in the self.result_hierarchy_nodes attribute nodes tha match all filters"""

        if self.__applied_filter_at_prev_depth:
            # get child_nodes
            child_node_ids = list()
            for level_id in self.__hierarchy_level_ids_result:
                level_child_node_ids = (
                    self.__depth_cache_of_level_parent_id_child_node_ids.get(
                        level_id
                    )
                )
                if level_child_node_ids:
                    child_node_ids.extend(level_child_node_ids)

            if child_node_ids:
                parent_ids = list(self.__hierarchy_node_ids_node_result)

                if len(parent_ids) > 0:
                    must_cond = [
                        {"terms": {"parent_id": parent_ids}},
                        {"terms": {"id": child_node_ids}},
                    ]
                    aggregation = {
                        "child_count": {
                            "terms": {
                                "field": "parent_id",
                                "size": len(parent_ids),
                            }
                        }
                    }

                    search_body = {
                        "query": {"bool": {"must": must_cond}},
                        "size": 0,
                        "aggs": aggregation,
                    }

                    aggr_res = await self.elastic_client.search(
                        body=search_body,
                        index=HIERARCHY_OBJ_INDEX,
                        ignore_unavailable=True,
                    )
                    aggr_res = aggr_res["aggregations"]["child_count"][
                        "buckets"
                    ]
                    aggr_res = {
                        item["key"]: item["doc_count"] for item in aggr_res
                    }
                else:
                    aggr_res = {}

                for (
                    node_id,
                    node,
                ) in self.__hierarchy_node_ids_node_result.items():
                    level_id = node["level_id"]
                    node_child_count = aggr_res.get(node_id, 0)
                    if (
                        level_id
                        in self.__result_level_ids_with_show_without_children_false
                        and node_child_count == 0
                    ):
                        continue
                    node["child_count"] = node_child_count
                    self.result_hierarchy_nodes.append(node)

            else:
                # all 0
                for (
                    node_id,
                    node,
                ) in self.__hierarchy_node_ids_node_result.items():
                    level_id = node["level_id"]
                    if (
                        level_id
                        in self.__result_level_ids_with_show_without_children_false
                    ):
                        continue
                    node["child_count"] = 0
                    self.result_hierarchy_nodes.append(node)
        else:
            # return nodes _from result
            self.result_hierarchy_nodes = [
                v for _, v in self.__hierarchy_node_ids_node_result.items()
            ]

    async def __process_aggregation_results(self):
        """Adds aggregation data into the self.result_hierarchy_nodes nodes"""
        if self.__result_aggregation_by_node_id:
            # create aggregation request
            aggr_filters = {
                k: {"terms": {"id": v}}
                for k, v in self.__result_aggregation_by_node_id.items()
            }
            tmo_id = self.__aggregation_instance.tmo_id
            tprm_id = self.__aggregation_instance.tprm_id
            aggr_type = self.__aggregation_instance.aggregation_type
            parameter_name = f"{INVENTORY_PARAMETERS_FIELD_NAME}.{tprm_id}"
            search_body = {
                "size": 0,
                "aggs": {
                    "f": {
                        "filters": {"filters": aggr_filters},
                        "aggs": {
                            "tprm_aggr": {aggr_type: {"field": parameter_name}}
                        },
                    }
                },
            }

            index_name = get_index_name_by_tmo(tmo_id)
            agg_res = await self.elastic_client.search(
                index=index_name, body=search_body, ignore_unavailable=True
            )
            agg_res = agg_res["aggregations"]["f"]["buckets"]

            # add aggregate_res to results
            for node in self.result_hierarchy_nodes:
                node_id = node["id"]
                agg_res_for_node_id = agg_res.get(node_id)
                if agg_res_for_node_id:
                    node["aggregation_value"] = agg_res_for_node_id[
                        "tprm_aggr"
                    ]["value"]
                    node["aggregation_doc_count"] = agg_res_for_node_id[
                        "doc_count"
                    ]
                else:
                    node["aggregation_value"] = None
                    node["aggregation_doc_count"] = 0

    # PROCESS RESULTS BLOCK - END


class LevelConditionsOrderHandlerWithoutFilters(LevelConditionsOrderHandler):
    _enable_timing = True

    def __init__(
        self,
        limit: int = 9_999,
        offset: int = 0,
        return_results: bool = False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._es_size_query = 10_000
        self.size_per_step = limit or self._es_size_query
        self.offset = offset
        self.return_inventory_results = return_results

    def __create_parent_condition_if_parent_exist(self):
        if self.parent_node_dto:
            parent_path = create_path_for_children_node_by_parent_node(
                parent_node=self.parent_node_dto
            )
            if parent_path:
                self.__parent_path_must_condition = [
                    {"exists": {"field": "path"}},
                    {
                        "wildcard": {
                            "path": {
                                "value": f"{parent_path}*",
                                "case_insensitive": False,
                            }
                        }
                    },
                ]

    @async_timing_decorator(enable=True)
    async def process_without_filters(
        self,
    ) -> tuple[HierarchyFilterHierarchyItem, HierarchyFilterInventoryItem]:
        self.__create_parent_condition_if_parent_exist()
        hierarchy_res = await self.__get_hierarchy_information()
        hierarchy_result = HierarchyFilterHierarchyItem.model_validate(
            hierarchy_res
        )
        mo_ids = []
        # Check virtual levels. If level is virtual we shouldn't set data to inventory
        if not self.parent_node_dto:
            current_cond_order = 1
        else:
            current_cond_order = self.parent_node_dto.level + 1
        if not self.level_cond_order[current_cond_order][0].level.is_virtual:
            mo_ids = [int(el["object_id"]) for el in hierarchy_res["objects"]]

        if self.return_inventory_results and mo_ids:
            inventory_res = await self.__get_inventory_information(
                mo_ids=mo_ids,
                tmo_id=int(hierarchy_res["objects"][0]["object_type_id"]),
            )
            inventory_result = HierarchyFilterInventoryItem.model_validate(
                inventory_res
            )
        else:
            inventory_result = HierarchyFilterInventoryItem()
        return hierarchy_result, inventory_result

    async def __get_hierarchy_information(self) -> dict:
        hierarchies: list[dict] = list()
        if self.parent_node_dto:
            bool_condition = {
                "must": {"term": {"parent_id": self.parent_node_dto.id}}
            }
        else:
            # We get 1 level for given hierarchy id
            bool_condition = {
                "must": [
                    {
                        "term": {
                            "hierarchy_id": self.level_cond_order[1][
                                0
                            ].level.hierarchy_id
                        }
                    },
                    {"term": {"level": 1}},
                ]
            }
        search_body = {
            "query": {"bool": bool_condition},
            "sort": {"_doc": {"order": "asc"}},
            "track_total_hits": True,
            "size": self.size_per_step,
            "from_": self.offset,
        }
        if self.size_per_step + self.offset < self._es_size_query:
            search_res = await self.elastic_client.search(
                index=HIERARCHY_OBJ_INDEX,
                body=search_body,
                ignore_unavailable=True,
            )
            total_hits = search_res.get("hits").get("total").get("value")
            search_res = search_res.get("hits").get("hits")
            hierarchies = [
                HierarchyModel.model_validate(
                    item.get("_source", None)
                ).model_dump(by_alias=True)
                for item in search_res
            ]
        else:
            search_body.update({"index": HIERARCHY_OBJ_INDEX})
            search_body.update({"ignore_unavailable": True})
            search_body.update({"track_total_hits": True})
            total_hits = 0
            async for item in search_after_generator_with_total(
                self.elastic_client, body=search_body
            ):
                hierarchies.append(
                    HierarchyModel.model_validate(
                        item.get("data", None)
                    ).model_dump(by_alias=True)
                )
                total_hits = item["hits"]
        return {"objects": hierarchies, "total_hits": total_hits}

    async def __get_inventory_information(
        self, mo_ids: list[int], tmo_id: int
    ) -> dict:
        inventory_data: list[dict] = list()
        index_name = get_index_name_by_tmo(tmo_id)

        bool_condition = {"must": {"terms": {"id": mo_ids}}}

        search_body = {
            "query": {"bool": bool_condition},
            "sort": {"_doc": {"order": "asc"}},
            "track_total_hits": True,
            "size": self.size_per_step,
        }
        if len(mo_ids) < self._es_size_query:
            search_res = await self.elastic_client.search(
                index=index_name,
                body=search_body,
                ignore_unavailable=True,
            )
            total_hits = search_res.get("hits").get("total").get("value")
            search_res = search_res.get("hits").get("hits")
            inventory_data = [
                InventoryModel.model_validate(
                    item.get("_source", None)
                ).model_dump(exclude_unset=True)
                for item in search_res
            ]
        else:
            search_body.update({"index": index_name})
            search_body.update({"ignore_unavailable": True})
            search_body.update({"track_total_hits": True})
            total_hits = 0
            async for item in search_after_generator_with_total(
                self.elastic_client, body=search_body
            ):
                inventory_data.append(
                    InventoryModel.model_validate(
                        item.get("data", None)
                    ).model_dump(by_alias=True)
                )
                total_hits = item.get("hits", 0)
        return {"objects": inventory_data, "total_hits": total_hits}
