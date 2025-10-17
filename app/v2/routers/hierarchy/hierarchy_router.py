import sys
import traceback
import uuid

from collections import defaultdict
from typing import Annotated, List, Literal, Union

from elasticsearch import AsyncElasticsearch, BadRequestError
from fastapi import APIRouter, Body, Depends, HTTPException, Query

from common_utils.dto_models.models import (
    AllResAsListQueryModel,
    HierarchyAggregateByTMO,
)
from common_utils.features.utils import (
    get_count_and_all_items_as_list_from_special_index,
)
from elastic.client import get_async_client
from elastic.config import INVENTORY_TMO_INDEX_V2, INVENTORY_TPRM_INDEX_V2
from elastic.pydantic_models import (
    SortColumn,
    HierarchyFilter,
    InventoryResAdditionalConditions,
)
from elastic.query_builder_service.inventory_index.mo_object.utils import (
    get_dict_of_inventory_attr_and_params_types,
    get_sort_query_for_inventory_obj_index,
    get_search_query_for_inventory_obj_index,
)
from elastic.query_builder_service.inventory_index.utils.index_utils import (
    get_index_name_by_tmo,
)
from indexes_mapping.inventory.mapping import INVENTORY_PERMISSIONS_FIELD_NAME
from security.security_data_models import UserData, UserPermissionBuilder
from security.security_factory import security
from services.hierarchy_services.elastic.configs import (
    HIERARCHY_HIERARCHIES_INDEX,
    HIERARCHY_LEVELS_INDEX,
    HIERARCHY_OBJ_INDEX,
)
from services.hierarchy_services.elastic.mapping import (
    HIERARCHY_PERMISSIONS_FIELD_NAME,
)
from services.hierarchy_services.features.dependet_level_factory import (
    DependentLevelsChainFactory,
    LevelDataForChain,
)
from services.hierarchy_services.models.dto import (
    LITERAL_HIERARCHY_SORT_PARAMETERS,
    HierarchyDTO,
    NodeDTO,
)
from services.hierarchy_services.models.response import (
    CountAndListOfHierarchiesResponse,
)
from services.inventory_services.utils.security.filter_by_realm import (
    check_permission_is_admin,
    get_permissions_from_client_role,
    raise_forbidden_ex_if_user_has_no_permission,
    get_only_available_to_read_tmo_ids_for_special_client,
)
from utils_by_services.hierarchy.level_cond_query_handler import (
    LevelConditionsOrderHandler,
    LevelConditionsOrderHandlerWithoutFilters,
)
from utils_by_services.hierarchy.level_condition_grouper import (
    InventoryLevelConditionGrouper,
)
from utils_by_services.hierarchy.node_checker import NodeCheckerWithFilters
from utils_by_services.hierarchy.result_aggregation_handler import (
    AggregationsByRangesHandler,
)
from utils_by_services.inventory.tprm_features import (
    get_list_of_unavailable_parameters,
)
from v2.routers.hierarchy.configs import HIERARCHY_ROUTER_PREFIX
from v2.routers.hierarchy.utils.checkers import (
    get_hierarchy_with_permission_check_or_raise_error,
    get_node_or_raise_error,
)
from v2.routers.hierarchy.utils.response_models import (
    HierarchyFilterResults,
    HierarchyFilterHierarchyItem,
    HierarchyFilterInventoryItem,
)
from v2.routers.severity.utils import get_group_inventory_must_not_conditions
from v2.tasks.hierarchy.filter_hierarchies_current_level import (
    FilterHierarchiesCurrentLevel,
)
from v2.tasks.hierarchy.find_mo_id_in_hierarchies import FindMoIdsInHierarchies

router = APIRouter(
    prefix=HIERARCHY_ROUTER_PREFIX, tags=["Hierarchy indexes: Hierarchy"]
)


@router.get(
    "/", status_code=200, response_model=CountAndListOfHierarchiesResponse
)
async def get_hierarchies(
    with_lifecycle: bool = False,
    elastic_client: AsyncElasticsearch = Depends(get_async_client),
    user_data: UserData = Depends(security),
    sort_by_field: Union[LITERAL_HIERARCHY_SORT_PARAMETERS, None] = "id",
    sort_direction: Union[None, Literal["asc", "desc"]] = "asc",
    limit: Annotated[int | None, Query(ge=0, le=2000000)] = None,
    offset: Annotated[int | None, Query(ge=0)] = 0,
):
    """
    Returns list of Hierarchies
    """
    is_admin = check_permission_is_admin(client_role=user_data.realm_access)
    user_permissions = get_permissions_from_client_role(
        client_role=user_data.realm_access
    )
    if not is_admin:
        raise_forbidden_ex_if_user_has_no_permission(
            client_permissions=user_permissions
        )

    search_conditions = []

    if not is_admin:
        # TODO : remove comment to enable permissions
        # search_conditions.append({"terms": {HIERARCHY_PERMISSIONS_FIELD_NAME: user_permissions}})
        pass

    if with_lifecycle:
        # get all tmo with lifecycle
        sort_cond = {"id": {"order": "asc"}}

        search_query = {
            "bool": {
                "must": [{"exists": {"field": "lifecycle_process_definition"}}]
            }
        }
        query_model = AllResAsListQueryModel(
            query_condition={"query": search_query},
            sort_cond={"sort": sort_cond},
            source_conditions={"_source": {"includes": "id"}},
        )

        all_tmo_with_lifecycle = (
            await get_count_and_all_items_as_list_from_special_index(
                index=INVENTORY_TMO_INDEX_V2,
                query_model=query_model,
                async_client=elastic_client,
            )
        )

        if all_tmo_with_lifecycle.count == 0:
            return all_tmo_with_lifecycle

        # get levels with this tmo
        tmo_ids = [tmo["id"] for tmo in all_tmo_with_lifecycle.items]

        sort_cond = {"id": {"order": "asc"}}

        search_query = {
            "bool": {"must": [{"terms": {"object_type_id": tmo_ids}}]}
        }
        query_model = AllResAsListQueryModel(
            query_condition={"query": search_query},
            sort_cond={"sort": sort_cond},
            source_conditions={"_source": {"includes": "hierarchy_id"}},
        )
        all_level_with_lifecycle = (
            await get_count_and_all_items_as_list_from_special_index(
                index=HIERARCHY_LEVELS_INDEX,
                query_model=query_model,
                async_client=elastic_client,
            )
        )
        if all_level_with_lifecycle.count == 0:
            return all_level_with_lifecycle

        hierarchy_ids = list(
            {level["hierarchy_id"] for level in all_level_with_lifecycle.items}
        )

        search_conditions.append({"terms": {"id": hierarchy_ids}})

    if not search_conditions:
        search_conditions.append({"match_all": {}})

    sort_cond = {sort_by_field: {"order": sort_direction}}

    search_query = {"bool": {"must": search_conditions}}

    query_model = AllResAsListQueryModel(
        query_condition={"query": search_query},
        sort_cond={"sort": sort_cond},
        source_conditions={
            "_source": {"excludes": [HIERARCHY_PERMISSIONS_FIELD_NAME]}
        },
    )

    if offset:
        query_model.offset = offset

    if limit:
        query_model.limit = limit
    res = await get_count_and_all_items_as_list_from_special_index(
        index=HIERARCHY_HIERARCHIES_INDEX,
        query_model=query_model,
        async_client=elastic_client,
    )
    return res


@router.get("/{hierarchy_id}", response_model=HierarchyDTO)
async def get_hierarchy(
    hierarchy_id: int,
    elastic_client: AsyncElasticsearch = Depends(get_async_client),
    user_data: UserData = Depends(security),
):
    hierarchy = await get_hierarchy_with_permission_check_or_raise_error(
        hierarchy_id=hierarchy_id,
        user_data=user_data,
        elastic_client=elastic_client,
    )
    return hierarchy


@router.post(
    "/get_hierarchy_and_inventory_results_by_filter",
    response_model=HierarchyFilterResults,
    description="""
    <b>hierarchy_id</b>: id of existing hierarchy <br/>
    <b>filters</b>: filters for inventory objects with a specific tmo id <br/>
    (there should be levels in this hierarchy based on this tmo id) <br/>
    <b>parent_node_id</b>: id of parent node <br/>
    <b>aggregation</b>: aggregation conditions for inventory objects, will be grouped by nodes <br/>
    (can be used for parameter types of current level tmo_id and any bottom level tmo_id) <br/>
    <b>inventory_res</b>: <br/>
    &nbsp;&nbsp;&nbsp;&nbsp;- <b>return_results</b> - return or not inventory results<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;- <b>sort_by</b> - sorts inventory results<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;- <b>with_groups</b> - returns results with groups and not<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;- <b>aggregation_by_ranges</b> - returns aggregation by ranges based on inventory results
     grouped by range aggr_name
    """,
)
async def get_hierarchy_and_inventory_results_by_filter(
    hierarchy_id: int = Body(),
    filters: List[HierarchyFilter] = Body(None),
    elastic_client: AsyncElasticsearch = Depends(get_async_client),
    user_data: UserData = Depends(security),
    parent_node_id: str = Body(None),
    aggregation: HierarchyAggregateByTMO = Body(None),
    inventory_res: InventoryResAdditionalConditions = Body(None),
):
    """
    :param hierarchy_id: id of existing hierarchy
    :param filters: filters for inventory objects with a specific tmo id
    (there should be levels in this hierarchy based on this tmo id)
    :param parent_node_id: id of parent node
    :param aggregation: aggregation conditions for inventory objects, will be grouped by nodes
    (can be used for parameter types of current level tmo_id and any bottom level tmo_id)
    :param inventory_res:
    - return_results - return or not inventory results<br/>
    - sort_by - sorts inventory results<br/>
    - with_groups - returns results with groups and not<br/>
    - aggregation_by_ranges - returns aggregation by ranges based on inventory results grouped by range aggr_name
    """
    # Default response
    res_for_hierarchy = HierarchyFilterHierarchyItem()
    res_for_inventory = HierarchyFilterInventoryItem()
    default_resp = HierarchyFilterResults(
        hierarchy_results=res_for_hierarchy, inventory_results=res_for_inventory
    )

    user_permissions = UserPermissionBuilder(user_data=user_data)
    user_permissions = user_permissions.get_user_permissions()
    # check permissions for filters aggregation, sorting - start
    if user_permissions.is_admin is False:
        all_tprms = set()
        all_tmos = set()
        if filters:
            for h_filter in filters:
                if not h_filter.filter_columns:
                    continue
                f_tprms = {
                    int(f_c.column_name)
                    for f_c in h_filter.filter_columns
                    if f_c.column_name.isdigit()
                }
                all_tprms.update(f_tprms)
                all_tmos.add(h_filter.tmo_id)
        if aggregation:
            all_tprms.add(aggregation.tprm_id)
            all_tmos.add(aggregation.tmo_id)
        if inventory_res:
            # check sorting for inventory
            if inventory_res.sort_by:
                sort_by_tprms = {
                    int(s_c.column_name)
                    for s_c in inventory_res.sort_by
                    if s_c.column_name.isdigit()
                }
                all_tprms.update(sort_by_tprms)

            # check ranges for inventory
            if inventory_res.aggregation_by_ranges:
                for (
                    aggr_items
                ) in inventory_res.aggregation_by_ranges.aggr_items:
                    step_tprm_ids = {
                        int(f_c.column_name)
                        for f_c in aggr_items.aggr_filters
                        if f_c.column_name.isdigit()
                    }
                    all_tprms.update(step_tprm_ids)

        if all_tprms:
            search_body = {
                "query": {
                    "bool": {
                        "must": [
                            {"terms": {"id": list(all_tprms)}},
                            {
                                "terms": {
                                    INVENTORY_PERMISSIONS_FIELD_NAME: user_permissions.user_permissions
                                }
                            },
                        ]
                    }
                },
                "size": len(all_tprms),
                "_source": {"includes": "id"},
            }

            search_res = await elastic_client.search(
                index=INVENTORY_TPRM_INDEX_V2,
                body=search_body,
                ignore_unavailable=True,
            )

            search_res = search_res["hits"]["hits"]
            available_tprms = {item["_source"]["id"] for item in search_res}

            difference = all_tprms.difference(available_tprms)
            if difference:
                raise HTTPException(
                    status_code=403,
                    detail=f"User has no permissions for tprms: {difference}",
                )

        if all_tmos:
            available_tmos = (
                await get_only_available_to_read_tmo_ids_for_special_client(
                    client_permissions=user_permissions.user_permissions,
                    elastic_client=elastic_client,
                    tmo_ids=list(all_tmos),
                )
            )
            difference = set(all_tmos).difference(available_tmos)
            if difference:
                raise HTTPException(
                    status_code=403,
                    detail=f"User has no permissions for tmos: {difference}",
                )

    # check permissions for filters aggregation, sorting - end

    # check parent_id
    if not parent_node_id:
        parent_node_id = None
    else:
        if parent_node_id.upper() in ("ROOT", "NONE", "NULL"):
            parent_node_id = None
        else:
            try:
                parent_node_id = uuid.UUID(parent_node_id)
            except ValueError:
                raise HTTPException(
                    status_code=422,
                    detail="parent_node_id must be instance of"
                    " UUID or be one of values: root, none, null",
                )

    # check node_id
    if isinstance(parent_node_id, uuid.UUID):
        parent_node = await get_node_or_raise_error(
            node_id=parent_node_id, elastic_client=elastic_client
        )
        node_hierarchy_id_as_int = int(parent_node["hierarchy_id"])
        if node_hierarchy_id_as_int != hierarchy_id:
            raise HTTPException(
                status_code=422,
                detail=f"Parent node hierarchy id does not match hierarchy_id "
                f"({node_hierarchy_id_as_int} != {hierarchy_id})",
            )
        is_active = parent_node.get("active")
        if not is_active:
            raise HTTPException(
                status_code=422, detail="Parent node is not active"
            )

    else:
        parent_node = None
    # check hierarchy
    hierarchy = await get_hierarchy_with_permission_check_or_raise_error(
        hierarchy_id=hierarchy_id,
        user_data=user_data,
        elastic_client=elastic_client,
    )

    hierarchy_dto = HierarchyDTO.model_validate(hierarchy)

    if parent_node is None:
        parent_node_dto = None
        parent_level_data = None

    else:
        parent_node_dto = NodeDTO.model_validate(parent_node)
        parent_level_data = LevelDataForChain(
            level_id=parent_node_dto.level_id,
            level_level=int(parent_node_dto.level),
        )

    factory_dep_level = DependentLevelsChainFactory(
        elastic_client, hierarchy_id, parent_level_data=parent_level_data
    )

    chain_of_p_levels = (
        await factory_dep_level.create_dependent_levels_chain_by()
    )
    print(f"{chain_of_p_levels=}")
    if parent_node:
        checker = NodeCheckerWithFilters(
            node=parent_node_dto,
            filters=filters,
            hierarchy=hierarchy_dto,
            elastic_client=elastic_client,
            dependent_levels_chain=chain_of_p_levels,
            user_permissions=user_permissions,
        )
        check_res = await checker.check()

        if (
            check_res.full_match_filter_condition is False
            and not check_res.mo_ids_matched_conditions
        ):
            return default_resp

    order_builder = InventoryLevelConditionGrouper(
        depends_level_chain=chain_of_p_levels,
        hierarchy=hierarchy_dto,
        hierarchy_filters=filters,
        aggregation=aggregation,
    )

    order_to_process = order_builder.get_order_to_process()
    print(f"{order_to_process=}")
    if not order_to_process:
        return default_resp

    handler = LevelConditionsOrderHandler(
        level_cond_order=order_to_process,
        elastic_client=elastic_client,
        parent_node_dto=parent_node_dto,
        user_permissions=user_permissions,
    )

    # await handler.process()
    handler_res = await handler.process()
    print(f"{handler_res=}")
    # inventory_results = handler.level_to_return_mo_data

    default_resp.hierarchy_results.total_hits = len(handler_res.nodes)
    default_resp.hierarchy_results.objects = handler_res.nodes
    # res = {"hierarchy_results": hierarchy_results,
    #        "inventory_results": list()}

    if (
        not inventory_res
        or inventory_res.return_results is False
        or not handler_res.mo_ids
    ):
        return default_resp

    # add additional conditions for inventory results
    inv_must_cond = [{"terms": {"id": handler_res.mo_ids}}]
    inv_must_not = list()

    # get with_groups query condition
    group_condition_must_not = get_group_inventory_must_not_conditions(
        with_groups=inventory_res.with_groups
    )
    inv_must_not.extend(group_condition_must_not)

    # sort query start

    sort_query = []

    if inventory_res.sort_by:
        column_names = [
            sort_item.column_name for sort_item in inventory_res.sort_by
        ]
        dict_of_types = await get_dict_of_inventory_attr_and_params_types(
            array_of_attr_and_params_names=column_names,
            elastic_client=elastic_client,
        )
        sort_query = get_sort_query_for_inventory_obj_index(
            sort_columns=inventory_res.sort_by, dict_of_types=dict_of_types
        )
    conditions = dict()
    if inv_must_cond:
        conditions["must"] = inv_must_cond
    if inv_must_not:
        conditions["must_not"] = inv_must_not

    # TODO optimize
    if inventory_res.should_filter_conditions:
        column_names_post_filter = set()
        for l_filters in inventory_res.should_filter_conditions:
            for filter_item in l_filters:
                column_names_post_filter.add(filter_item.column_name)

        dict_of_types = await get_dict_of_inventory_attr_and_params_types(
            array_of_attr_and_params_names=column_names_post_filter,
            elastic_client=elastic_client,
        )

        should_conditions = []

        for filter_list in inventory_res.should_filter_conditions:
            s_f_cond = get_search_query_for_inventory_obj_index(
                filter_list, dict_of_types
            )
            should_conditions.append(s_f_cond)

        if should_conditions:
            conditions["should"] = should_conditions
            conditions["minimum_should_match"] = 1

    main_query = {"bool": conditions}

    # get unavailable tprms for user
    unavailable_parameters = list()
    if user_permissions.is_admin is False:
        unavailable_parameters = await get_list_of_unavailable_parameters(
            elastic_client=elastic_client,
            user_permissions=user_permissions,
            tmo_id=handler_res.level.object_type_id,
        )

    excludes_fields = [INVENTORY_PERMISSIONS_FIELD_NAME]
    if unavailable_parameters:
        excludes_fields.extend(unavailable_parameters)

    body = {
        "query": main_query,
        "_source": {"excludes": excludes_fields},
        "track_total_hits": True,
    }

    if sort_query:
        body["sort"] = sort_query

    if inventory_res.limit:
        body["size"] = inventory_res.limit

    if inventory_res.offset is not None:
        body["from"] = inventory_res.offset
    print(f"{body=}")
    search_index = get_index_name_by_tmo(handler_res.level.object_type_id)
    try:
        search_res = await elastic_client.search(
            index=search_index, body=body, ignore_unavailable=True
        )
    except BadRequestError:
        print(traceback.format_exc(), file=sys.stderr)
        raise HTTPException(
            status_code=400, detail="Bad Request. Check filter parameters"
        )
    total_hits = search_res["hits"]["total"]["value"]
    objects = [item["_source"] for item in search_res["hits"]["hits"]]

    default_resp.inventory_results.total_hits = total_hits
    default_resp.inventory_results.objects = objects
    print(f"{default_resp=}")
    if inventory_res.aggregation_by_ranges:
        aggr_res_handler = AggregationsByRangesHandler(
            tmo_id=handler_res.level.object_type_id,
            mo_ids=handler_res.mo_ids,
            aggregation=inventory_res.aggregation_by_ranges,
            elastic_client=elastic_client,
        )
        default_resp.aggregation_by_ranges = (
            await aggr_res_handler.get_results()
        )

    return default_resp


@router.post(
    "/{hierarchy_id}/parent/{parent_id}/with_conditions", response_model=None
)
async def get_child_nodes_of_parent_id_with_filter_condition(
    hierarchy_id: int,
    parent_id: str,
    hierarchy_filter: str = None,
    # hierarchy_filter: List[HierarchyFilter] | None = None,
    nodes_sort_by: list[SortColumn] = None,
    elastic_client: AsyncElasticsearch = Depends(get_async_client),
    limit: int = Body(2000000, ge=0, le=2000000),
    offset: int = Body(0, ge=0),
    search_children_node_by_key: str = Body(None),
    user_data: UserData = Depends(security),
    aggregation_tprm: int = Body(None),
    aggregation_type: str = None,
):
    default_resp = {}
    # TODO only active nodes
    # check user permission
    hierarchy = await get_hierarchy_with_permission_check_or_raise_error(
        hierarchy_id=hierarchy_id,
        user_data=user_data,
        elastic_client=elastic_client,
    )

    # check parent_id
    if parent_id.upper() in ("ROOT", "NONE", "NULL"):
        parent_id = None
    else:
        try:
            parent_id = uuid.UUID(parent_id)
        except ValueError:
            raise HTTPException(
                status_code=422,
                detail="parent_id must be instance of UUID or be one of values: root, none, null",
            )

    # check node_id
    if isinstance(parent_id, uuid.UUID):
        parent_node = await get_node_or_raise_error(
            node_id=parent_id, elastic_client=elastic_client
        )
        node_hierarchy_id_as_int = int(parent_node["hierarchy_id"])
        if node_hierarchy_id_as_int != hierarchy_id:
            raise HTTPException(
                status_code=422,
                detail=f"Parent node hierarchy id does not match hierarchy_id "
                f"({node_hierarchy_id_as_int} != {hierarchy_id})",
            )
        is_active = parent_node.get("active")
        if not is_active:
            raise HTTPException(
                status_code=422, detail="Parent node is not active"
            )

    else:
        parent_node = None

    # get levels for node
    level_must_condition = list()
    level_must_not_condition = list()

    if parent_node:
        level_must_condition.append(
            {"match": {"parent_id": parent_node["level_id"]}}
        )

    else:
        level_must_condition.append({"match": {"hierarchy_id": hierarchy_id}})
        level_must_not_condition.append({"exists": {"field": "parent_id"}})

    query = {"bool": {"must": level_must_condition}}
    if level_must_not_condition:
        query["bool"]["must_not"] = level_must_not_condition

    search_body = {
        "query": query,
        "size": 10000,
    }
    levels = await elastic_client.search(
        index=HIERARCHY_LEVELS_INDEX, body=search_body, ignore_unavailable=True
    )

    levels = [level["_source"] for level in levels["hits"]["hits"]]

    if not levels:
        return default_resp

    level_ids = [level["id"] for level in levels]

    # если нет фильтров проверяем вывод
    if not hierarchy_filter:
        should_query_list = list()

        must_conditions = [
            {"match": {"hierarchy_id": hierarchy_id}},
            {"match": {"active": True}},
        ]
        must_not_conditions = []

        if parent_node:
            must_conditions.append({"match": {"parent_id": parent_node["id"]}})
        else:
            must_conditions.append({"terms": {"level_id": level_ids}})

        # if hierarchy create_empty_nodes
        create_empty_nodes = hierarchy.get("create_empty_nodes")
        if not create_empty_nodes:
            must_not_conditions.append({"match": {"key_is_empty": True}})

        for level in levels:
            show_without_children = level.get("show_without_children")
            if not show_without_children:
                should_cond = {
                    "bool": {
                        "must": [{"match": {"level_id": level["id"]}}],
                        "must_not": [{"match": {"child_count": 0}}],
                    }
                }
                should_query_list.append(should_cond)

        search_query = defaultdict(dict)

        if must_conditions:
            search_query["bool"]["must"] = must_conditions

        if must_not_conditions:
            search_query["bool"]["must_not"] = must_not_conditions

        if should_query_list:
            search_query["bool"]["should"] = should_query_list
            search_query["bool"]["minimum_should_match"] = 1

        # sort_cond
        if nodes_sort_by:
            sort_cond = [
                {
                    item.column_name: {
                        "order": "asc" if item.ascending else "desc"
                    }
                }
                for item in nodes_sort_by
            ]
        else:
            sort_cond = [{"level_id": {"order": "asc"}}]

        query_model = AllResAsListQueryModel(
            query_condition={"query": search_query},
            sort_cond={"sort": sort_cond},
            # items_per_step=100_000
        )

        if offset:
            query_model.offset = offset

        if limit:
            query_model.limit = limit

        res = await get_count_and_all_items_as_list_from_special_index(
            index=HIERARCHY_OBJ_INDEX,
            query_model=query_model,
            async_client=elastic_client,
        )
        return res
    return default_resp


@router.post("/filter")
async def filter_hierarchy(
    elastic_client: Annotated[AsyncElasticsearch, Depends(get_async_client)],
    user_data: Annotated[UserData, Depends(security)],
    hierarchy_id: Annotated[int, Body()],
    filters: Annotated[list[HierarchyFilter], Body()] = None,
    parent_node_id: Annotated[str, Body()] = None,
):
    user_permissions = UserPermissionBuilder(user_data=user_data)
    user_permissions = user_permissions.get_user_permissions()
    task = FilterHierarchiesCurrentLevel(
        elastic_client=elastic_client,
        hierarchy_id=hierarchy_id,
        parent_node_id=parent_node_id,
        user_permissions=user_permissions,
        filters=filters,
    )
    result = await task.execute()
    return result


@router.post("/find_mo_id_in_hierarchies")
async def find_mo_id_in_hierarchies(
    elastic_client: Annotated[AsyncElasticsearch, Depends(get_async_client)],
    user_data: Annotated[UserData, Depends(security)],
    mo_id: Annotated[int, Body(ge=1, embed=True)],
):
    user_permissions = UserPermissionBuilder(user_data=user_data)
    user_permissions = user_permissions.get_user_permissions()
    task = FindMoIdsInHierarchies(
        elastic_client=elastic_client,
        mo_ids=[mo_id],
        permission=user_permissions,
    )
    result = await task.execute()
    return result


@router.post(
    "/get_hierarchy_and_inventory_results",
    response_model=HierarchyFilterResults,
)
async def get_hierarchy_and_inventory_results(
    hierarchy_id: int = Body(),
    filters: List[HierarchyFilter] = Body(None),
    elastic_client: AsyncElasticsearch = Depends(get_async_client),
    user_data: UserData = Depends(security),
    parent_node_id: str = Body(None),
    aggregation: HierarchyAggregateByTMO = Body(None),
    inventory_res: InventoryResAdditionalConditions = Body(None),
):
    # Default response
    res_for_hierarchy = HierarchyFilterHierarchyItem()
    res_for_inventory = HierarchyFilterInventoryItem()
    default_resp = HierarchyFilterResults(
        hierarchy_results=res_for_hierarchy, inventory_results=res_for_inventory
    )

    user_permissions = UserPermissionBuilder(user_data=user_data)
    user_permissions = user_permissions.get_user_permissions()
    # check permissions for filters aggregation, sorting - start
    if user_permissions.is_admin is False:
        all_tprms = set()
        all_tmos = set()
        if filters:
            for h_filter in filters:
                if not h_filter.filter_columns:
                    continue
                f_tprms = {
                    int(f_c.column_name)
                    for f_c in h_filter.filter_columns
                    if f_c.column_name.isdigit()
                }
                all_tprms.update(f_tprms)
                all_tmos.add(h_filter.tmo_id)
        if aggregation:
            all_tprms.add(aggregation.tprm_id)
            all_tmos.add(aggregation.tmo_id)
        if inventory_res:
            # check sorting for inventory
            if inventory_res.sort_by:
                sort_by_tprms = {
                    int(s_c.column_name)
                    for s_c in inventory_res.sort_by
                    if s_c.column_name.isdigit()
                }
                all_tprms.update(sort_by_tprms)

            # check ranges for inventory
            if inventory_res.aggregation_by_ranges:
                for (
                    aggr_items
                ) in inventory_res.aggregation_by_ranges.aggr_items:
                    step_tprm_ids = {
                        int(f_c.column_name)
                        for f_c in aggr_items.aggr_filters
                        if f_c.column_name.isdigit()
                    }
                    all_tprms.update(step_tprm_ids)

        if all_tprms:
            search_body = {
                "query": {
                    "bool": {
                        "must": [
                            {"terms": {"id": list(all_tprms)}},
                            {
                                "terms": {
                                    INVENTORY_PERMISSIONS_FIELD_NAME: user_permissions.user_permissions
                                }
                            },
                        ]
                    }
                },
                "size": len(all_tprms),
                "_source": {"includes": "id"},
            }

            search_res = await elastic_client.search(
                index=INVENTORY_TPRM_INDEX_V2,
                body=search_body,
                ignore_unavailable=True,
            )

            search_res = search_res["hits"]["hits"]
            available_tprms = {item["_source"]["id"] for item in search_res}

            difference = all_tprms.difference(available_tprms)
            if difference:
                raise HTTPException(
                    status_code=403,
                    detail=f"User has no permissions for tprms: {difference}",
                )

        if all_tmos:
            available_tmos = (
                await get_only_available_to_read_tmo_ids_for_special_client(
                    client_permissions=user_permissions.user_permissions,
                    elastic_client=elastic_client,
                    tmo_ids=list(all_tmos),
                )
            )
            difference = set(all_tmos).difference(available_tmos)
            if difference:
                raise HTTPException(
                    status_code=403,
                    detail=f"User has no permissions for tmos: {difference}",
                )

    # check permissions for filters aggregation, sorting - end
    # check parent_id
    if not parent_node_id:
        parent_node_id = None
    else:
        if parent_node_id.upper() in ("ROOT", "NONE", "NULL"):
            parent_node_id = None
        else:
            try:
                parent_node_id = uuid.UUID(parent_node_id)
            except ValueError:
                raise HTTPException(
                    status_code=422,
                    detail="parent_node_id must be instance of"
                    " UUID or be one of values: root, none, null",
                )
    # check node_id
    if isinstance(parent_node_id, uuid.UUID):
        parent_node = await get_node_or_raise_error(
            node_id=parent_node_id, elastic_client=elastic_client
        )
        node_hierarchy_id_as_int = int(parent_node["hierarchy_id"])
        if node_hierarchy_id_as_int != hierarchy_id:
            raise HTTPException(
                status_code=422,
                detail=f"Parent node hierarchy id does not match hierarchy_id "
                f"({node_hierarchy_id_as_int} != {hierarchy_id})",
            )
        is_active = parent_node.get("active")
        if not is_active:
            raise HTTPException(
                status_code=422, detail="Parent node is not active"
            )

    else:
        parent_node = None
    # check hierarchy
    hierarchy = await get_hierarchy_with_permission_check_or_raise_error(
        hierarchy_id=hierarchy_id,
        user_data=user_data,
        elastic_client=elastic_client,
    )
    hierarchy_dto = HierarchyDTO.model_validate(hierarchy)

    if parent_node is None:
        parent_node_dto = None
        parent_level_data = None

    else:
        parent_node_dto = NodeDTO.model_validate(parent_node)
        parent_level_data = LevelDataForChain(
            level_id=parent_node_dto.level_id,
            level_level=int(parent_node_dto.level),
        )

    factory_dep_level = DependentLevelsChainFactory(
        elastic_client, hierarchy_id, parent_level_data=parent_level_data
    )

    chain_of_p_levels = (
        await factory_dep_level.create_dependent_levels_chain_by()
    )
    if parent_node:
        checker = NodeCheckerWithFilters(
            node=parent_node_dto,
            filters=filters,
            hierarchy=hierarchy_dto,
            elastic_client=elastic_client,
            dependent_levels_chain=chain_of_p_levels,
            user_permissions=user_permissions,
        )
        check_res = await checker.check()

        if (
            check_res.full_match_filter_condition is False
            and not check_res.mo_ids_matched_conditions
        ):
            return default_resp

    order_builder = InventoryLevelConditionGrouper(
        depends_level_chain=chain_of_p_levels,
        hierarchy=hierarchy_dto,
        hierarchy_filters=filters,
        aggregation=aggregation,
    )

    order_to_process = order_builder.get_order_to_process()
    if not order_to_process:
        return default_resp

    handler = LevelConditionsOrderHandlerWithoutFilters(
        level_cond_order=order_to_process,
        elastic_client=elastic_client,
        parent_node_dto=parent_node_dto,
        user_permissions=user_permissions,
        limit=inventory_res.limit,
        offset=inventory_res.offset,
        return_results=inventory_res.return_results,
    )

    hierarchy_res, inventory_result = await handler.process_without_filters()
    default_resp.hierarchy_results = hierarchy_res
    default_resp.inventory_results = inventory_result
    default_resp.aggregation_by_ranges = None
    return default_resp
