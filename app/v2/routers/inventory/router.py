import io
import time

from collections import defaultdict
from typing import List, Annotated, Literal

import pandas as pd
from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import APIRouter, Depends, Query, Body, HTTPException, status, Path
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.responses import StreamingResponse

from elastic.client import get_async_client
from elastic.config import (
    INVENTORY_TPRM_INDEX_V2,
    INVENTORY_OBJ_INDEX_PREFIX,
    INVENTORY_TMO_INDEX_V2,
    INVENTORY_MO_LINK_INDEX,
    ALL_MO_OBJ_INDEXES_PATTERN,
)
from elastic.enum_models import (
    SearchOperator,
    LogicalOperator,
    InventoryFieldValType,
    ElasticFieldValType,
)
from elastic.pydantic_models import SearchModel, SortColumn, FilterColumn
from elastic.query_builder_service.inventory_index.mo_object.utils import (
    get_search_query_for_inventory_obj_index,
    get_dict_of_inventory_attr_and_params_types,
)
from elastic.query_builder_service.inventory_index.search_query_builder import (
    InventoryIndexQueryBuilder,
)
from elastic.query_builder_service.inventory_index.utils.index_utils import (
    get_index_names_fo_list_of_tmo_ids,
    get_index_name_by_tmo,
)
from elastic.query_builder_service.inventory_index.utils.parameter_search_utils import (
    get_operators_for_parameter_field_by_inventory_val_type_or_raise_error,
    get_available_operators_for_inventory_val_types,
)
from elastic.query_builder_service.search_operators.first_depth_fields.utils import (
    get_where_condition_functions_for_first_depth_fields_by_val_type_or_raise_error,
)
from grpc_clients.inventory.getters.getters_with_channel import (
    get_all_tmo_data_from_inventory_channel_in,
)
from indexes_mapping.inventory.mapping import (
    INVENTORY_OBJ_INDEX_MAPPING,
    INVENTORY_PARAMETERS_FIELD_NAME,
    INVENTORY_PERMISSIONS_FIELD_NAME,
    INVENTORY_FUZZY_FIELD_NAME,
)
from indexes_mapping.inventory.zeebe_enums import ZeebeProcessInstanceFields
from security.security_data_models import UserData
from security.security_factory import security
from services.group_builder.models import GroupStatisticUniqueFields
from services.group_builder.reload.utils import GroupBuilderReloader
from services.inventory_services.coord_features.common_models import (
    VertexItemImpl,
    EdgeItemImpl,
    WayItemResponseModel,
)
from services.inventory_services.coord_features.points_in_one_line.impl import (
    reform_all_connected_points_between_start_point_and_end_point_into_line,
)
from services.inventory_services.mo_link.mo_link_info_finder import (
    MOLinkInfoFinder,
)
from services.inventory_services.models import (
    InventoryMODefaultFields,
    InventoryMOProcessedFields,
    InventoryFuzzySearchFields,
)
from services.inventory_services.reload.inventory_data import (
    InventoryIndexesReloader,
)
from services.inventory_services.reload.inventory_security import (
    InventorySecurityReloader,
)

from services.inventory_services.utils.security.filter_by_realm import (
    check_permission_is_admin,
    get_permissions_from_client_role,
    get_cleared_filter_columns_with_available_tprm_ids,
    get_cleared_sort_columns_with_available_tprm_ids,
    get_only_available_to_read_tmo_ids_for_special_client,
    get_only_available_to_read_tprm_ids_for_special_client,
    raise_forbidden_ex_if_user_has_no_permission,
    get_post_filter_condition_according_user_permissions,
    raise_forbidden_ex_if_user_has_no_permission_to_special_mo,
    check_availability_of_tmo_data,
)
from services.zeebe_services.reload.utils import ZeebeProcessInstanceReloader
from settings.config import (
    ZEEBE_CLIENT_HOST,
    ZEEBE_CLIENT_GRPC_PORT,
    GROUP_BUILDER_HOST,
    GROUP_BUILDER_GRPC_PORT,
)
from v2.database.database import get_session
from v2.routers.inventory.utils.create_inventory_data_filter import (
    create_inventory_data_filter,
    InventoryDataFilter,
)
from v2.routers.inventory.utils.helpers import (
    get_tprm_val_types_need_to_check_in_global_search_by_inputted_data,
    get_operator_for_global_search_by_inventory_val_type,
    get_query_to_find_tprms_by_val_types,
)
from v2.routers.inventory.utils.models import (
    MOLinkInfoResponse,
    FuzzySearchRequestModel,
    MOWithParametersResponseModel,
    PaginationMetaData,
    MOLinkOutInfoResponse,
    MOLinkInListInfoResponse,
)
from v2.routers.inventory.utils.search_by_value_utils import (
    get_query_for_search_by_value_in_tmo_scope,
)
from v2.routers.severity.utils import (
    clear_receiving_columns,
    get_group_inventory_must_not_conditions,
)

router = APIRouter(prefix="/inventory")


@router.get("/reload_all_inventory_indexes", tags=["Inventory indexes: main"])
async def reload_all_inventory_indexes(
    elastic_client: AsyncElasticsearch = Depends(get_async_client),
    db_session: AsyncSession = Depends(get_session),
    user_data: UserData = Depends(security),
):
    rebuilder = InventoryIndexesReloader(elastic_client, session=db_session)
    await rebuilder.refresh_all_inventory_indexes()

    all_tmo = await get_all_tmo_data_from_inventory_channel_in()

    for item in all_tmo:
        tmo_id = item.get("id")
        if tmo_id:
            if all([ZEEBE_CLIENT_HOST, ZEEBE_CLIENT_GRPC_PORT]):
                zeebe_reloader = ZeebeProcessInstanceReloader(
                    elastic_client=elastic_client, session=db_session
                )
                await zeebe_reloader.reload_zeebe_data_for_tmo_id(tmo_id)

            if all([GROUP_BUILDER_HOST, GROUP_BUILDER_GRPC_PORT]):
                group_reloader = GroupBuilderReloader(
                    elastic_client=elastic_client, session=db_session
                )
                await group_reloader.reload_group_data_for_tmo_id(tmo_id)

    rebuilder = InventorySecurityReloader(elastic_client, session=db_session)
    await rebuilder.refresh_all_inventory_security_indexes()


@router.post("/get_objects_by_ids", tags=["Inventory indexes: main"])
async def get_objects_by_ids(
    mo_ids: List[int] = Body(max_length=10_000),
    elastic_client: AsyncElasticsearch = Depends(get_async_client),
    user_data: UserData = Depends(security),
):
    """Returns Inventory objects with attributes and parameters by list of objects ids"""

    is_admin = check_permission_is_admin(client_role=user_data.realm_access)
    user_permissions = get_permissions_from_client_role(
        client_role=user_data.realm_access
    )

    # get object only by specific ids
    search_conditions = [{"terms": {"id": mo_ids}}]

    # here we store searching by requested MO ids and TMO ids, which available for current user
    must_query = {"bool": {"must": search_conditions}}

    body = {"query": must_query}

    if not is_admin:
        raise_forbidden_ex_if_user_has_no_permission(
            client_permissions=user_permissions
        )
        search_conditions.append(
            {"terms": {INVENTORY_PERMISSIONS_FIELD_NAME: user_permissions}}
        )

        # get available TMOs
        available_tmos = (
            await get_only_available_to_read_tmo_ids_for_special_client(
                client_permissions=user_permissions,
                elastic_client=elastic_client,
            )
        )

        if not available_tmos:
            # user doesn't have permission for any TMO
            return []

        query_for_available_tmos = {"terms": {"tmo_id": available_tmos}}
        search_conditions.append(query_for_available_tmos)

        # get available TPRMs
        available_tprms = (
            await get_only_available_to_read_tprm_ids_for_special_client(
                client_permissions=user_permissions,
                elastic_client=elastic_client,
                tmo_ids=available_tmos,
            )
        )

        # format query to get only those parameters, which can be returned by user permission
        source_includes = [f.value for f in InventoryMODefaultFields]
        source_includes.extend(
            [
                f"{INVENTORY_PARAMETERS_FIELD_NAME}.{tprm_id}"
                for tprm_id in available_tprms
            ]
        )

        if source_includes:
            body["_source"] = {"includes": source_includes}

    # send query to get MOs by available TMOs and with available TPMS/PRM pares
    search_res = await elastic_client.search(
        index=f"{INVENTORY_OBJ_INDEX_PREFIX}*",
        body=body,
        ignore_unavailable=True,
        size=10_000,
    )

    # format response from elastic view
    res_objects = []

    for item in search_res["hits"]["hits"]:
        # if 'available_tprms' is empty - we will get only objects attributes
        # it`s not ok to send 'parameters' attr in one time but in another not send him
        # We need to show, that parameters are empty
        if "parameters" not in item["_source"]:
            item["_source"]["parameters"] = {}

        res_objects.append(item["_source"])

    return res_objects


@router.get("/get_objects_by_coords", tags=["Inventory indexes: main"])
async def read_objects_by_coords(
    latitude_min: Annotated[float, Query(ge=-90, le=90)],
    latitude_max: Annotated[float, Query(ge=-90, le=90)],
    longitude_min: Annotated[float, Query(ge=-180, le=180)],
    longitude_max: Annotated[float, Query(ge=-180, le=180)],
    tmo_ids: List[int] = Query(None),
    limit: int = Query(default=2000000, ge=0, le=2000000),
    offset: int = Query(default=0, ge=0),
    elastic_client: AsyncElasticsearch = Depends(get_async_client),
    only_active: bool = Query(True),
    with_parameters: bool = Query(False),
    user_data: UserData = Depends(security),
):
    """Returns Inventory objects with all params that match filter condition"""

    is_admin = check_permission_is_admin(client_role=user_data.realm_access)
    user_permissions = get_permissions_from_client_role(
        client_role=user_data.realm_access
    )
    if not is_admin:
        raise_forbidden_ex_if_user_has_no_permission(
            client_permissions=user_permissions
        )

    # coords validation
    if latitude_max < latitude_min:
        raise HTTPException(
            status_code=400,
            detail="latitude_min can`t be more than latitude_max",
        )

    if longitude_max < longitude_min:
        raise HTTPException(
            status_code=400,
            detail="longitude_min can`t be more than longitude_max",
        )

    source_includes = None
    if with_parameters is False:
        source_includes = [f.value for f in InventoryMODefaultFields]

        returnable_param_ids = list()
        # get not required params for tmo

        search_conditions = list()
        search_conditions.append({"match": {"returnable": True}})
        if not is_admin:
            # TODO : remove comment to enable tprm permissions
            # search_conditions.append({"terms": {INVENTORY_PERMISSIONS_FIELD_NAME: user_permissions}})
            pass

        if tmo_ids:
            search_conditions.append({"terms": {"tmo_id": tmo_ids}})

        search_query = {"bool": {"must": search_conditions}}

        sort_cond = {"id": {"order": "asc"}}
        size_per_step = 10000
        search_args = {
            "query": search_query,
            "sort": sort_cond,
            "size": size_per_step,
            "track_total_hits": True,
            "index": INVENTORY_TPRM_INDEX_V2,
        }

        search_after = None

        while True:
            search_res = await elastic_client.search(
                **search_args, search_after=search_after, source_includes=["id"]
            )
            tprm_ids = [
                item["_source"]["id"] for item in search_res["hits"]["hits"]
            ]

            total_hits = search_res["hits"]["total"]["value"]
            returnable_param_ids.extend(tprm_ids)

            if total_hits < size_per_step:
                break

            if len(tprm_ids) == size_per_step:
                search_after = search_res["hits"]["hits"][-1]["sort"]
            else:
                break

        if returnable_param_ids:
            source_includes.extend(
                [
                    f"{INVENTORY_PARAMETERS_FIELD_NAME}.{tprm_id}"
                    for tprm_id in returnable_param_ids
                ]
            )
    else:
        # if with_parameters is True
        if not is_admin:
            available_tprm_ids = (
                await get_only_available_to_read_tprm_ids_for_special_client(
                    client_permissions=user_permissions,
                    elastic_client=elastic_client,
                    tmo_ids=tmo_ids,
                )
            )
            source_includes = [f.value for f in InventoryMODefaultFields]
            source_includes.extend(
                [
                    f"{INVENTORY_PARAMETERS_FIELD_NAME}.{tprm_id}"
                    for tprm_id in available_tprm_ids
                ]
            )

    # search by geometry_field
    query1 = {
        "bool": {
            "must": [
                {"exists": {"field": "geometry.path"}},
                {
                    "geo_bounding_box": {
                        "geometry.path": {
                            "top_left": {
                                "lat": latitude_max,
                                "lon": longitude_min,
                            },
                            "bottom_right": {
                                "lat": latitude_min,
                                "lon": longitude_max,
                            },
                        }
                    }
                },
            ]
        }
    }

    obj_active_search = SearchModel(
        column_name="active",
        operator=SearchOperator.EQUALS.value,
        column_type=InventoryFieldValType.BOOL.value,
        value=only_active,
        multiple=False,
    )

    additional_query_for_query_1 = InventoryIndexQueryBuilder(
        logical_operator=LogicalOperator.AND.value,
        search_list_order=[obj_active_search],
    )
    additional_query_for_query_1 = (
        additional_query_for_query_1.create_query_as_dict()
    )
    query1["bool"]["must"].extend(additional_query_for_query_1["bool"]["must"])

    min_lat_search = SearchModel(
        column_name="latitude",
        operator=SearchOperator.MORE_OR_EQ.value,
        column_type="float",
        value=latitude_min,
        multiple=False,
    )
    max_lat_search = SearchModel(
        column_name="latitude",
        operator=SearchOperator.LESS_OR_EQ.value,
        column_type="float",
        value=latitude_max,
        multiple=False,
    )
    min_long_search = SearchModel(
        column_name="longitude",
        operator=SearchOperator.MORE_OR_EQ.value,
        column_type="float",
        value=longitude_min,
        multiple=False,
    )
    max_long_search = SearchModel(
        column_name="longitude",
        operator=SearchOperator.LESS_OR_EQ.value,
        column_type="float",
        value=longitude_max,
        multiple=False,
    )

    search_models = [
        min_lat_search,
        max_lat_search,
        min_long_search,
        max_long_search,
        obj_active_search,
    ]

    search_index = f"{INVENTORY_OBJ_INDEX_PREFIX}*"

    if tmo_ids:
        if not is_admin:
            tmo_ids = (
                await get_only_available_to_read_tmo_ids_for_special_client(
                    client_permissions=user_permissions,
                    elastic_client=elastic_client,
                    tmo_ids=tmo_ids,
                )
            )

        search_indexes = get_index_names_fo_list_of_tmo_ids(tmo_ids)
        search_index = search_indexes
    else:
        if not is_admin:
            tmo_ids = (
                await get_only_available_to_read_tmo_ids_for_special_client(
                    client_permissions=user_permissions,
                    elastic_client=elastic_client,
                )
            )
            search_indexes = get_index_names_fo_list_of_tmo_ids(tmo_ids)
            search_index = search_indexes

    if not search_index:
        return {"objects": [], "total_hits": 0}

    query_builder = InventoryIndexQueryBuilder(
        logical_operator=LogicalOperator.AND.value,
        search_list_order=search_models,
    )

    query2 = query_builder.create_query_as_dict()

    # exclude groups from results query
    group_condition_must_not = [
        {"exists": {"field": GroupStatisticUniqueFields.GROUP_NAME.value}}
    ]

    final_query = {
        "bool": {
            "must_not": group_condition_must_not,
            "should": [query1, query2],
            "minimum_should_match": 1,
        }
    }

    body = {
        "query": final_query,
        "_source": {"excludes": [INVENTORY_PERMISSIONS_FIELD_NAME]},
        "track_total_hits": True,
    }

    if source_includes:
        body["_source"] = {"includes": source_includes}

    if limit:
        body["size"] = limit

    if offset:
        body["from_"] = offset
    # use body to senf POST request (GET request is limited by size)
    res = await elastic_client.search(
        index=search_index, body=body, ignore_unavailable=True
    )
    total_hit = res["hits"]["total"]["value"]

    res = [item["_source"] for item in res["hits"]["hits"]]
    return {"objects": res, "total_hits": total_hit}


@router.post(
    "/get_inventory_objects_by_filters", tags=["Inventory indexes: main"]
)
async def get_inventory_objects_by_filters(
    filter_columns: List[FilterColumn] = None,
    sort_by: list[SortColumn] = None,
    tmo_id: int = Body(),
    with_groups: Annotated[bool | None, Body(alias="with_groups")] = False,
    elastic_client: AsyncElasticsearch = Depends(get_async_client),
    limit: int = Body(2000000, ge=0, le=2000000),
    offset: int = Body(0, ge=0),
    search_by_value: str = Body(None),
    user_data: UserData = Depends(security),
):
    """Returns filtered results from Inventory object index. Replaced mo_link ids and prm_link ids with corresponding
    values. Added parent_name into results. search_by_value uses for search by all mo attrs and params
    (in present time not implemented for mo_link, prm_link)"""
    inventory_filter: InventoryDataFilter = await create_inventory_data_filter(
        user_data=user_data,
        filter_columns=filter_columns,
        sort_by=sort_by,
        search_by_value=search_by_value,
        elastic_client=elastic_client,
        limit=limit,
        offset=offset,
        with_groups=with_groups,
        tmo_id=tmo_id,
    )

    search_res = await elastic_client.search(
        index=inventory_filter.search_index,
        body=inventory_filter.body,
        ignore_unavailable=True,
    )
    total_hits = search_res["hits"]["total"]["value"]
    objects = [item["_source"] for item in search_res["hits"]["hits"]]
    return {"objects": objects, "total_hits": total_hits}


@router.get("/get_inventory_objects_by_value", tags=["Inventory indexes: main"])
async def get_inventory_objects_by_value(
    search_value: str,
    tmo_ids: List[int] = Query(None),
    with_groups: bool = Query(default="False"),
    limit: int = Query(default=2000000, le=2000000, ge=0),
    offset: int = Query(default=0, ge=0),
    elastic_client: AsyncElasticsearch = Depends(get_async_client),
    include_tmo_name: bool = False,
    only_active: bool = Query(False),
    user_data: UserData = Depends(security),
):
    """Search for objects which contains value in the name attribute and returnable params"""
    # main security checks start
    start_time = time.perf_counter()
    is_admin = check_permission_is_admin(client_role=user_data.realm_access)
    user_permissions = get_permissions_from_client_role(
        client_role=user_data.realm_access
    )
    if not is_admin:
        raise_forbidden_ex_if_user_has_no_permission(
            client_permissions=user_permissions
        )
        if tmo_ids:
            tmo_ids = (
                await get_only_available_to_read_tmo_ids_for_special_client(
                    client_permissions=user_permissions,
                    elastic_client=elastic_client,
                    tmo_ids=tmo_ids,
                )
            )
        else:
            tmo_ids = (
                await get_only_available_to_read_tmo_ids_for_special_client(
                    client_permissions=user_permissions,
                    elastic_client=elastic_client,
                )
            )

    tprm_val_types = (
        get_tprm_val_types_need_to_check_in_global_search_by_inputted_data(
            search_value
        )
    )

    if not is_admin:
        tprms_query = get_query_to_find_tprms_by_val_types(
            val_types=tprm_val_types,
            tmo_ids=tmo_ids,
            only_returnable=False,
            client_permissions=user_permissions,
        )
    else:
        tprms_query = get_query_to_find_tprms_by_val_types(
            val_types=tprm_val_types,
            only_returnable=False,
            tmo_ids=tmo_ids,
        )
    # main security checks end

    tprms = await elastic_client.search(
        index=INVENTORY_TPRM_INDEX_V2,
        query=tprms_query,
        size=100000,
        track_total_hits=True,
    )

    tprms = tprms["hits"]["hits"]

    search_name_boost_1 = {
        "bool": {
            "must": [{"match": {"name": {"query": search_value, "boost": 2.0}}}]
        }
    }

    search_name_boost_2 = {
        "bool": {
            "must": [
                {
                    "wildcard": {
                        "name": {
                            "value": f"*{search_value}*",
                            "case_insensitive": True,
                            "boost": 1.5,
                        }
                    }
                }
            ]
        }
    }
    search_name_boost_3 = {
        "bool": {
            "must": [
                {
                    "fuzzy": {
                        "name": {
                            "value": search_value,
                            "fuzziness": "AUTO",
                            "boost": 1.0,
                            "max_expansions": 50,
                        }
                    }
                }
            ]
        }
    }

    search_query_base = {
        "bool": {
            "must": [
                {
                    "bool": {
                        "minimum_should_match": 1,
                        "should": [
                            search_name_boost_1,
                            search_name_boost_2,
                            search_name_boost_3,
                        ],
                    }
                }
            ]
        }
    }
    if only_active:
        search_query_base["bool"].update(
            {"filter": [{"term": {"active": True}}]}
        )

    inventory_search_models = []

    for tprm in tprms:
        column_type = tprm["_source"]["val_type"]
        search_operator = get_operator_for_global_search_by_inventory_val_type(
            column_type
        )

        search_model = SearchModel(
            column_name=str(tprm["_source"]["id"]),
            operator=search_operator,
            column_type=column_type,
            multiple=False,
            value=search_value,
        )

        inventory_search_models.append(search_model)

    search_query_by_prms = InventoryIndexQueryBuilder(
        logical_operator=LogicalOperator.OR.value,
        search_list_order=inventory_search_models,
    )

    search_query_by_prms = search_query_by_prms.create_query_as_dict()

    # get with_groups query condition
    group_condition_must_not = get_group_inventory_must_not_conditions(
        with_groups=with_groups
    )

    if group_condition_must_not:
        search_query_base["bool"]["must_not"] = group_condition_must_not

    if search_query_by_prms:
        try:
            should_for_tprms = search_query_by_prms["bool"]["must"][0]["bool"][
                "should"
            ]
        except Exception as ex:
            print(ex)
        else:
            search_query_base["bool"]["must"][0]["bool"]["should"].extend(
                should_for_tprms
            )

    search_index = f"{INVENTORY_OBJ_INDEX_PREFIX}*"
    if tmo_ids:
        search_indexes = get_index_names_fo_list_of_tmo_ids(tmo_ids)
        search_index = search_indexes

    body = {
        "query": search_query_base,
        "track_total_hits": True,
        "_source": {"excludes": [INVENTORY_PERMISSIONS_FIELD_NAME]},
    }

    if limit:
        body["size"] = limit

    if offset:
        body["from"] = offset

    if not is_admin:
        body["post_filter"] = (
            get_post_filter_condition_according_user_permissions(
                user_permissions
            )
        )

        source_includes = [f.value for f in InventoryMODefaultFields]

        available_tprm_ids = (
            await get_only_available_to_read_tprm_ids_for_special_client(
                client_permissions=user_permissions,
                elastic_client=elastic_client,
                tmo_ids=tmo_ids,
            )
        )
        source_includes.extend(
            [
                f"{INVENTORY_PARAMETERS_FIELD_NAME}.{tprm_id}"
                for tprm_id in available_tprm_ids
            ]
        )
        body["fields"] = source_includes
    search_res = await elastic_client.search(
        index=search_index,
        body=body,
        ignore_unavailable=True,
        search_type="dfs_query_then_fetch",
    )
    total_hits = search_res["hits"]["total"]["value"]

    if include_tmo_name:
        tmo_ids = {
            item["_source"]["tmo_id"] for item in search_res["hits"]["hits"]
        }
        search_query = {"terms": {"id": list(tmo_ids)}}
        tmo_res = await elastic_client.search(
            index=INVENTORY_TMO_INDEX_V2, query=search_query, size=len(tmo_ids)
        )
        tmo_res = {
            item["_source"]["id"]: item["_source"]["name"]
            for item in tmo_res["hits"]["hits"]
        }

        objects = list()
        for item in search_res["hits"]["hits"]:
            item = item["_source"]
            item["tmo_name"] = tmo_res.get(item["tmo_id"])
            objects.append(item)
    else:
        objects = [item["_source"] for item in search_res["hits"]["hits"]]
    print("Elapsed time search by value:", time.perf_counter() - start_time)
    return {"objects": objects, "total_hits": total_hits}


@router.get(
    "/global_search_for_objects_in_the_special_tmo_scope",
    tags=["Inventory indexes: main"],
    deprecated=True,
)
async def get_inventory_objects_by_value_in_the_special_tmo_scope(
    search_value: str,
    tmo_id: int,
    add_parent_name: bool = Query(True),
    limit: int = Query(default=2000000, le=2000000, ge=0),
    offset: int = Query(default=0, ge=0),
    elastic_client: AsyncElasticsearch = Depends(get_async_client),
    user_data: UserData = Depends(security),
):
    """Search for objects which contains value in the name attribute and all params.
    DEPRECATED: use get_inventory_objects_by_filters"""
    search_query = get_query_for_search_by_value_in_tmo_scope(
        elastic_client, tmo_ids=[tmo_id], search_value=search_value
    )
    if not search_query:
        search_query = {"match_all": {}}

    search_index = get_index_name_by_tmo(tmo_id)

    search_params = {
        "index": search_index,
        "query": search_query,
        "track_total_hits": True,
    }

    if limit:
        search_params["size"] = limit

    if offset:
        search_params["from_"] = offset
    try:
        res = await elastic_client.search(**search_params)
    except NotFoundError:
        raise HTTPException(
            status_code=404, detail=f"TMO with id = {tmo_id} does not exist."
        )
    total_hits = res["hits"]["total"]["value"]

    objects = [item["_source"] for item in res["hits"]["hits"]]
    return {"objects": objects, "total_hits": total_hits}


@router.get(
    "/get_available_search_operators_for_special_mo_attr_or_tprm",
    tags=["Inventory indexes: helpers"],
)
async def get_available_search_operators_for_special_mo_attr_or_tprm(
    mo_attr_or_tprm_id: "str",
    elastic_client: AsyncElasticsearch = Depends(get_async_client),
    user_data: UserData = Depends(security),
):
    operators = ""

    tprm = ""
    mo_attr = ""

    if mo_attr_or_tprm_id.isdigit():
        tprm = mo_attr_or_tprm_id

    else:
        mo_attr = mo_attr_or_tprm_id

    val_type = None

    if tprm:
        search_model = SearchModel(
            column_name="id",
            operator=SearchOperator.EQUALS.value,
            column_type=ElasticFieldValType.INTEGER.value,
            multiple=False,
            value=tprm,
        )
        search_query = InventoryIndexQueryBuilder(
            logical_operator=LogicalOperator.OR.value,
            search_list_order=[search_model],
        )

        search_query = search_query.create_query_as_dict()

        tprm_from_search = await elastic_client.search(
            index=INVENTORY_TPRM_INDEX_V2,
            query=search_query,
            ignore_unavailable=True,
        )

        if tprm_from_search["hits"]["total"]["value"] > 0:
            val_type = tprm_from_search["hits"]["hits"][0]["_source"][
                "val_type"
            ]
            try:
                operators = get_operators_for_parameter_field_by_inventory_val_type_or_raise_error(
                    val_type
                )
            except NotImplementedError as e:
                raise HTTPException(status_code=400, detail=str(e))

    elif mo_attr:
        attr_data = INVENTORY_OBJ_INDEX_MAPPING["properties"].get(mo_attr)
        if attr_data:
            val_type = attr_data["type"]
            try:
                operators = get_where_condition_functions_for_first_depth_fields_by_val_type_or_raise_error(
                    val_type
                )
            except NotImplementedError as e:
                raise HTTPException(status_code=400, detail=str(e))

    if operators and val_type:
        return {"val_type": val_type, "operators": list(operators.keys())}
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Available operators does not exist for "
            f"mo_attr_or_tprm_id = {mo_attr_or_tprm_id}",
        )


@router.get(
    "/refresh_all_data_of_special_tmo", tags=["Inventory indexes: main"]
)
async def refresh_all_data_of_special_tmo(
    tmo_id: int,
    elastic_client: AsyncElasticsearch = Depends(get_async_client),
    db_session: AsyncSession = Depends(get_session),
    user_data: UserData = Depends(security),
):
    reloader = InventoryIndexesReloader(
        elastic_client=elastic_client, session=db_session
    )

    await reloader.refresh_index_by_tmo_id(tmo_id)

    if all([ZEEBE_CLIENT_HOST, ZEEBE_CLIENT_GRPC_PORT]):
        zeebe_reloader = ZeebeProcessInstanceReloader(
            elastic_client=elastic_client, session=db_session
        )

        await zeebe_reloader.reload_zeebe_data_for_tmo_id(tmo_id)

    if all([GROUP_BUILDER_HOST, GROUP_BUILDER_GRPC_PORT]):
        group_reloader = GroupBuilderReloader(
            elastic_client=elastic_client, session=db_session
        )
        await group_reloader.reload_group_data_for_tmo_id(tmo_id)

    rebuilder = InventorySecurityReloader(elastic_client, session=db_session)
    await rebuilder.refresh_all_inventory_security_indexes()


@router.get("/get_connected_by_mo_link_objects_to_special_mo_id")
async def get_connected_by_mo_link_objects_to_special_mo_id(
    mo_id: int,
    elastic_client: AsyncElasticsearch = Depends(get_async_client),
    limit: int = Query(default=2000000, le=2000000, ge=0),
    offset: int = Query(default=0, ge=0),
    user_data: UserData = Depends(security),
):
    is_admin = check_permission_is_admin(client_role=user_data.realm_access)
    user_permissions = get_permissions_from_client_role(
        client_role=user_data.realm_access
    )
    special_tprms = list()
    SIZE_PER_STEP = 10000

    if not is_admin:
        raise_forbidden_ex_if_user_has_no_permission(
            client_permissions=user_permissions
        )
        await raise_forbidden_ex_if_user_has_no_permission_to_special_mo(
            client_permissions=user_permissions,
            elastic_client=elastic_client,
            mo_id=mo_id,
        )

        tprms_query = get_query_to_find_tprms_by_val_types(
            val_types=[
                InventoryFieldValType.MO_LINK.value,
                InventoryFieldValType.TWO_WAY_MO_LINK.value,
            ],
            only_returnable=False,
            client_permissions=user_permissions,
        )
        sort_cond = {"id": {"order": "asc"}}

        body = {
            "query": tprms_query,
            "track_total_hits": True,
            "fields": ["id"],
            "size": SIZE_PER_STEP,
            "sort": sort_cond,
        }

        while True:
            search_res = await elastic_client.search(
                index=INVENTORY_TPRM_INDEX_V2,
                body=body,
                ignore_unavailable=True,
            )

            total_hits = search_res["hits"]["total"]["value"]

            tprm_ids = [
                item["_source"]["id"] for item in search_res["hits"]["hits"]
            ]
            special_tprms.extend(tprm_ids)

            if total_hits < SIZE_PER_STEP:
                break

            if len(tprm_ids) == SIZE_PER_STEP:
                search_after = search_res["hits"]["hits"][-1]["sort"]
                body["search_after"] = search_after
            else:
                break

    search_conditions = [{"terms": {"value": [mo_id]}}]
    if special_tprms:
        search_conditions.append({"terms": {"tprm_id": special_tprms}})

    query = {"bool": {"must": search_conditions}}

    body = {
        "query": query,
        "track_total_hits": True,
        "_source": {"includes": ["mo_id"]},
        "size": SIZE_PER_STEP,
        "sort": {"id": {"order": "asc"}},
    }

    linked_mo_ids = list()

    while True:
        search_res = await elastic_client.search(
            index=INVENTORY_MO_LINK_INDEX, body=body, ignore_unavailable=True
        )

        total_hits = search_res["hits"]["total"]["value"]

        mo_ids = [
            item["_source"]["mo_id"] for item in search_res["hits"]["hits"]
        ]
        linked_mo_ids.extend(mo_ids)

        if total_hits < SIZE_PER_STEP:
            break

        if len(mo_ids) == SIZE_PER_STEP:
            search_after = search_res["hits"]["hits"][-1]["sort"]
            body["search_after"] = search_after
        else:
            break

    if not linked_mo_ids:
        return {"objects": [], "total_hits": 0}

    search_conditions = [{"terms": {"id": linked_mo_ids}}]
    if not is_admin:
        search_conditions.append(
            {"terms": {INVENTORY_PERMISSIONS_FIELD_NAME: user_permissions}}
        )

    query = {"bool": {"must": search_conditions}}

    body = {
        "query": query,
        "track_total_hits": True,
        "size": limit,
        "from": offset,
        "_source": {"excludes": [INVENTORY_PERMISSIONS_FIELD_NAME]},
        "sort": {"id": {"order": "asc"}},
    }

    res = await elastic_client.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN, body=body, ignore_unavailable=True
    )
    total_hits = res["hits"]["total"]["value"]
    objects = [item["_source"] for item in res["hits"]["hits"]]
    return {"objects": objects, "total_hits": total_hits}


@router.get(
    "/mo_link_info",
    status_code=status.HTTP_200_OK,
    response_model=MOLinkInfoResponse,
)
async def mo_link_info(
    mo_id: int,
    elastic_client: AsyncElasticsearch = Depends(get_async_client),
    limit: int = Query(default=2000000, le=2000000, ge=0),
    offset: int = Query(default=0, ge=0),
    user_data: UserData = Depends(security),
) -> MOLinkInfoResponse:
    """Search for all MO that have our MO in their parameters"""
    if not mo_id:
        return MOLinkInfoResponse(mo_link_info=[], additional_info=[], total=0)

    is_admin = check_permission_is_admin(client_role=user_data.realm_access)
    user_permissions = get_permissions_from_client_role(
        client_role=user_data.realm_access
    )

    if not is_admin:
        raise_forbidden_ex_if_user_has_no_permission(
            client_permissions=user_permissions
        )
        await raise_forbidden_ex_if_user_has_no_permission_to_special_mo(
            client_permissions=user_permissions,
            elastic_client=elastic_client,
            mo_id=mo_id,
        )

    mo_link_info_finder = MOLinkInfoFinder(elastic_client=elastic_client)
    try:
        result = await mo_link_info_finder.get_in_mo_link(
            mo_id=mo_id,
            is_admin=is_admin,
            user_permissions=user_permissions,
        )
        return result
    except ValueError as ex:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Error: {ex}.",
        )


@router.get("/operator_type_helper", tags=["Inventory indexes: helpers"])
async def get_operators_for_columns(
    user_data: UserData = Depends(security),
):
    """Returns available operators for MS Inventory val_types
    (prm_link does not included because it's corresponding with special TPRM type, use
    the 'get_available_search_operators_for_special_mo_attr_or_tprm' endpoint )"""

    return get_available_operators_for_inventory_val_types()


@router.get(
    "/reload_inventory_security_indexes", tags=["Inventory indexes: main"]
)
async def reload_inventory_security_indexes(
    elastic_client: AsyncElasticsearch = Depends(get_async_client),
    db_session: AsyncSession = Depends(get_session),
    user_data: UserData = Depends(security),
):
    rebuilder = InventorySecurityReloader(elastic_client, session=db_session)
    await rebuilder.refresh_all_inventory_security_indexes()


@router.post("/export", tags=["Inventory indexes: main"])
async def export_data(
    tmo_id: Annotated[int, Body()],
    find_by_value: Annotated[str | None, Body()] = None,
    filters_list: Annotated[list[FilterColumn] | None, Body()] = None,
    with_groups: Annotated[bool | None, Body()] = True,
    sort: Annotated[list[SortColumn] | None, Body()] = None,
    columns: Annotated[list[str] | None, Body()] = None,
    file_type: Annotated[Literal["csv", "xlsx"], Body()] = "csv",
    csv_delimiter: Annotated[str, Body(max_length=1)] = None,
    elastic_client: AsyncElasticsearch = Depends(get_async_client),
    with_parents_data: bool = Body(False),
    user_data: UserData = Depends(security),
):
    # get user permissions
    is_admin = check_permission_is_admin(client_role=user_data.realm_access)
    user_permissions = get_permissions_from_client_role(
        client_role=user_data.realm_access
    )
    if not is_admin:
        raise_forbidden_ex_if_user_has_no_permission(
            client_permissions=user_permissions
        )

    # check if index for tmo_id exists
    mo_index_for_tmo_id = get_index_name_by_tmo(tmo_id)
    if not await elastic_client.indices.exists(index=mo_index_for_tmo_id):
        raise HTTPException(
            status_code=404,
            detail=f"Index for tmo_id = {tmo_id} does not exist",
        )

    # get severity id for tmo-id
    # check if id exists
    search_conditions = [{"match": {"id": tmo_id}}]
    if not is_admin:
        search_conditions.append(
            {"terms": {INVENTORY_PERMISSIONS_FIELD_NAME: user_permissions}}
        )
    search_query = {"bool": {"must": search_conditions}}
    tmo_data = await elastic_client.search(
        index=INVENTORY_TMO_INDEX_V2,
        query=search_query,
        size=1,
        track_total_hits=True,
        ignore_unavailable=True,
    )

    tmo_data = tmo_data["hits"]["hits"]
    if not tmo_data:
        raise HTTPException(
            status_code=404, detail=f"TMO with id = {tmo_id} does not exist"
        )

    tmo_data = tmo_data[0]["_source"]

    if not is_admin:
        # get all available tprm_ids fot special user
        available_tprm_ids = (
            await get_only_available_to_read_tprm_ids_for_special_client(
                client_permissions=user_permissions,
                elastic_client=elastic_client,
                tmo_ids=[tmo_id],
            )
        )
        set_of_available_tprm_ids = set(available_tprm_ids)

        # clear filters_list
        if filters_list:
            filters_list = get_cleared_filter_columns_with_available_tprm_ids(
                filter_columns=filters_list,
                available_tprm_ids=set_of_available_tprm_ids,
            )

        # clear sort
        if sort:
            sort = get_cleared_sort_columns_with_available_tprm_ids(
                sort_columns=sort, available_tprm_ids=set_of_available_tprm_ids
            )

    # get column_names types from ranges_objects and filters_list
    column_names = set()

    if filters_list:
        column_names.update(
            {filter_column.column_name for filter_column in filters_list}
        )

    if sort:
        column_names.update(sort_column.column_name for sort_column in sort)

    dict_of_types = await get_dict_of_inventory_attr_and_params_types(
        array_of_attr_and_params_names=column_names,
        elastic_client=elastic_client,
    )
    # get filter column query
    filters_must_cond = list()
    filters_must_not_cond = list()
    if filters_list:
        filter_column_query = get_search_query_for_inventory_obj_index(
            filter_columns=filters_list, dict_of_types=dict_of_types
        )
        filter_bool = filter_column_query.get("bool")
        if filter_bool:
            filter_must_cond = filter_bool.get("must")
            filter_must_not_cond = filter_bool.get("must_not")
            if filter_must_cond:
                filters_must_cond.extend(filter_must_cond)

            if filter_must_not_cond:
                filters_must_not_cond.extend(filter_must_not_cond)

    # get find_by_value search query
    find_by_value_must_cond = list()
    find_by_value_must_not_cond = list()
    if find_by_value:
        by_value_search_query = (
            await get_query_for_search_by_value_in_tmo_scope(
                elastic_client, tmo_ids=[tmo_id], search_value=find_by_value
            )
        )
        find_by_value_bool = by_value_search_query.get("bool")
        if find_by_value_bool:
            by_value_must_cond = find_by_value_bool.get("must")
            by_value_must_not_cond = find_by_value_bool.get("must_not")
            if by_value_must_cond:
                find_by_value_must_cond.extend(by_value_must_cond)

            if by_value_must_not_cond:
                find_by_value_must_not_cond.extend(by_value_must_not_cond)

    # get group condition query
    group_condition_must_not = get_group_inventory_must_not_conditions(
        with_groups
    )

    # tmo_id condition query
    tmo_id_condition_must_query = [{"match": {"tmo_id": tmo_id}}]

    all_must_cond = [
        filters_must_cond,
        find_by_value_must_cond,
        tmo_id_condition_must_query,
    ]
    all_must_not_cond = [
        filters_must_not_cond,
        find_by_value_must_not_cond,
        group_condition_must_not,
    ]

    main_query_must_cond = list()
    main_query_must_not_cond = list()

    for must_cond in all_must_cond:
        main_query_must_cond.extend(must_cond)

    for must_not_cond in all_must_not_cond:
        main_query_must_not_cond.extend(must_not_cond)

    main_query = dict()

    if main_query_must_cond:
        main_query["must"] = main_query_must_cond

    if main_query_must_not_cond:
        main_query["must_not"] = main_query_must_not_cond

    if main_query:
        main_query = {"bool": main_query}

    sort_cond = list()
    if sort:
        for sort_column in sort:
            column_name = sort_column.column_name
            sort_column_type = dict_of_types.get(column_name)
            if sort_column_type:
                sort_column_type = sort_column_type.get("val_type")

            if column_name.isdigit():
                column_name = f"{INVENTORY_PARAMETERS_FIELD_NAME}.{column_name}"
            order_direction = "asc" if sort_column.ascending else "desc"
            data = dict()
            data["order"] = order_direction
            if sort_column_type in {"date", "datetime"}:
                data["type"] = "date"
            sort_cond.append({column_name: {"order": order_direction}})

    # append sort by id to implement search_after
    sort_cond.append({"id": {"order": "asc"}})

    search_args = {
        "index": ALL_MO_OBJ_INDEXES_PATTERN,
        "query": main_query,
        "size": 10000,
        "track_total_hits": True,
        "sort": sort_cond,
        "_source_excludes": [INVENTORY_PERMISSIONS_FIELD_NAME],
    }

    kwargs_for_clearing_columns = {
        "tmo_id": tmo_id,
        "elastic_client": elastic_client,
        "columns": columns,
    }

    # if not admin return only available tprms
    if not is_admin:
        kwargs_for_clearing_columns["user_permissions"] = user_permissions

    cleared_columns_data_dict = await clear_receiving_columns(
        **kwargs_for_clearing_columns
    )
    ordered_columns_list = cleared_columns_data_dict["cleared_ordered_columns"]
    data_of_included_tprms = cleared_columns_data_dict["data_of_included_tprms"]

    result_df = pd.DataFrame()

    parent_tmo_av_data = None

    if with_parents_data:
        # check if there are parent_id
        parent_tmo_id = tmo_data.get("p_id")
        if parent_tmo_id:
            parent_tmo_av_data = await check_availability_of_tmo_data(
                elastic_client=elastic_client,
                is_admin=is_admin,
                tmo_id=parent_tmo_id,
                user_permissions=user_permissions,
            )
    parent_data_source_includes = None
    if parent_tmo_av_data is not None and parent_tmo_av_data.data_available:
        mo_default_attrs = [x.value for x in InventoryMODefaultFields]
        mo_processed_attrs = [x.value for x in InventoryMOProcessedFields]
        mo_camunda_fields = [x.value for x in ZeebeProcessInstanceFields]
        parent_tprms_fields = list()
        if parent_tmo_av_data.available_tprm_data:
            parent_tprms_fields = [
                f"{INVENTORY_PARAMETERS_FIELD_NAME}.{tprm_id}"
                for tprm_id in parent_tmo_av_data.available_tprm_data
            ]

        parent_data_source_includes = (
            mo_default_attrs
            + mo_processed_attrs
            + mo_camunda_fields
            + parent_tprms_fields
        )

    if main_query and ordered_columns_list:
        default_dict_of_data = dict()

        include_columns = dict(id=None, p_id=None)
        for column_name in ordered_columns_list:
            if column_name.isdigit():
                include_columns[
                    f"{INVENTORY_PARAMETERS_FIELD_NAME}.{column_name}"
                ] = None
            else:
                include_columns[column_name] = None
            default_dict_of_data[column_name] = None

        search_after = []
        while True:
            search_res = await elastic_client.search(
                **search_args,
                search_after=search_after if search_after else None,
                source_includes=list(include_columns),
            )
            rows = dict()
            parent_id_item_ids = defaultdict(list)
            for row in search_res["hits"]["hits"]:
                row = row["_source"]
                item_id = row.get("id")
                item_p_id = row.get("p_id")
                row_parameters = row.get(INVENTORY_PARAMETERS_FIELD_NAME)
                if row_parameters:
                    row.update(row_parameters)

                ordered_row_data = {k: row.get(k) for k in default_dict_of_data}
                rows[item_id] = ordered_row_data
                if item_p_id:
                    parent_id_item_ids[item_p_id].append(item_id)

            # get data for parents and add to results Start
            if (
                parent_tmo_av_data is not None
                and parent_tmo_av_data.data_available
            ):
                if parent_id_item_ids:
                    parent_ids = list(parent_id_item_ids)

                    search_conditions = [{"terms": {"id": parent_ids}}]
                    if not is_admin:
                        search_conditions.append(
                            {
                                "terms": {
                                    INVENTORY_PERMISSIONS_FIELD_NAME: user_permissions
                                }
                            }
                        )

                    body = {
                        "query": {"bool": {"must": search_conditions}},
                        "size": len(parent_ids),
                        "track_total_hits": True,
                        "_source": {"includes": parent_data_source_includes},
                    }

                    parent_data = await elastic_client.search(
                        index=ALL_MO_OBJ_INDEXES_PATTERN,
                        body=body,
                        ignore_unavailable=True,
                    )
                    parent_data = parent_data["hits"]["hits"]
                    if parent_data:
                        # add data to results
                        for parent_item in parent_data:
                            parent_item = parent_item["_source"]
                            parent_item_parameters = parent_item.get(
                                INVENTORY_PARAMETERS_FIELD_NAME
                            )
                            if parent_item_parameters:
                                parent_item.update(parent_item_parameters)
                                del parent_item[INVENTORY_PARAMETERS_FIELD_NAME]
                            id_of_parent = parent_item.get("id")
                            parent_item = {
                                f"__parent__.{k}": v
                                for k, v in parent_item.items()
                            }

                            list_of_children = parent_id_item_ids.get(
                                id_of_parent
                            )
                            if list_of_children:
                                for children_id in list_of_children:
                                    rows[children_id].update(parent_item)
                # get data for parents and add to results End

            if rows:
                search_after = search_res["hits"]["hits"][-1]["sort"]
                step_df = pd.DataFrame(list(rows.values()))
                result_df = pd.concat([result_df, step_df])
            else:
                break

    # rename df columns
    rename_tprm_id_tprm_name = dict()
    if data_of_included_tprms:
        rename_tprm_id_tprm_name = {
            str(k): v.get("name") for k, v in data_of_included_tprms.items()
        }
        result_df.rename(columns=rename_tprm_id_tprm_name, inplace=True)

    rename_parent_tprm_id_tprm_name = dict()
    if (
        parent_tmo_av_data
        and parent_tmo_av_data.data_available
        and parent_tmo_av_data.available_tprm_data
    ):
        rename_parent_tprm_id_tprm_name = {
            f"__parent__.{k}": f"__parent__.{v.get('name')}"
            for k, v in parent_tmo_av_data.available_tprm_data.items()
        }

    rename_data = {
        **rename_tprm_id_tprm_name,
        **rename_parent_tprm_id_tprm_name,
    }
    if rename_data:
        result_df.rename(columns=rename_data, inplace=True)

    file_name = "pm_data"
    if file_type == "csv":
        default_csv_separator = ";"
        if csv_delimiter:
            default_csv_separator = csv_delimiter
        file_name = f"{file_name}.csv"

        headers = {"Content-Disposition": f'attachment; filename="{file_name}"'}
        file_like_obj = io.StringIO()
        result_df.to_csv(file_like_obj, index=False, sep=default_csv_separator)

    else:
        file_like_obj = io.BytesIO()
        file_name = f"{file_name}.xlsx"
        headers = {"Content-Disposition": f'attachment; filename="{file_name}"'}
        result_df.to_excel(file_like_obj, index=False)

    del result_df
    file_like_obj.seek(0)

    return StreamingResponse(file_like_obj, headers=headers)


@router.get(
    "/get_children_grouped_by_tmo/{p_id}", tags=["Inventory indexes: main"]
)
async def get_children_grouped_by_tmo(
    p_id: int = Path(gt=0),
    elastic_client: AsyncElasticsearch = Depends(get_async_client),
    user_data: UserData = Depends(security),
):
    # get user permissions
    is_admin = check_permission_is_admin(client_role=user_data.realm_access)
    user_permissions = get_permissions_from_client_role(
        client_role=user_data.realm_access
    )
    if not is_admin:
        raise_forbidden_ex_if_user_has_no_permission(
            client_permissions=user_permissions
        )

    search_conditions = [
        {"match": {"p_id": p_id}},
        {"exists": {"field": "tmo_id"}},
    ]

    if not is_admin:
        search_conditions.append(
            {"terms": {INVENTORY_PERMISSIONS_FIELD_NAME: user_permissions}}
        )

    body = {
        "query": {"bool": {"must": search_conditions}},
        "size": 0,
        "track_total_hits": True,
        "aggs": {"by_tmo_id": {"terms": {"field": "tmo_id"}}},
    }

    search_res = await elastic_client.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN, body=body, ignore_unavailable=True
    )

    search_res = search_res["aggregations"]["by_tmo_id"]["buckets"]

    res = {
        int(item["key"]): doc_count
        for item in search_res
        if (doc_count := item["doc_count"])
    }
    return res


@router.post(
    "/get_objects_by_name_with_misspelled_words_or_typo_mistakes",
    tags=["Inventory indexes: main"],
    response_model=MOWithParametersResponseModel,
    response_model_exclude_none=True,
)
async def get_objects_by_name_with_misspelled_words_or_typo_mistakes(
    search_cond: FuzzySearchRequestModel,
    elastic_client: AsyncElasticsearch = Depends(get_async_client),
    user_data: UserData = Depends(security),
):
    """Search for inventory items with a specific name, even if the name was entered with typos or misspellings.
    Returns found inventory objects with parameters."""

    # get user permissions
    is_admin = check_permission_is_admin(client_role=user_data.realm_access)
    user_permissions = get_permissions_from_client_role(
        client_role=user_data.realm_access
    )
    if not is_admin:
        raise_forbidden_ex_if_user_has_no_permission(
            client_permissions=user_permissions
        )

    must_conditions = []

    if not is_admin:
        must_conditions.append(
            {"terms": {INVENTORY_PERMISSIONS_FIELD_NAME: user_permissions}}
        )

    all_fuzzy_search_fields = [
        f"{INVENTORY_FUZZY_FIELD_NAME}.{enum_item.value}"
        for enum_item in InventoryFuzzySearchFields
    ]
    should_conditions = [
        {
            "multi_match": {
                "query": search_cond.search_value,
                "fields": all_fuzzy_search_fields,
            }
        },
        {
            "query_string": {
                "query": f"{search_cond.search_value}*",
                "fields": all_fuzzy_search_fields,
            }
        },
        {
            "multi_match": {
                "query": search_cond.search_value,
                "fields": all_fuzzy_search_fields,
                "fuzziness": 2,
            }
        },
    ]

    search_query = dict()
    if must_conditions:
        search_query["must"] = must_conditions

    if should_conditions:
        search_query["should"] = should_conditions
        search_query["minimum_should_match"] = 1

    search_query = {"bool": search_query}

    body = {
        "query": search_query,
        "size": search_cond.limit,
        "from": search_cond.offset,
        "track_total_hits": True,
        "_source": {
            "excludes": [
                INVENTORY_PERMISSIONS_FIELD_NAME,
                INVENTORY_FUZZY_FIELD_NAME,
            ]
        },
    }

    search_index = ALL_MO_OBJ_INDEXES_PATTERN
    if search_cond.tmo_id:
        search_index = get_index_name_by_tmo(search_cond.tmo_id)

    search_res = await elastic_client.search(
        index=search_index, body=body, ignore_unavailable=True
    )

    res_objects = search_res["hits"]["hits"]
    res_objects = [item["_source"] for item in res_objects]

    res_metadata = PaginationMetaData(
        limit=search_cond.limit,
        offset=search_cond.offset,
        step_count=len(res_objects),
        total_hits=search_res["hits"]["total"]["value"],
    )
    res = MOWithParametersResponseModel(
        metadata=res_metadata, objects=res_objects
    )

    return res


@router.post(
    "/reform_all_connected_points_between_start_point_and_end_point_into_line",
    tags=["Inventory indexes: main"],
    response_model=List[WayItemResponseModel],
)
def line_up_the_points_evenly_between_start_point_and_end_point(
    start_points: List[VertexItemImpl],
    end_points: List[VertexItemImpl],
    list_of_lines: List[EdgeItemImpl],
    list_of_points: List[VertexItemImpl],
    user_data: UserData = Depends(security),
):
    return (
        reform_all_connected_points_between_start_point_and_end_point_into_line(
            start_points, end_points, list_of_lines, list_of_points
        )
    )


@router.post(
    "/out_mo_link",
    status_code=status.HTTP_200_OK,
    response_model=MOLinkOutInfoResponse,
)
async def out_mo_link(
    mo_ids: list[int],
    elastic_client: AsyncElasticsearch = Depends(get_async_client),
    user_data: UserData = Depends(security),
) -> MOLinkOutInfoResponse:
    """Search for all MO that exist at the mo_id in the parameters of type mo_link with user permission restrict"""
    if not mo_ids:
        return MOLinkOutInfoResponse(out_mo_link_info=[])

    is_admin = check_permission_is_admin(client_role=user_data.realm_access)
    user_permissions = get_permissions_from_client_role(
        client_role=user_data.realm_access
    )

    for mo_id in mo_ids:
        if not is_admin:
            raise_forbidden_ex_if_user_has_no_permission(
                client_permissions=user_permissions
            )
            await raise_forbidden_ex_if_user_has_no_permission_to_special_mo(
                client_permissions=user_permissions,
                elastic_client=elastic_client,
                mo_id=mo_id,
            )

    mo_link_info_finder = MOLinkInfoFinder(elastic_client=elastic_client)
    try:
        result = await mo_link_info_finder.get_out_mo_link(
            mo_ids=mo_ids, is_admin=is_admin, user_permissions=user_permissions
        )
        return result
    except ValueError as ex:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Error: {ex}.",
        )


@router.post(
    "/in_mo_link",
    status_code=status.HTTP_200_OK,
    response_model=MOLinkInListInfoResponse,
)
async def in_mo_link(
    mo_ids: list[int],
    elastic_client: AsyncElasticsearch = Depends(get_async_client),
    user_data: UserData = Depends(security),
) -> MOLinkInListInfoResponse:
    if not mo_ids:
        return MOLinkInListInfoResponse(list_info=[])

    is_admin = check_permission_is_admin(client_role=user_data.realm_access)
    user_permissions = get_permissions_from_client_role(
        client_role=user_data.realm_access
    )

    for mo_id in mo_ids:
        if not is_admin:
            raise_forbidden_ex_if_user_has_no_permission(
                client_permissions=user_permissions
            )
            await raise_forbidden_ex_if_user_has_no_permission_to_special_mo(
                client_permissions=user_permissions,
                elastic_client=elastic_client,
                mo_id=mo_id,
            )

    mo_link_info_finder = MOLinkInfoFinder(elastic_client=elastic_client)
    try:
        result = await mo_link_info_finder.get_in_list_mo_link(
            mo_ids=mo_ids, is_admin=is_admin, user_permissions=user_permissions
        )
        return result
    except ValueError as ex:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Error: {ex}.",
        )
