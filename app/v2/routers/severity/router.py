import io
from collections import defaultdict
from typing import Annotated, Literal

from elasticsearch import AsyncElasticsearch
from fastapi import APIRouter, Depends, Body, HTTPException
from starlette.responses import StreamingResponse

from elastic.client import get_async_client
from elastic.config import (
    INVENTORY_TMO_INDEX_V2,
    ALL_MO_OBJ_INDEXES_PATTERN,
    INVENTORY_TPRM_INDEX_V2,
)
from elastic.pydantic_models import FilterColumn
from elastic.query_builder_service.inventory_index.mo_object.utils import (
    get_dict_of_inventory_attr_and_params_types,
    get_search_query_for_inventory_obj_index,
)
from elastic.query_builder_service.inventory_index.utils.index_utils import (
    get_index_name_by_tmo,
)
from indexes_mapping.inventory.mapping import (
    INVENTORY_PARAMETERS_FIELD_NAME,
    INVENTORY_PERMISSIONS_FIELD_NAME,
)
from indexes_mapping.inventory.zeebe_enums import ZeebeProcessInstanceFields
from security.security_data_models import UserData
from security.security_factory import security
from services.group_builder.kafka.consumers.group.configs import (
    INVENTORY_GROUP_TYPE,
)
from services.group_builder.models import GroupStatisticUniqueFields
from services.inventory_services.models import (
    InventoryMODefaultFields,
    InventoryMOProcessedFields,
    InventoryMOAdditionalFields,
)
from services.inventory_services.utils.security.filter_by_realm import (
    check_permission_is_admin,
    get_permissions_from_client_role,
    raise_forbidden_ex_if_user_has_no_permission,
    raise_forbidden_exception,
    get_only_available_to_read_tprm_ids_for_special_client,
    get_cleared_filter_columns_with_available_tprm_ids,
    get_cleared_ranges_by_user_permissions,
    get_cleared_sort_columns_with_available_tprm_ids,
    check_availability_of_tmo_data,
)

from v2.routers.inventory.utils.search_by_value_utils import (
    get_query_for_search_by_value_in_tmo_scope,
)
from v2.routers.severity.models import (
    FilterDataInput,
    ResponseSeverityItem,
    Ranges,
    SortColumn,
    Limit,
    Processes,
)
import pandas as pd

from v2.routers.severity.utils import (
    clear_receiving_columns,
    get_group_pm_must_not_conditions,
    get_process_search_args,
)

router = APIRouter(prefix="/severity", tags=["Process Instance indexes"])


@router.post("/by_filters")
async def get_severity_by_filters(
    filters: Annotated[list[FilterDataInput], Body()],
    elastic_client: AsyncElasticsearch = Depends(get_async_client),
    user_data: UserData = Depends(security),
):
    if not filters:
        raise ValueError("Filters not set")

    is_admin = check_permission_is_admin(client_role=user_data.realm_access)
    user_permissions = get_permissions_from_client_role(
        client_role=user_data.realm_access
    )
    if not is_admin:
        raise_forbidden_ex_if_user_has_no_permission(
            client_permissions=user_permissions
        )

    tmo_ids_in_filters = set()
    for filter_item in filters:
        tmo_ids_in_filters.add(filter_item.tmo_id)

    search_conditions = [{"terms": {"id": list(tmo_ids_in_filters)}}]
    if not is_admin:
        search_conditions.append(
            {"terms": {INVENTORY_PERMISSIONS_FIELD_NAME: user_permissions}}
        )
    search_query = {"bool": {"must": search_conditions}}
    search_res = await elastic_client.search(
        index=INVENTORY_TMO_INDEX_V2,
        query=search_query,
        track_total_hits=True,
        size=len(tmo_ids_in_filters),
    )
    existing_tmo_with_severity = dict()

    severity_ids = set()

    for tmo_item in search_res["hits"]["hits"]:
        tmo_item = tmo_item["_source"]

        severity_id = tmo_item.get("severity_id")
        if severity_id:
            tmo_id = tmo_item["id"]
            existing_tmo_with_severity[tmo_id] = tmo_item
            severity_ids.add(severity_id)

    if not severity_ids:
        return [
            {
                "filter_name": filter_item.filter_name,
                "count": 0,
                "max_severity": "undetermined",
            }
            for filter_item in filters
        ]

    # check if severity available for the user
    if not is_admin:
        # TODO : remove comment to enable tprm permissions
        # search_query = {'bool': {'must': [
        #     {'terms': {INVENTORY_PERMISSIONS_FIELD_NAME: user_permissions}},
        #     {'terms': {'id': list(severity_ids)}}
        # ]}}
        #
        # search_res = await elastic_client.search(index=INVENTORY_TPRM_INDEX_V2, query=search_query,
        #                                          track_total_hits=True,
        #                                          size=len(severity_ids))
        # new_existing_tmo_with_severity = dict()
        # severity_ids = set()
        # for tprm_item in search_res['hits']['hits']:
        #     tprm_item = tprm_item['_source']
        #     tprm_id = tprm_item.get('id')
        #     tprm_tmo_id = tprm_item.get('tmo_id')
        #
        #     new_existing_tmo_with_severity[tprm_tmo_id] = existing_tmo_with_severity.get(tprm_tmo_id)
        #     severity_ids.add(tprm_id)
        #
        # existing_tmo_with_severity = new_existing_tmo_with_severity
        pass

    if not severity_ids:
        return [
            {
                "filter_name": filter_item.filter_name,
                "count": 0,
                "max_severity": "undetermined",
            }
            for filter_item in filters
        ]

    # clear filters by existing tmo and severity
    clear_filters = []
    for filter_item in filters:
        if filter_item.tmo_id in existing_tmo_with_severity:
            clear_filters.append(filter_item)

    # get column_names types from filters
    column_names = set()
    for filter_data_input in clear_filters:
        data_input_column_names = {
            filter_column.column_name
            for filter_column in filter_data_input.column_filters
        }
        column_names.update(data_input_column_names)

    # clear filters by available tprm columns
    if not is_admin:
        # TODO : remove comment to enable tprm permissions
        # tprms_columns = {int(column_name) for column_name in column_names if column_name.isdigit()}
        #
        # if tprms_columns:
        #     search_query = {'bool': {'must': [
        #         {'terms': {INVENTORY_PERMISSIONS_FIELD_NAME: user_permissions}},
        #         {'terms': {'id': list(tprms_columns)}}
        #     ]}}
        #
        #     search_res = await elastic_client.search(index=INVENTORY_TPRM_INDEX_V2, query=search_query,
        #                                              track_total_hits=True,
        #                                              size=len(tprms_columns))
        #     available_tprm_columns = {str(item['source']['id']) for item in search_res['hits']['hits']}
        #
        #     if len(available_tprm_columns) != len(tprms_columns):
        #         copy_of_clear_filters = list()
        #         for filter_data_input in clear_filters:
        #             available_filter_columns = list()
        #
        #             for filter_column in filter_data_input.column_filters:
        #                 if not filter_column.column_name.isdigit():
        #                     available_filter_columns.append(filter_column)
        #                 else:
        #                     if filter_column.column_name in available_tprm_columns:
        #                         available_filter_columns.append(filter_column)
        #
        #             if available_filter_columns:
        #                 filter_data_input.column_filters = available_filter_columns
        #                 copy_of_clear_filters.append(filter_data_input)
        #         clear_filters = copy_of_clear_filters
        pass

    # get column types
    dict_of_types = await get_dict_of_inventory_attr_and_params_types(
        array_of_attr_and_params_names=column_names,
        elastic_client=elastic_client,
    )
    # get aggregations
    aggregations = {}
    direction_severity = {"asc": "max", "desc": "min"}
    for filter_data_input in clear_filters:
        tmo_severity_id = existing_tmo_with_severity.get(
            filter_data_input.tmo_id
        )["severity_id"]

        filter_direction = direction_severity.get(
            filter_data_input.severity_direction, "asc"
        )

        must_filter_condition = [
            {"match": {"tmo_id": filter_data_input.tmo_id}}
        ]
        if not is_admin:
            must_filter_condition.append(
                {"terms": {INVENTORY_PERMISSIONS_FIELD_NAME: user_permissions}}
            )

        must_not_filter_condition = list()

        # exclude inventory groups
        must_not_filter_condition.extend(
            [
                {"exists": {"field": InventoryMOAdditionalFields.GROUPS.value}},
                {
                    "match": {
                        GroupStatisticUniqueFields.GROUP_TYPE.value: INVENTORY_GROUP_TYPE
                    }
                },
            ]
        )

        filter_cond = get_search_query_for_inventory_obj_index(
            filter_columns=filter_data_input.column_filters,
            dict_of_types=dict_of_types,
        )

        if filter_cond:
            bool_cond = filter_cond.get("bool")
            if bool_cond:
                must_cond = bool_cond.get("must")
                must_not_cond = bool_cond.get("must_not")
                if must_cond:
                    must_filter_condition.extend(must_cond)
                if must_not_cond:
                    must_not_filter_condition.extend(must_not_cond)

        main_filter_cond = dict()
        if must_filter_condition:
            main_filter_cond["must"] = must_filter_condition
        if must_not_filter_condition:
            main_filter_cond["must_not"] = must_not_filter_condition

        main_filter_cond = {"bool": main_filter_cond}

        aggregations[filter_data_input.filter_name] = {
            "filter": main_filter_cond,
            "aggs": {
                "agg_val": {
                    f"{filter_direction}": {
                        "field": f"{INVENTORY_PARAMETERS_FIELD_NAME}.{tmo_severity_id}"
                    }
                }
            },
        }

    # main query

    main_query = {"terms": {"tmo_id": list(existing_tmo_with_severity)}}

    search_res = await elastic_client.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=main_query,
        aggregations=aggregations,
        size=0,
    )

    resp = []

    aggregations_data = search_res["aggregations"]

    for filter_item in filters:
        agr_data_for_filter = aggregations_data.get(filter_item.filter_name)

        if not agr_data_for_filter:
            data = {
                "filter_name": filter_item.filter_name,
                "count": 0,
                "max_severity": "undetermined",
            }
        else:
            max_severity = agr_data_for_filter["agg_val"].get("value")
            if not max_severity:
                max_severity = 0
            data = {
                "filter_name": filter_item.filter_name,
                "count": agr_data_for_filter["doc_count"],
                "max_severity": max_severity,
            }
        resp.append(data)

    return resp


@router.post("/by_ranges", response_model=list[ResponseSeverityItem])
async def get_severity_by_ranges(
    tmo_id: Annotated[int, Body(alias="tmoId")],
    ranges_object: Annotated[Ranges, Body(alias="rangesObject")],
    filters_list: Annotated[
        list[FilterColumn] | None, Body(alias="columnFilters")
    ] = None,
    find_by_value: Annotated[str | None, Body(alias="findByValue")] = None,
    elastic_client: AsyncElasticsearch = Depends(get_async_client),
    user_data: UserData = Depends(security),
):
    is_admin = check_permission_is_admin(client_role=user_data.realm_access)
    user_permissions = get_permissions_from_client_role(
        client_role=user_data.realm_access
    )
    if not is_admin:
        raise_forbidden_ex_if_user_has_no_permission(
            client_permissions=user_permissions
        )

    mo_index_for_tmo_id = get_index_name_by_tmo(tmo_id)

    # check if id exists
    search_conditions = [{"match": {"id": tmo_id}}]
    if not is_admin:
        search_conditions.append(
            {"terms": {INVENTORY_PERMISSIONS_FIELD_NAME: user_permissions}}
        )
    search_query = {"bool": {"must": search_conditions}}
    search_res = await elastic_client.search(
        index=INVENTORY_TMO_INDEX_V2,
        query=search_query,
        size=1,
        track_total_hits=True,
        ignore_unavailable=True,
    )
    search_res = search_res["hits"]["hits"]

    if not search_res:
        raise HTTPException(
            status_code=404, detail=f"TMO with id = {tmo_id} does not exist"
        )

    severity_id = search_res[0]["_source"].get("severity_id")
    if not severity_id:
        raise HTTPException(
            status_code=404,
            detail=f"TMO with id = {tmo_id} does not have severity_id",
        )

    if not is_admin:
        # TODO : remove comment to enable tprm permissions
        # search_query = {"bool": {"must": [
        #     {"match": {"id": severity_id}},
        #     {'terms': {INVENTORY_PERMISSIONS_FIELD_NAME: user_permissions}}
        # ]}}
        # search_res = await elastic_client.search(index=INVENTORY_TPRM_INDEX_V2, query=search_query, size=1,
        #                                          track_total_hits=True,
        #                                          ignore_unavailable=True)
        # if not search_res['hits']['hits']:
        #     raise_forbidden_exception()
        #
        # # get all available tprm_ids fot special user
        # available_tprm_ids = await get_only_available_to_read_tprm_ids_for_special_client(
        #     client_permissions=user_permissions,
        #     elastic_client=elastic_client,
        #     tmo_ids=[tmo_id])
        # set_of_available_tprm_ids = set(available_tprm_ids)
        #
        # # clear filters_list
        # if filters_list:
        #     filters_list = get_cleared_filter_columns_with_available_tprm_ids(
        #         filter_columns=filters_list,
        #         available_tprm_ids=set_of_available_tprm_ids)
        #
        # # clear ranges_object
        # if ranges_object:
        #     cleared_ranges_object = get_cleared_ranges_by_user_permissions(
        #         ranges_object=ranges_object,
        #         available_tprm_ids=set_of_available_tprm_ids)
        #
        #     if cleared_ranges_object:
        #         ranges_object = cleared_ranges_object
        #
        #     else:
        #         resp = [{'filter_name': range_name, 'count': 'undetermined', 'max_severity': 'undetermined'}
        #                 for range_name in ranges_object.ranges.keys()]
        #         return resp
        pass

    # get column_names types from ranges_objects and filters_list
    column_names = set()

    if ranges_object:
        for filter_name, filters_as_fr_set in ranges_object.ranges.items():
            list_of_filters = list()
            severity_founded = False
            for filter_column in filters_as_fr_set:
                if filter_column.column_name == "severity":
                    severity_founded = True
                    copy_of_filter_column = FilterColumn(
                        columnName=str(severity_id),
                        rule=filter_column.rule,
                        filters=filter_column.filters,
                    )
                    list_of_filters.append(copy_of_filter_column)
                    column_names.add(str(severity_id))
                else:
                    list_of_filters.append(filter_column)
                    column_names.add(filter_column.column_name)
            if severity_founded:
                ranges_object.ranges[filter_name] = frozenset(list_of_filters)

    if filters_list:
        column_names.update(
            {filter_column.column_name for filter_column in filters_list}
        )

    dict_of_types = await get_dict_of_inventory_attr_and_params_types(
        array_of_attr_and_params_names=column_names,
        elastic_client=elastic_client,
    )
    # get filter column query
    filter_column_query = dict()
    if filters_list:
        filter_column_query = get_search_query_for_inventory_obj_index(
            filter_columns=filters_list, dict_of_types=dict_of_types
        )

    # get find_by_value search query
    by_value_search_query = dict()
    if find_by_value:
        if is_admin:
            by_value_search_query = (
                await get_query_for_search_by_value_in_tmo_scope(
                    elastic_client, tmo_ids=[tmo_id], search_value=find_by_value
                )
            )
        else:
            by_value_search_query = (
                await get_query_for_search_by_value_in_tmo_scope(
                    elastic_client,
                    tmo_ids=[tmo_id],
                    search_value=find_by_value,
                    client_permissions=user_permissions,
                )
            )

    aggregations = {}
    direction_severity = {"asc": "max", "desc": "min"}
    ranges_severity_direction = direction_severity.get(
        ranges_object.severity_direction, "max"
    )
    for range_name, filter_columns in ranges_object.ranges.items():
        filter_cond = get_search_query_for_inventory_obj_index(
            filter_columns=filter_columns, dict_of_types=dict_of_types
        )

        aggregations[range_name] = {
            "filter": filter_cond,
            "aggs": {
                "agg_val": {
                    f"{ranges_severity_direction}": {
                        "field": f"{INVENTORY_PARAMETERS_FIELD_NAME}.{severity_id}"
                    }
                }
            },
        }

    # main search query start
    main_must_conditions = [{"match": {"tmo_id": tmo_id}}]
    main_must_not_conditions = list()

    # exclude inventory groups
    main_must_not_conditions.extend(
        [
            {"exists": {"field": InventoryMOAdditionalFields.GROUPS.value}},
            {
                "match": {
                    GroupStatisticUniqueFields.GROUP_TYPE.value: INVENTORY_GROUP_TYPE
                }
            },
        ]
    )

    if filter_column_query:
        filter_must_cond = filter_column_query["bool"].get("must")
        filter_must_not_cond = filter_column_query["bool"].get("must_not")

        if filter_must_cond:
            main_must_conditions.extend(filter_must_cond)

        if filter_must_not_cond:
            main_must_not_conditions.extend(filter_must_not_cond)

    if by_value_search_query:
        filter_must_cond = by_value_search_query["bool"].get("must")
        filter_must_not_cond = by_value_search_query["bool"].get("must_not")

        if filter_must_cond:
            main_must_conditions.extend(filter_must_cond)

        if filter_must_not_cond:
            main_must_not_conditions.extend(filter_must_not_cond)

    search_conditions = dict()
    if main_must_conditions:
        search_conditions["must"] = main_must_conditions

    if main_must_not_conditions:
        search_conditions["must_not"] = main_must_not_conditions

    search_query = {"bool": search_conditions}

    search_res = await elastic_client.search(
        index=mo_index_for_tmo_id,
        query=search_query,
        aggregations=aggregations,
        size=0,
    )

    resp = []
    for aggr_name, agg_data in search_res["aggregations"].items():
        max_severity = agg_data["agg_val"]["value"]
        if not max_severity:
            max_severity = 0
        data = {
            "filter_name": aggr_name,
            "count": agg_data["doc_count"],
            "max_severity": max_severity,
        }
        resp.append(data)

    return resp


@router.post("/processes", response_model=Processes)
async def get_processes(
    tmo_id: Annotated[int | None, Body(alias="tmoId")],
    find_by_value: Annotated[str | None, Body(alias="findByValue")] = None,
    filters_list: Annotated[
        list[FilterColumn] | None, Body(alias="columnFilters")
    ] = None,
    with_groups: Annotated[bool | None, Body(alias="withGroups")] = True,
    ranges_object: Annotated[Ranges | None, Body(alias="rangesObject")] = None,
    sort: Annotated[list[SortColumn] | None, Body()] = None,
    limit: Annotated[Limit, Body()] = Limit(),
    elastic_client: AsyncElasticsearch = Depends(get_async_client),
    user_data: UserData = Depends(security),
):
    search_args: dict = await get_process_search_args(
        user_data=user_data,
        tmo_id=tmo_id,
        elastic_client=elastic_client,
        ranges_object=ranges_object,
        filters_list=filters_list,
        find_by_value=find_by_value,
        with_groups=with_groups,
        sort=sort,
        limit=limit,
        group_by=None,
    )
    if not search_args.get("query", None):
        return {"rows": [], "totalCount": 0}

    search_res = await elastic_client.search(**search_args)
    rows = list()
    for item in search_res["hits"]["hits"]:
        item = item["_source"]
        params = item.get(INVENTORY_PARAMETERS_FIELD_NAME)
        if params:
            item.update(params)
            del item[INVENTORY_PARAMETERS_FIELD_NAME]
        rows.append(item)

    resp = {"rows": rows, "totalCount": search_res["hits"]["total"]["value"]}
    return resp


@router.post("/export")
async def export_processes(
    tmo_id: Annotated[int, Body()],
    find_by_value: Annotated[str | None, Body()] = None,
    filters_list: Annotated[list[FilterColumn] | None, Body()] = None,
    with_groups: Annotated[bool | None, Body()] = True,
    ranges_object: Annotated[Ranges | None, Body()] = None,
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

    severity_id = tmo_data.get("severity_id")
    if not severity_id:
        raise HTTPException(
            status_code=404,
            detail=f"TMO with id = {tmo_id} does not have severity_id",
        )

    if not is_admin:
        search_query = {
            "bool": {
                "must": [
                    {"match": {"id": severity_id}},
                    {
                        "terms": {
                            INVENTORY_PERMISSIONS_FIELD_NAME: user_permissions
                        }
                    },
                ]
            }
        }
        search_res = await elastic_client.search(
            index=INVENTORY_TPRM_INDEX_V2,
            query=search_query,
            size=1,
            track_total_hits=True,
            ignore_unavailable=True,
        )
        if not search_res["hits"]["hits"]:
            raise_forbidden_exception()

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

        # clear ranges_object
        if ranges_object:
            ranges_object = get_cleared_ranges_by_user_permissions(
                ranges_object=ranges_object,
                available_tprm_ids=set_of_available_tprm_ids,
            )

        # clear sort
        if sort:
            sort = get_cleared_sort_columns_with_available_tprm_ids(
                sort_columns=sort, available_tprm_ids=set_of_available_tprm_ids
            )

    # get column_names types from ranges_objects and filters_list
    column_names = set()

    if ranges_object:
        for filter_name, filters_as_fr_set in ranges_object.ranges.items():
            list_of_filters = list()
            severity_founded = False
            for filter_column in filters_as_fr_set:
                if filter_column.column_name == "severity":
                    severity_founded = True
                    copy_of_filter_column = FilterColumn(
                        columnName=str(severity_id),
                        rule=filter_column.rule,
                        filters=filter_column.filters,
                    )
                    list_of_filters.append(copy_of_filter_column)
                    column_names.add(str(severity_id))
                else:
                    list_of_filters.append(filter_column)
                    column_names.add(filter_column.column_name)
            if severity_founded:
                ranges_object.ranges[filter_name] = frozenset(list_of_filters)

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

    # get ranges query
    rangers_should_cond = list()
    if ranges_object:
        for range_name, filter_columns in ranges_object.ranges.items():
            range_cond = get_search_query_for_inventory_obj_index(
                filter_columns=filter_columns, dict_of_types=dict_of_types
            )
            if range_cond:
                rangers_should_cond.append(range_cond)

    # get group condition query
    group_condition_must_not = get_group_pm_must_not_conditions(with_groups)

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

    if rangers_should_cond:
        if main_query:
            main_bool = main_query["bool"]
            if isinstance(main_bool, dict):
                main_bool["filter"] = {
                    "bool": {
                        "minimum_should_match": 1,
                        "should": rangers_should_cond,
                    }
                }
        else:
            main_query = {
                "bool": {
                    "filter": {
                        "bool": {
                            "minimum_should_match": 1,
                            "should": rangers_should_cond,
                        }
                    }
                }
            }

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
