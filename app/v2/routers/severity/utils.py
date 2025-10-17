from typing import List

from elasticsearch import AsyncElasticsearch
from fastapi import HTTPException

from elastic.config import INVENTORY_TPRM_INDEX_V2, INVENTORY_TMO_INDEX_V2
from elastic.pydantic_models import FilterColumn, SortColumn
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
from services.group_builder.kafka.consumers.group.configs import (
    INVENTORY_GROUP_TYPE,
    PM_GROUP_TYPE,
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
)
from v2.routers.inventory.utils.search_by_value_utils import (
    get_query_for_search_by_value_in_tmo_scope,
)
from v2.routers.severity.models import Ranges, Limit


async def clear_receiving_columns(
    tmo_id: int,
    elastic_client: AsyncElasticsearch,
    columns: List[str] = None,
    user_permissions: list[str] = None,
) -> dict:
    """Returns cleared columns with respecting the input queue and .
    Result contains only:
     existing mo default attrs names
     existing mo processed attrs names
     existing tprms paths - 'Parameter.tprm_id'"""

    get_ordered_fields = []
    data_of_included_tprms = dict()

    if not columns:
        mo_default_attrs = [x.value for x in InventoryMODefaultFields]
        mo_processed_attrs = [x.value for x in InventoryMOProcessedFields]
        mo_camunda_fields = [x.value for x in ZeebeProcessInstanceFields]

        returnable_tprms_search_conditions = [
            {"match": {"tmo_id": tmo_id}},
            {"match": {"returnable": True}},
        ]
        if user_permissions:
            # TODO : remove comment to enable tprm permissions
            # returnable_tprms_search_conditions.append({"terms": {INVENTORY_PERMISSIONS_FIELD_NAME: user_permissions}})
            pass

        returnable_tprms_search_query = {
            "bool": {"must": returnable_tprms_search_conditions}
        }

        search_res = await elastic_client.search(
            index=INVENTORY_TPRM_INDEX_V2,
            query=returnable_tprms_search_query,
            size=10000,
        )
        included_tprms = []
        for tprm_data in search_res["hits"]["hits"]:
            tprm_data = tprm_data["_source"]
            included_tprms.append(
                f"{INVENTORY_PARAMETERS_FIELD_NAME}.{tprm_data['id']}"
            )

            data_of_included_tprms[tprm_data["id"]] = tprm_data

        get_ordered_fields = (
            mo_default_attrs
            + mo_processed_attrs
            + mo_camunda_fields
            + included_tprms
        )

    else:
        mo_attrs_from_column = set()
        tprm_ids_from_column = set()
        cleaned_mo_attrs = set()
        cleaned_tprm_ids_as_str = set()

        for column in columns:
            if column.isdigit():
                tprm_ids_from_column.add(column)
            else:
                mo_attrs_from_column.add(column)

        if mo_attrs_from_column:
            mo_default_attrs_set = {x.value for x in InventoryMODefaultFields}
            mo_processed_attrs_set = {
                x.value for x in InventoryMOProcessedFields
            }
            mo_camunda_attrs_set = {x.value for x in ZeebeProcessInstanceFields}

            existing_mo_attrs = set().union(
                mo_default_attrs_set,
                mo_processed_attrs_set,
                mo_camunda_attrs_set,
            )

            cleaned_mo_attrs.update(
                existing_mo_attrs.intersection(mo_attrs_from_column)
            )

        if tprm_ids_from_column:
            search_conditions = [
                {"match": {"tmo_id": tmo_id}},
                {"terms": {"id": list(tprm_ids_from_column)}},
            ]
            if user_permissions:
                # TODO : remove comment to enable tprm permissions
                # search_conditions.append(
                #     {"terms": {INVENTORY_PERMISSIONS_FIELD_NAME: user_permissions}})
                pass

            search_query = {"bool": {"must": search_conditions}}

            search_res = await elastic_client.search(
                index=INVENTORY_TPRM_INDEX_V2, query=search_query, size=10000
            )

            for tprm_data in search_res["hits"]["hits"]:
                tprm_data = tprm_data["_source"]
                cleaned_tprm_ids_as_str.add(str(tprm_data["id"]))

                data_of_included_tprms[tprm_data["id"]] = tprm_data

        for column_from_order in columns:
            if column_from_order in cleaned_mo_attrs:
                get_ordered_fields.append(column_from_order)
            elif column_from_order in cleaned_tprm_ids_as_str:
                get_ordered_fields.append(f"{column_from_order}")

    return {
        "cleared_ordered_columns": get_ordered_fields,
        "data_of_included_tprms": data_of_included_tprms,
    }


def get_group_must_not_conditions(with_groups: bool) -> list:
    """Returns list of must_not query conditions in different cases for with_groups values"""

    if with_groups:
        group_condition_must_not = [
            {"exists": {"field": InventoryMOAdditionalFields.GROUPS.value}}
        ]
    else:
        group_condition_must_not = [
            {"exists": {"field": GroupStatisticUniqueFields.GROUP_NAME.value}}
        ]

    return group_condition_must_not


def get_group_pm_must_not_conditions(with_groups: bool) -> list:
    """Returns list of must_not query conditions in different cases for with_groups values"""

    if with_groups:
        group_condition_must_not = [
            {"exists": {"field": InventoryMOAdditionalFields.GROUPS.value}},
            {
                "match": {
                    GroupStatisticUniqueFields.GROUP_TYPE.value: INVENTORY_GROUP_TYPE
                }
            },
        ]
    else:
        group_condition_must_not = [
            {"exists": {"field": GroupStatisticUniqueFields.GROUP_NAME.value}}
        ]

    return group_condition_must_not


def get_group_inventory_must_not_conditions(with_groups: bool) -> list:
    """Returns list of must_not query conditions in different cases for with_groups values"""

    if with_groups:
        group_condition_must_not = [
            {"exists": {"field": InventoryMOAdditionalFields.GROUPS.value}},
            {
                "match": {
                    GroupStatisticUniqueFields.GROUP_TYPE.value: PM_GROUP_TYPE
                }
            },
        ]
    else:
        group_condition_must_not = [
            {"exists": {"field": GroupStatisticUniqueFields.GROUP_NAME.value}}
        ]

    return group_condition_must_not


async def get_process_search_args(
    user_data: UserData,
    tmo_id: int | None,
    elastic_client: AsyncElasticsearch,
    ranges_object: Ranges | None,
    filters_list: list[FilterColumn] | None,
    sort: list[SortColumn],
    limit: Limit,
    find_by_value: str | None,
    with_groups: bool,
    group_by: list[str | int] | None,
    min_group_qty: int = 1,
) -> dict:
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
        #     ranges_object = get_cleared_ranges_by_user_permissions(
        #         ranges_object=ranges_object,
        #         available_tprm_ids=set_of_available_tprm_ids)
        #
        # # clear sort
        # if sort:
        #     sort = get_cleared_sort_columns_with_available_tprm_ids(sort_columns=sort,
        #                                                             available_tprm_ids=set_of_available_tprm_ids)
        # if group_by:
        #     group_by = get_cleared_group_by_columns_with_available_tprm_ids(
        #         group_by=group_by, available_tprm_ids=set_of_available_tprm_ids)
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

    if sort:
        column_names.update(sort_column.column_name for sort_column in sort)
    # default sorting for search_after
    else:
        column_names.update("sort")

    dict_of_types = await get_dict_of_inventory_attr_and_params_types(
        array_of_attr_and_params_names=column_names,
        elastic_client=elastic_client,
    )

    if group_by:
        column_names.update(group_by)

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

    # get main query
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
    else:
        sort_cond.append({"id": {"order": "asc"}})

    group_cond = None
    if group_by:
        # put the group together in reverse order and get a matryoshka doll
        for group_item in group_by[::-1]:
            column_name = (
                f"{INVENTORY_PARAMETERS_FIELD_NAME}.{group_item}"
                if isinstance(group_item, int)
                else group_item
            )
            group_name = (
                group_item if isinstance(group_item, str) else str(group_item)
            )
            data: dict = {
                group_name: {
                    "terms": {
                        "field": column_name,
                        "size": 1000,
                        "min_doc_count": min_group_qty,
                    }
                }
            }
            if group_cond:
                data[group_name]["aggs"] = group_cond
            group_cond = data

    search_args = {
        "index": mo_index_for_tmo_id,
        "query": main_query,
        "size": limit.limit,
        "from_": limit.offset,
        "track_total_hits": True,
        "_source_excludes": [INVENTORY_PERMISSIONS_FIELD_NAME],
    }

    if sort_cond:
        search_args["sort"] = sort_cond

    if group_cond:
        search_args["aggs"] = group_cond

    print(search_args)

    return search_args
