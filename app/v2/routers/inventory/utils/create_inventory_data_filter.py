import dataclasses

from elasticsearch import AsyncElasticsearch

from elastic.pydantic_models import FilterColumn, SortColumn
from elastic.query_builder_service.inventory_index.mo_object.utils import (
    get_dict_of_inventory_attr_and_params_types,
    get_sort_query_for_inventory_obj_index,
    get_search_query_for_inventory_obj_index,
)
from elastic.query_builder_service.inventory_index.utils.index_utils import (
    get_index_name_by_tmo,
)
from indexes_mapping.inventory.mapping import (
    INVENTORY_PERMISSIONS_FIELD_NAME,
    INVENTORY_PARAMETERS_FIELD_NAME,
)
from security.security_data_models import UserData
from services.inventory_services.models import InventoryMODefaultFields
from services.inventory_services.utils.security.filter_by_realm import (
    check_permission_is_admin,
    get_permissions_from_client_role,
    raise_forbidden_ex_if_user_has_no_permission,
    raise_forbidden_ex_if_user_has_no_permission_to_special_tmo,
    get_only_available_to_read_tprm_ids_for_special_client,
    get_cleared_filter_columns_with_available_tprm_ids,
    get_cleared_sort_columns_with_available_tprm_ids,
    get_post_filter_condition_according_user_permissions,
)
from v2.routers.inventory.utils.search_by_value_utils import (
    get_query_for_search_by_value_in_tmo_scope,
)
from v2.routers.severity.utils import get_group_inventory_must_not_conditions


@dataclasses.dataclass
class InventoryDataFilter:
    search_index: str
    body: dict


async def create_inventory_data_filter(
    user_data: UserData,
    elastic_client: AsyncElasticsearch,
    tmo_id: int,
    filter_columns: list[FilterColumn] | None,
    sort_by: list[SortColumn] | None,
    search_by_value: str | None,
    with_groups: bool | None,
    limit: int,
    offset: int,
) -> InventoryDataFilter:
    # main security checks start

    set_of_available_tprm_ids = set()
    is_admin = check_permission_is_admin(client_role=user_data.realm_access)
    user_permissions = get_permissions_from_client_role(
        client_role=user_data.realm_access
    )
    if not is_admin:
        raise_forbidden_ex_if_user_has_no_permission(
            client_permissions=user_permissions
        )

        await raise_forbidden_ex_if_user_has_no_permission_to_special_tmo(
            client_permissions=user_permissions,
            elastic_client=elastic_client,
            tmo_id=tmo_id,
        )

        available_tprm_ids = (
            await get_only_available_to_read_tprm_ids_for_special_client(
                client_permissions=user_permissions,
                elastic_client=elastic_client,
                tmo_ids=[tmo_id],
            )
        )

        set_of_available_tprm_ids = set(available_tprm_ids)

        if filter_columns:
            filter_columns = get_cleared_filter_columns_with_available_tprm_ids(
                filter_columns=filter_columns,
                available_tprm_ids=set_of_available_tprm_ids,
            )

        if sort_by:
            sort_by = get_cleared_sort_columns_with_available_tprm_ids(
                sort_columns=sort_by,
                available_tprm_ids=set_of_available_tprm_ids,
            )

    # main security checks end
    column_names = set()

    if filter_columns:
        column_names.update(
            {filter_column.column_name for filter_column in filter_columns}
        )

    if sort_by:
        column_names.update(
            {sort_column.column_name for sort_column in sort_by}
        )

    dict_of_types = await get_dict_of_inventory_attr_and_params_types(
        array_of_attr_and_params_names=column_names,
        elastic_client=elastic_client,
    )
    # sort query start
    sort_query = []
    if sort_by:
        sort_query = get_sort_query_for_inventory_obj_index(
            sort_columns=sort_by, dict_of_types=dict_of_types
        )

    # sort query end

    # get filter column query
    filters_must_cond = list()
    filters_must_not_cond = list()
    if filter_columns:
        filter_column_query = get_search_query_for_inventory_obj_index(
            filter_columns=filter_columns, dict_of_types=dict_of_types
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
    if search_by_value:
        if is_admin:
            by_value_search_query = (
                await get_query_for_search_by_value_in_tmo_scope(
                    elastic_client,
                    tmo_ids=[tmo_id],
                    search_value=search_by_value,
                )
            )
        else:
            by_value_search_query = (
                await get_query_for_search_by_value_in_tmo_scope(
                    elastic_client,
                    tmo_ids=[tmo_id],
                    search_value=search_by_value,
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

    # get with_groups query condition
    group_condition_must_not = get_group_inventory_must_not_conditions(
        with_groups=with_groups
    )

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

    search_index = get_index_name_by_tmo(tmo_id)

    print(main_query)

    body = {
        "query": main_query,
        "_source": {"excludes": [INVENTORY_PERMISSIONS_FIELD_NAME]},
        "track_total_hits": True,
    }
    if sort_query:
        body["sort"] = sort_query

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
        source_includes.extend(
            [
                f"{INVENTORY_PARAMETERS_FIELD_NAME}.{tprm_id}"
                for tprm_id in set_of_available_tprm_ids
            ]
        )
        body["fields"] = source_includes

    return InventoryDataFilter(search_index=search_index, body=body)
