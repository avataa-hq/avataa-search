from typing import List, Iterable

from elasticsearch import AsyncElasticsearch
from fastapi import HTTPException

from elastic.config import INVENTORY_TPRM_INDEX_V2, INVENTORY_TMO_INDEX_V2
from elastic.pydantic_models import FilterColumn, SortColumn
from indexes_mapping.inventory.mapping import INVENTORY_PERMISSIONS_FIELD_NAME
from security.security_config import ADMIN_ROLE
from security.security_data_models import ClientRoles
from services.inventory_services.elastic.security.configs import (
    INVENTORY_SECURITY_TMO_PERMISSION_INDEX,
    INVENTORY_SECURITY_MO_PERMISSION_INDEX,
)
from services.inventory_services.utils.security.pydantic_models import (
    AvailableTMOdata,
)
from v2.routers.severity.models import Ranges


def check_permission_is_admin(client_role: ClientRoles = None) -> bool:
    """Returns True if client has admin permissions"""
    if not client_role:
        return False
    user_roles = set(client_role.roles)
    if ADMIN_ROLE in user_roles:
        return True
    return False


def get_permissions_from_client_role(
    client_role: ClientRoles = None,
) -> List[str]:
    """Returns a list of permissions as they store in elasticsearch"""
    if not client_role:
        return list()
    permissions_to_search = [
        f"{client_role.name}.{role}" for role in client_role.roles
    ]
    return permissions_to_search


def raise_forbidden_exception():
    raise HTTPException(status_code=403, detail="Forbidden. No permissions")


def raise_forbidden_ex_if_user_has_no_permission(client_permissions: List[str]):
    """Raises exception if user has no permissions"""
    if not client_permissions:
        raise_forbidden_exception()


def get_post_filter_condition_according_user_permissions(
    client_permissions: List[str],
) -> dict:
    """Returns post filter conditions according user permissions"""
    return {"terms": {INVENTORY_PERMISSIONS_FIELD_NAME: client_permissions}}


async def check_permission_for_special_tmo(
    client_permissions: List[str],
    elastic_client: AsyncElasticsearch,
    tmo_id: int,
) -> bool:
    """Returns True if client has permission for this tmo_id"""
    if not client_permissions:
        return False

    search_query = {
        "bool": {
            "must": [
                {"match": {"parent_id": tmo_id}},
                {"terms": {"permission": client_permissions}},
                {"match": {"read": True}},
            ]
        }
    }
    body = {"query": search_query, "size": 0, "track_total_hits": True}

    res = await elastic_client.search(
        index=INVENTORY_SECURITY_TMO_PERMISSION_INDEX,
        body=body,
        ignore_unavailable=True,
    )

    if res["hits"]["total"]["value"] > 0:
        return True

    return False


async def raise_forbidden_ex_if_user_has_no_permission_to_special_tmo(
    client_permissions: List[str],
    elastic_client: AsyncElasticsearch,
    tmo_id: int,
):
    """Raises exception if user has no permissions to special tmo"""
    if not await check_permission_for_special_tmo(
        client_permissions=client_permissions,
        elastic_client=elastic_client,
        tmo_id=tmo_id,
    ):
        raise_forbidden_exception()


async def check_permission_for_special_mo(
    client_permissions: List[str],
    elastic_client: AsyncElasticsearch,
    mo_id: int,
) -> bool:
    """Returns True if client has permission for this tmo_id"""
    if not client_permissions:
        return False

    search_query = {
        "bool": {
            "must": [
                {"match": {"parent_id": mo_id}},
                {
                    "terms": {
                        INVENTORY_PERMISSIONS_FIELD_NAME: client_permissions
                    }
                },
                {"match": {"read": True}},
            ]
        }
    }
    body = {"query": search_query, "size": 0, "track_total_hits": True}

    res = await elastic_client.search(
        index=INVENTORY_SECURITY_MO_PERMISSION_INDEX,
        body=body,
        ignore_unavailable=True,
    )

    if res["hits"]["total"]["value"] > 0:
        return True

    return False


async def raise_forbidden_ex_if_user_has_no_permission_to_special_mo(
    client_permissions: List[str],
    elastic_client: AsyncElasticsearch,
    mo_id: int,
):
    """Raises exception if user has no permissions to special mo"""
    if not await check_permission_for_special_mo(
        client_permissions=client_permissions,
        elastic_client=elastic_client,
        mo_id=mo_id,
    ):
        raise_forbidden_exception()


async def get_only_available_to_read_tmo_ids_for_special_client(
    client_permissions: List[str],
    elastic_client: AsyncElasticsearch,
    tmo_ids: List[int] = None,
) -> List[int]:
    """Returns a list containing only the available tmo ids that the client has read access to.
    If tmo_ids is not None - limits the results to the scope of these tmo_ids"""
    SIZE_PER_STEP = 10000

    if not client_permissions:
        return list()

    search_conditions = [
        {"terms": {"permission": client_permissions}},
        {"match": {"read": True}},
    ]
    if tmo_ids:
        search_conditions.append({"terms": {"parent_id": tmo_ids}})

    search_query = {"bool": {"must": search_conditions}}
    sort_cond = {"id": {"order": "asc"}}

    body = {
        "query": search_query,
        "size": SIZE_PER_STEP,
        "sort": sort_cond,
        "track_total_hits": True,
        "_source": {"includes": ["parent_id"]},
    }

    available_tmo_ids = list()
    while True:
        search_res = await elastic_client.search(
            index=INVENTORY_SECURITY_TMO_PERMISSION_INDEX,
            body=body,
            ignore_unavailable=True,
        )

        total_hits = search_res["hits"]["total"]["value"]
        search_res = search_res["hits"]["hits"]
        tmo_ids = [item["_source"]["parent_id"] for item in search_res]
        available_tmo_ids.extend(tmo_ids)

        if total_hits < SIZE_PER_STEP:
            break

        if len(tmo_ids) == SIZE_PER_STEP:
            search_after = search_res[-1]["sort"]
            body["search_after"] = search_after
        else:
            break

    return available_tmo_ids


async def get_only_available_to_read_tprm_ids_for_special_client(
    client_permissions: List[str],
    elastic_client: AsyncElasticsearch,
    tmo_ids: List[int] = None,
) -> List[int]:
    """Returns a list containing only the available tprm ids that the client has read access to.
    If tmo ids is not None - limits the results to the scope of these tmo_ids"""
    SIZE_PER_STEP = 10000

    if not client_permissions:
        return list()

    search_conditions = list()
    # TODO : remove comment to enable tprm permissions
    search_conditions.append({"match_all": {}})
    # search_conditions.append({"terms": {INVENTORY_PERMISSIONS_FIELD_NAME: client_permissions}})
    if tmo_ids:
        search_conditions.append({"terms": {"tmo_id": tmo_ids}})

    search_query = {"bool": {"must": search_conditions}}
    sort_cond = {"id": {"order": "asc"}}

    body = {
        "query": search_query,
        "size": SIZE_PER_STEP,
        "sort": sort_cond,
        "track_total_hits": True,
        "_source": {"includes": ["id"]},
    }

    available_tprm_ids = list()
    while True:
        search_res = await elastic_client.search(
            index=INVENTORY_TPRM_INDEX_V2, body=body, ignore_unavailable=True
        )

        total_hits = search_res["hits"]["total"]["value"]
        search_res = search_res["hits"]["hits"]

        tprm_ids = [item["_source"]["id"] for item in search_res]

        available_tprm_ids.extend(tprm_ids)

        if total_hits < SIZE_PER_STEP:
            break

        if len(tprm_ids) == SIZE_PER_STEP:
            search_after = search_res[-1]["sort"]
            body["search_after"] = search_after
        else:
            break

    return available_tprm_ids


async def get_all_tprm_ids_of_special_tmo_ids(
    elastic_client: AsyncElasticsearch, tmo_ids: list[int]
) -> List[int]:
    """Returns list of all tprm ids of special tmo"""
    SIZE_PER_STEP = 10000

    search_query = {"terms": {"tmo_id": tmo_ids}}
    sort_cond = {"id": {"order": "asc"}}

    body = {
        "query": search_query,
        "size": SIZE_PER_STEP,
        "sort": sort_cond,
        "track_total_hits": True,
        "_source": {"includes": ["id"]},
    }

    available_tprm_ids = list()
    while True:
        search_res = await elastic_client.search(
            index=INVENTORY_TPRM_INDEX_V2, body=body, ignore_unavailable=True
        )

        total_hits = search_res["hits"]["total"]["value"]
        search_res = search_res["hits"]["hits"]
        tprm_ids = [item["_source"]["id"] for item in search_res]

        available_tprm_ids.extend(tprm_ids)

        if total_hits < SIZE_PER_STEP:
            break

        if len(tprm_ids) == SIZE_PER_STEP:
            search_after = search_res[-1]["sort"]
            body["search_after"] = search_after
        else:
            break

    return available_tprm_ids


def get_cleared_filter_columns_with_available_tprm_ids(
    filter_columns: Iterable[FilterColumn], available_tprm_ids: set[int]
) -> List[FilterColumn]:
    """Returns cleared list with filter_columns. Deletes all unavailable filters from filter_columns"""
    res = list()

    for item in filter_columns:
        if not item.column_name.isdigit():
            res.append(item)
        else:
            int_column = int(item.column_name)
            if int_column in available_tprm_ids:
                res.append(item)

    return res


def get_cleared_sort_columns_with_available_tprm_ids(
    sort_columns: list[SortColumn], available_tprm_ids: set[int]
) -> list[SortColumn]:
    """Returns cleared list with SortColumn. Deletes all unavailable columns from sort_columns"""
    res = list()

    for item in sort_columns:
        if not item.column_name.isdigit():
            res.append(item)
        else:
            int_column = int(item.column_name)
            if int_column in available_tprm_ids:
                res.append(item)

    return res


def get_cleared_group_by_columns_with_available_tprm_ids(
    group_by: list[int | str], available_tprm_ids: set[int]
) -> list[int | str]:
    res = list()

    for item in group_by:
        # convert
        if isinstance(item, str):
            if item.isdigit():
                item = int(item)

        # check
        if isinstance(item, int):
            if item in available_tprm_ids:
                res.append(item)
        else:
            res.append(item)

    return res


def get_cleared_ranges_by_user_permissions(
    ranges_object: Ranges, available_tprm_ids: set[int]
) -> Ranges | None:
    new_ranges = dict()

    for range_name, range_filters in ranges_object.ranges.items():
        cleared_filters = get_cleared_filter_columns_with_available_tprm_ids(
            filter_columns=range_filters, available_tprm_ids=available_tprm_ids
        )
        if cleared_filters:
            new_ranges[range_name] = frozenset(cleared_filters)

    if not new_ranges:
        return None
    return Ranges(
        ranges=new_ranges, severity_direction=ranges_object.severity_direction
    )


async def get_all_tprm_data_of_special_tmo_ids(
    elastic_client: AsyncElasticsearch,
    tmo_ids: list[int],
    is_admin: bool,
    user_permissions: List[str] = None,
    get_only_tprm_attrs: List[str] = None,
) -> dict[int, dict]:
    """Returns list of all tprm ids of special tmo"""
    SIZE_PER_STEP = 10000

    result = dict()

    if not is_admin and not user_permissions:
        return result

    search_conditions = [{"terms": {"tmo_id": tmo_ids}}]

    if not is_admin:
        # TODO : remove comment to enable tprm permissions
        # search_conditions.append({'terms': {INVENTORY_PERMISSIONS_FIELD_NAME: user_permissions}})
        pass

    search_query = {"bool": {"must": search_conditions}}
    sort_cond = {"id": {"order": "asc"}}

    body = {
        "query": search_query,
        "size": SIZE_PER_STEP,
        "sort": sort_cond,
        "track_total_hits": True,
    }

    if get_only_tprm_attrs:
        body["_source"] = {"includes": get_only_tprm_attrs}

    while True:
        search_res = await elastic_client.search(
            index=INVENTORY_TPRM_INDEX_V2, body=body, ignore_unavailable=True
        )

        total_hits = search_res["hits"]["total"]["value"]
        search_res = search_res["hits"]["hits"]

        for tprm_data in search_res:
            tprm_data = tprm_data["_source"]
            tprm_id = tprm_data.get("id")
            if tprm_id:
                result[tprm_id] = tprm_data

        if total_hits < SIZE_PER_STEP:
            break

        if search_res:
            search_after = search_res[-1]["sort"]
            body["search_after"] = search_after
        else:
            break

    return result


async def check_availability_of_tmo_data(
    elastic_client: AsyncElasticsearch,
    is_admin: bool,
    tmo_id: int,
    user_permissions: List[str] = None,
) -> AvailableTMOdata:
    """Checks if tmo_data is available, and returns AvailableTMOdata"""
    result = AvailableTMOdata(data_available=False)

    if not is_admin and not user_permissions:
        return result

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
        return result

    tmo_data = tmo_data[0]["_source"]

    result.data_available = True
    result.tmo_data_as_dict = tmo_data

    available_tprm_data = await get_all_tprm_data_of_special_tmo_ids(
        elastic_client=elastic_client,
        tmo_ids=[tmo_id],
        is_admin=is_admin,
        user_permissions=user_permissions,
        get_only_tprm_attrs=["id", "name"],
    )
    if available_tprm_data:
        result.available_tprm_data = available_tprm_data

    return result
