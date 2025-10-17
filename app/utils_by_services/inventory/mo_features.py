from typing import Any

from elasticsearch import AsyncElasticsearch

from elastic.config import (
    INVENTORY_MO_LINK_INDEX,
    ALL_MO_OBJ_INDEXES_PATTERN,
    INVENTORY_TMO_INDEX_V2,
)
from indexes_mapping.inventory.mapping import INVENTORY_PERMISSIONS_FIELD_NAME
from utils_by_services.inventory.common import search_after


async def get_info_about_tmos(
    elastic_client: AsyncElasticsearch,
    is_admin: bool,
    user_permissions: list[str],
    tmo_ids: list[int],
    returned_columns: list[str] | None = None,
    sorting: dict | None = None,
) -> list[Any]:
    body = generate_request_body(
        field_for_request="id",
        ids=tmo_ids,
        tprm_ids=[],
        is_admin=is_admin,
        user_permissions=user_permissions,
        returned_columns=returned_columns,
        sorting=sorting,
    )
    output = await search_after(
        elastic_client=elastic_client, index=INVENTORY_TMO_INDEX_V2, body=body
    )
    return output


async def get_info_mos(
    elastic_client: AsyncElasticsearch,
    is_admin: bool,
    user_permissions: list[str],
    mo_ids: list[int],
    returned_columns: list[str] | None = None,
    sorting: dict | None = None,
) -> list[Any]:
    """Implement request to Elastic ALL_MO_OBJ_INDEXES_PATTERN with correct request and return _source data"""
    body = generate_request_body(
        field_for_request="id",
        ids=mo_ids,
        tprm_ids=[],
        is_admin=is_admin,
        user_permissions=user_permissions,
        returned_columns=returned_columns,
        sorting=sorting,
    )
    output = await search_after(
        elastic_client=elastic_client,
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        body=body,
    )
    return output


async def get_mo_link_data_by_mo_ids(
    elastic_client: AsyncElasticsearch,
    mo_ids: list[int],
    tprm_ids: list[int],
    is_admin: bool,
    user_permissions: list[str],
    returned_columns: list[str] | None = None,
    sorting: dict | None = None,
) -> list[Any]:
    """Implement request to Elastic INVENTORY_MO_LINK_INDEX with correct request and return _source data.
    _source contains: id(prm_id), mo_id, tprm_id(for mo id), value(linked mo id), version
    """
    body = generate_request_body(
        field_for_request="mo_id",
        ids=mo_ids,
        tprm_ids=tprm_ids,
        is_admin=is_admin,
        user_permissions=user_permissions,
        returned_columns=returned_columns,
        sorting=sorting,
    )
    output = await search_after(
        elastic_client=elastic_client, index=INVENTORY_MO_LINK_INDEX, body=body
    )
    return output


async def get_mo_link_data_by_value(
    elastic_client: AsyncElasticsearch,
    mo_ids: list[int],
    tprm_ids: list[int],
    is_admin: bool,
    user_permissions: list[str],
    returned_columns: list[str] | None = None,
    sorting: dict | None = None,
) -> list[Any]:
    """Implement request to Elastic INVENTORY_MO_LINK_INDEX with correct request and return _source data.
    _source contains: id(prm_id), mo_id, tprm_id(for mo id), value(linked mo id), version
    """
    body = generate_request_body(
        field_for_request="value",
        ids=mo_ids,
        tprm_ids=tprm_ids,
        is_admin=is_admin,
        user_permissions=user_permissions,
        returned_columns=returned_columns,
        sorting=sorting,
    )
    output = await search_after(
        elastic_client=elastic_client, index=INVENTORY_MO_LINK_INDEX, body=body
    )
    return output


def generate_request_body(
    field_for_request: str,
    ids: list[int],
    tprm_ids: list[int],
    is_admin: bool,
    user_permissions: list[str],
    returned_columns: list[str] | None = None,
    sorting: dict | None = None,
) -> dict:
    if len(ids) == 1:
        search_conditions = [{"term": {field_for_request: ids[0]}}]
    else:
        search_conditions = [{"terms": {field_for_request: ids}}]
    if tprm_ids:
        if len(tprm_ids) == 1:
            search_conditions.append({"term": {"tprm_id": tprm_ids[0]}})
        else:
            search_conditions.append({"terms": {"tprm_id": tprm_ids}})
    if not is_admin:
        search_conditions.append(
            {"terms": {INVENTORY_PERMISSIONS_FIELD_NAME: user_permissions}}
        )
    query = {"bool": {"must": search_conditions}}
    body = {
        "query": query,
        "track_total_hits": True,
        "_source": {"excludes": [INVENTORY_PERMISSIONS_FIELD_NAME]},
    }
    if sorting:
        body["sort"] = sorting
    if returned_columns:
        body["_source"].update({"includes": returned_columns})
    return body
