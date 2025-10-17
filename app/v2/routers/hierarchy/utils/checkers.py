from uuid import UUID

from elasticsearch import AsyncElasticsearch
from fastapi import HTTPException

from security.security_data_models import UserData
from services.hierarchy_services.elastic.configs import (
    HIERARCHY_HIERARCHIES_INDEX,
    HIERARCHY_OBJ_INDEX,
)
from services.hierarchy_services.elastic.mapping import (
    HIERARCHY_PERMISSIONS_FIELD_NAME,
)
from services.inventory_services.utils.security.filter_by_realm import (
    check_permission_is_admin,
    get_permissions_from_client_role,
    raise_forbidden_ex_if_user_has_no_permission,
)


async def get_hierarchy_with_permission_check_or_raise_error(
    hierarchy_id: int, user_data: UserData, elastic_client: AsyncElasticsearch
):
    """If user has permission to read hierarchy - returns the hierarchy, otherwise raises 404 error"""
    is_admin = check_permission_is_admin(client_role=user_data.realm_access)
    user_permissions = get_permissions_from_client_role(
        client_role=user_data.realm_access
    )
    if not is_admin:
        raise_forbidden_ex_if_user_has_no_permission(
            client_permissions=user_permissions
        )

    search_conditions = [{"match": {"id": hierarchy_id}}]

    if not is_admin:
        search_conditions.append(
            {"terms": {HIERARCHY_PERMISSIONS_FIELD_NAME: user_permissions}}
        )

    search_body = {
        "query": {"bool": {"must": search_conditions}},
        "size": 1,
        "_source": {"excludes": [HIERARCHY_PERMISSIONS_FIELD_NAME]},
    }

    search_res = await elastic_client.search(
        index=HIERARCHY_HIERARCHIES_INDEX,
        body=search_body,
        ignore_unavailable=True,
    )
    search_res = search_res["hits"]["hits"]

    if not search_res:
        raise HTTPException(
            status_code=404,
            detail=f"Hierarchy with id = {hierarchy_id} not found",
        )

    hierarchy = search_res[0]["_source"]

    return hierarchy


async def get_node_or_raise_error(
    node_id: UUID, elastic_client: AsyncElasticsearch
):
    """Returns node by node_id, otherwise raise 404 error"""

    search_body = {"query": {"match": {"id": node_id}}}
    search_res = await elastic_client.search(
        index=HIERARCHY_OBJ_INDEX, body=search_body, ignore_unavailable=True
    )
    search_res = search_res["hits"]["hits"]
    if not search_res:
        raise HTTPException(
            status_code=404, detail=f"Node with id = {node_id} not found"
        )

    return search_res[0]["_source"]
