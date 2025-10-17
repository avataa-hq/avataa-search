from elasticsearch import AsyncElasticsearch

from security.security_data_models import UserPermission
from services.hierarchy_services.elastic.configs import (
    HIERARCHY_HIERARCHIES_INDEX,
)
from services.hierarchy_services.elastic.mapping import (
    HIERARCHY_PERMISSIONS_FIELD_NAME,
)
from v2.tasks.utils.exceptions import (
    SearchPermissionException,
    SearchNotFoundException,
)


async def get_hierarchy_with_permission_check_or_raise_error(
    hierarchy_id: int,
    user_permission: UserPermission,
    elastic_client: AsyncElasticsearch,
):
    """If user has permission to read hierarchy - returns the hierarchy, otherwise raises 404 error"""
    if not user_permission.is_admin and not user_permission.user_permissions:
        raise SearchPermissionException("Forbidden. No permissions")

    search_conditions = [{"match": {"id": hierarchy_id}}]

    if not user_permission.is_admin:
        search_conditions.append(
            {
                "terms": {
                    HIERARCHY_PERMISSIONS_FIELD_NAME: user_permission.user_permissions
                }
            }
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
        raise SearchNotFoundException(
            f"Hierarchy with id = {hierarchy_id} not found"
        )

    hierarchy = search_res[0]["_source"]

    return hierarchy
