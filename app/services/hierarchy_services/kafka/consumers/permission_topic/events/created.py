from typing import Union

from elasticsearch import AsyncElasticsearch

from services.hierarchy_services.elastic.configs import (
    HIERARCHY_HIERARCHIES_INDEX,
)
from services.hierarchy_services.elastic.mapping import (
    HIERARCHY_PERMISSIONS_FIELD_NAME,
)


async def with_hierarchy_permission_created(
    payload_before: Union[dict, None],
    payload_after: Union[dict, None],
    elastic_client: AsyncElasticsearch,
):
    """Kafka event handler for Hierarchy permission create event"""
    if payload_after:
        hierarchy_id = payload_after.get("parent_id")
        permission = payload_after.get("permission")
        read = payload_after.get("read")
        if all([read, permission, hierarchy_id]):
            search_query = {"match": {"id": hierarchy_id}}
            update_script = {
                "source": "def permissions=ctx._source.%(field_name)s;"
                "if(permissions == null)"
                "{ctx._source.%(field_name)s = new ArrayList()}"
                "else if (permissions instanceof String)"
                "{ctx._source.%(field_name)s = new ArrayList();"
                "ctx._source.%(field_name)s.add(permissions)}"
                "ctx._source.%(field_name)s.add(params['perm_name'])"
                % {"field_name": HIERARCHY_PERMISSIONS_FIELD_NAME},
                "lang": "painless",
                "params": {"perm_name": permission},
            }

            await elastic_client.update_by_query(
                index=HIERARCHY_HIERARCHIES_INDEX,
                query=search_query,
                script=update_script,
                refresh=True,
            )
