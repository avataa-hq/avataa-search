from elasticsearch import AsyncElasticsearch

from services.hierarchy_services.elastic.configs import (
    HIERARCHY_HIERARCHIES_INDEX,
)
from services.hierarchy_services.elastic.mapping import (
    HIERARCHY_PERMISSIONS_FIELD_NAME,
)


async def on_create_hierarchy_permission(
    msg: dict[str : list[dict]] | None, elastic_client: AsyncElasticsearch
):
    """Kafka event handler for Hierarchy permission create event"""
    if not msg or not msg["objects"]:
        return
    for item in msg["objects"]:
        hierarchy_id = item.get("parent_id")
        permission = item.get("permission")
        read = item.get("read")
        if all([read, permission, hierarchy_id]):
            search_query = {"match": {"id": hierarchy_id}}
            update_script = {
                "source": "def permissions=ctx._source.%(field_name)s;"
                "if(permissions == null)"
                "{ctx._source.%(field_name)s = new ArrayList()}"
                "else if (permissions instanceof String)"
                "{ctx._source.%(field_name)s = new ArrayList();"
                "ctx._source.%(field_name)s.add(permissions)}"
                "if (!ctx._source.%(field_name)s.contains(params['perm_name']))"
                "{ctx._source.%(field_name)s.add(params['perm_name'])}"
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
