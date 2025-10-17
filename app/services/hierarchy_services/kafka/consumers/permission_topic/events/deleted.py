from typing import Union

from elasticsearch import AsyncElasticsearch

from services.hierarchy_services.elastic.configs import (
    HIERARCHY_HIERARCHIES_INDEX,
)
from services.hierarchy_services.elastic.mapping import (
    HIERARCHY_PERMISSIONS_FIELD_NAME,
)


async def with_hierarchy_permission_deleted(
    payload_before: Union[dict, None],
    payload_after: Union[dict, None],
    elastic_client: AsyncElasticsearch,
):
    """Kafka event handler for Hierarchy permission delete event"""
    if payload_after:
        hierarchy_id = payload_after.get("parent_id")
        permission = payload_after.get("permission")
        read = payload_after.get("read")
        if all([read, permission, hierarchy_id]):
            search_query = {
                "bool": {
                    "must": [
                        {"match": {"id": hierarchy_id}},
                        {"exists": {"field": HIERARCHY_PERMISSIONS_FIELD_NAME}},
                        {
                            "term": {
                                HIERARCHY_PERMISSIONS_FIELD_NAME: permission
                            }
                        },
                    ]
                }
            }
            update_script = {
                "source": "def permissions=ctx._source.%(field_name)s;"
                "if (permissions instanceof ArrayList)"
                "{ctx._source.%(field_name)s.removeIf(item -> item == params['perm_name']);"
                "int size = ctx._source.%(field_name)s.size();"
                "if (size == 1)"
                "{ctx._source.%(field_name)s = ctx._source.%(field_name)s[0]}"
                "}"
                "else if (permissions instanceof String)"
                "{ctx._source.%(field_name)s = params['str_default_value']}"
                % {"field_name": HIERARCHY_PERMISSIONS_FIELD_NAME},
                "lang": "painless",
                "params": {"perm_name": permission, "str_default_value": None},
            }

            await elastic_client.update_by_query(
                index=HIERARCHY_HIERARCHIES_INDEX,
                query=search_query,
                script=update_script,
                refresh=True,
            )
