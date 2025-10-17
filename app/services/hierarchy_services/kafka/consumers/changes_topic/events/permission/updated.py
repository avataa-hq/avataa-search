from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import BulkIndexError, async_bulk

from services.hierarchy_services.elastic.configs import (
    HIERARCHY_HIERARCHIES_INDEX,
)
from services.hierarchy_services.elastic.mapping import (
    HIERARCHY_PERMISSIONS_FIELD_NAME,
)
from services.hierarchy_services.kafka.consumers.changes_topic.events.limits import (
    QUERY_MAX_SIZE,
)


async def on_update_hierarchy_permission(
    msg: dict[str : list[dict]] | None, elastic_client: AsyncElasticsearch
):
    """Kafka event handler for Hierarchy permission update event"""
    if not msg or not msg["objects"]:
        return

    for item in msg["objects"]:
        hierarchy_id = item.get("parent_id")
        permission = item.get("permission")
        read = item.get("read")
        if all([hierarchy_id, permission]):
            if read:
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

            else:
                search_query = {
                    "bool": {
                        "must": [
                            {"match": {"id": hierarchy_id}},
                            {
                                "exists": {
                                    "field": HIERARCHY_PERMISSIONS_FIELD_NAME
                                }
                            },
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
                    "params": {
                        "perm_name": permission,
                        "str_default_value": None,
                    },
                }

            await elastic_client.update_by_query(
                index=HIERARCHY_HIERARCHIES_INDEX,
                query=search_query,
                script=update_script,
                refresh=True,
            )

    hierarchies = {i["id"]: i for i in msg["objects"]}
    hierarchy_ids = list(hierarchies.keys())
    chunks_hierarchy_ids = (
        hierarchy_ids[i : i + QUERY_MAX_SIZE]
        for i in range(0, len(hierarchy_ids), QUERY_MAX_SIZE)
    )
    # collecting existing hierarchies
    exists_hierarchies = {}
    for chunk_hierarchy_ids in chunks_hierarchy_ids:
        search_query = {"terms": {"id": chunk_hierarchy_ids}}
        hierarchies_exists = await elastic_client.search(
            index=HIERARCHY_HIERARCHIES_INDEX,
            query=search_query,
            size=QUERY_MAX_SIZE,
        )
        chunk_hierarchies_exists = {
            i["_source"]["id"]: i["_source"]
            for i in hierarchies_exists["hits"]["hits"]
        }
        exists_hierarchies.update(chunk_hierarchies_exists)

    # create actions for bulk operation
    actions = []
    for hierarchy_id, item in exists_hierarchies.items():
        update_item = hierarchies[hierarchy_id]
        item.update(update_item)
        actions.append(
            dict(
                _index=HIERARCHY_HIERARCHIES_INDEX,
                _op_type="update",
                _id=item["id"],
                doc=item,
            )
        )

    # save
    try:
        await async_bulk(client=elastic_client, refresh="true", actions=actions)
    except BulkIndexError as e:
        print(*actions, sep="\n")
        print(*e.errors, sep="\n")
        raise e
