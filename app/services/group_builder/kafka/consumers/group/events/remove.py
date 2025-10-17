from elasticsearch import AsyncElasticsearch

from elastic.config import INVENTORY_OBJ_INDEX_PREFIX
from services.group_builder.models import GroupFields
from services.inventory_services.models import InventoryMOAdditionalFields


async def with_group_remove(msg: dict, async_client: AsyncElasticsearch):
    entity_ids = msg.get(GroupFields.ENTITY_ID.value)
    group_name = msg.get(GroupFields.GROUP_NAME.value)
    group_tmo_id = str(msg.get(GroupFields.TMO_ID.value))

    if entity_ids and group_name:
        search_query = {
            "bool": {
                "must": [
                    {
                        "terms": {
                            InventoryMOAdditionalFields.GROUPS.value: [
                                group_name
                            ]
                        }
                    },
                    {"terms": {"id": entity_ids}},
                ]
            }
        }

        update_script = {
            "source": "def groups=ctx._source.groups;"
            "if (groups instanceof ArrayList)"
            "{ctx._source.groups.removeIf(item -> item == params['group_name']);"
            "int size = ctx._source.groups.size();"
            "if (size == 1)"
            "{ctx._source.groups = ctx._source.groups[0]}"
            "}"
            "else if (groups instanceof String)"
            "{ctx._source.groups = params['str_default_value']}",
            "lang": "painless",
            "params": {"group_name": group_name, "str_default_value": None},
        }

        await async_client.update_by_query(
            index=INVENTORY_OBJ_INDEX_PREFIX + group_tmo_id + "_index",
            query=search_query,
            script=update_script,
            scroll="5m",
            scroll_size=5000,
            search_timeout="10s",
            slices=2,
            refresh=True,
        )
