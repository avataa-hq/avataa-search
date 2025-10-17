from elasticsearch import AsyncElasticsearch

from elastic.config import INVENTORY_OBJ_INDEX_PREFIX
from services.group_builder.models import GroupFields


async def with_group_add(msg: dict, async_client: AsyncElasticsearch):
    entity_ids = msg.get(GroupFields.ENTITY_ID.value)
    group_name = msg.get(GroupFields.GROUP_NAME.value)
    group_tmo_id = str(msg.get(GroupFields.TMO_ID.value))

    if not entity_ids or not group_name:
        return
    search_query = {"terms": {"id": list(entity_ids)}}
    update_script = {
        "source": "def groups=ctx._source.groups;"
        "if(groups == null)"
        "{ctx._source.groups = new ArrayList()}"
        "else if (groups instanceof String)"
        "{ctx._source.groups = new ArrayList();"
        "ctx._source.groups.add(groups)}"
        "ctx._source.groups.add(params['group_name'])",
        "lang": "painless",
        "params": {"group_name": group_name},
    }
    await async_client.update_by_query(
        index=INVENTORY_OBJ_INDEX_PREFIX + group_tmo_id + "_index",
        query=search_query,
        script=update_script,
        scroll="2m",
        scroll_size=1000,
        search_timeout="30s",
        slices=2,
        refresh=True,
    )
