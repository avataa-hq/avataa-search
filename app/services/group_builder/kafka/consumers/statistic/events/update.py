from elasticsearch import AsyncElasticsearch

from elastic.query_builder_service.inventory_index.utils.index_utils import (
    get_index_name_by_tmo,
)
from services.group_builder.models import GroupStatisticUniqueFields


async def with_update_statistic(msg: dict, async_client: AsyncElasticsearch):
    group_name = msg.get(GroupStatisticUniqueFields.GROUP_NAME.value)
    tmo_id = msg.get("tmo_id")
    if group_name and tmo_id:
        index_name = get_index_name_by_tmo(tmo_id)
        msg["state"] = "ACTIVE"

        try:
            await async_client.index(
                index=index_name, id=group_name, document=msg, refresh="true"
            )
        except Exception as e:
            print(msg)
            raise e
