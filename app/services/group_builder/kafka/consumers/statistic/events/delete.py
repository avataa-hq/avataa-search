from elasticsearch import AsyncElasticsearch

from elastic.query_builder_service.inventory_index.utils.index_utils import (
    get_index_name_by_tmo,
)
from services.group_builder.models import GroupStatisticUniqueFields


async def with_delete_statistic(msg: dict, async_client: AsyncElasticsearch):
    group_name = msg.get(GroupStatisticUniqueFields.GROUP_NAME.value)
    tmo_id = msg.get("tmo_id")
    if group_name and tmo_id:
        index_name = get_index_name_by_tmo(tmo_id)
        delete_query = {"match": {"_id": group_name}}
        try:
            await async_client.delete_by_query(
                index=index_name,
                query=delete_query,
                ignore_unavailable=True,
                refresh=True,
            )
        except Exception as e:
            print(msg)
            raise e
