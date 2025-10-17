import grpc
from elasticsearch import AsyncElasticsearch

from sqlalchemy.ext.asyncio import AsyncSession
from elastic.query_builder_service.inventory_index.utils.index_utils import (
    get_index_name_by_tmo,
)
from grpc_clients.group_builder.getters.getters_without_channel import (
    get_list_of_groups_for_special_tmo,
    get_list_of_mo_ids_for_special_group,
    get_statistic_of_special_group,
)

from services.group_builder.models import GroupStatisticUniqueFields
from settings.config import GROUP_BUILDER_HOST, GROUP_BUILDER_GRPC_PORT


class GroupBuilderReloader:
    def __init__(
        self, elastic_client: AsyncElasticsearch, session: AsyncSession
    ):
        self.elastic_client = elastic_client
        self.db_session = session

    async def reload_group_data_for_tmo_id(self, tmo_id: int):
        index_name = get_index_name_by_tmo(tmo_id=tmo_id)

        if await self.elastic_client.indices.exists(index=index_name):
            async with grpc.aio.insecure_channel(
                f"{GROUP_BUILDER_HOST}:{GROUP_BUILDER_GRPC_PORT}"
            ) as async_channel:
                async for (
                    chunk_of_group_names
                ) in get_list_of_groups_for_special_tmo(
                    tmo_id=tmo_id, async_channel=async_channel
                ):
                    for group_name in chunk_of_group_names:
                        # update groups parameter for matched records
                        async for (
                            mo_ids_chunk
                        ) in get_list_of_mo_ids_for_special_group(
                            group_name=group_name, async_channel=async_channel
                        ):
                            search_query = {"terms": {"id": list(mo_ids_chunk)}}

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

                            await self.elastic_client.update_by_query(
                                index=index_name,
                                query=search_query,
                                script=update_script,
                                refresh=True,
                            )

                        # get and save current statistic
                        group_data = await get_statistic_of_special_group(
                            group_name=group_name, async_channel=async_channel
                        )

                        if group_data:
                            await self.elastic_client.index(
                                index=index_name,
                                id=group_data[
                                    GroupStatisticUniqueFields.GROUP_NAME.value
                                ],
                                document=group_data,
                                refresh="true",
                            )
