import logging
import math

import grpc
from elasticsearch import AsyncElasticsearch
from elasticsearch._async.helpers import async_bulk
from elasticsearch.helpers import BulkIndexError

from sqlalchemy.ext.asyncio import AsyncSession
from elastic.query_builder_service.inventory_index.utils.index_utils import (
    get_index_name_by_tmo,
)
from grpc_clients.zeebe.getters.getters_without_channel import (
    get_all_process_instance_data_from_ms_zeebe_grps,
)
from settings.config import ZEEBE_CLIENT_HOST, ZEEBE_CLIENT_GRPC_PORT


class ZeebeProcessInstanceReloader:
    def __init__(
        self, elastic_client: AsyncElasticsearch, session: AsyncSession
    ):
        self.elastic_client = elastic_client
        self.db_session = session

    async def reload_zeebe_data_for_tmo_id(self, tmo_id: int):
        index_name = get_index_name_by_tmo(tmo_id=tmo_id)
        items_per_query = 10000

        if await self.elastic_client.indices.exists(index=index_name):
            async with grpc.aio.insecure_channel(
                f"{ZEEBE_CLIENT_HOST}:{ZEEBE_CLIENT_GRPC_PORT}"
            ) as async_channel:
                async for (
                    grpc_chunk
                ) in get_all_process_instance_data_from_ms_zeebe_grps(
                    async_channel=async_channel, tmo_id=tmo_id
                ):
                    corresp_dict = {
                        item.id: {
                            field.name: value
                            for field, value in item.ListFields()
                            if field.name != "id"
                        }
                        for item in grpc_chunk.pr_inst_data
                    }

                    steps = math.ceil(len(corresp_dict) / items_per_query)
                    mo_ids = list(corresp_dict.keys())
                    for step in range(steps):
                        start = step * items_per_query
                        end = start + items_per_query
                        step_mo_ids = mo_ids[start:end]

                        search_query = {"terms": {"id": step_mo_ids}}

                        search_res = await self.elastic_client.search(
                            index=index_name,
                            query=search_query,
                            track_total_hits=True,
                            size=items_per_query,
                        )

                        search_res = search_res["hits"]["hits"]

                        actions = []

                        for item_from_elastic in search_res:
                            item_from_elastic = item_from_elastic["_source"]
                            data_from_zeebe = corresp_dict.get(
                                item_from_elastic["id"]
                            )

                            if data_from_zeebe:
                                item_from_elastic.update(data_from_zeebe)

                                action_item = dict(
                                    _index=index_name,
                                    _op_type="update",
                                    _id=item_from_elastic["id"],
                                    doc=item_from_elastic,
                                )
                                actions.append(action_item)

                        if actions:
                            try:
                                await async_bulk(
                                    client=self.elastic_client,
                                    refresh="true",
                                    actions=actions,
                                )
                            except BulkIndexError as e:
                                logging.error(e.errors)
                                raise e
