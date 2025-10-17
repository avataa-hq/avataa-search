from collections import defaultdict
from typing import List

import grpc
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk, BulkIndexError
from grpc.aio import Channel
from sqlalchemy.ext.asyncio import AsyncSession

from elastic.config import (
    ALL_MO_OBJ_INDEXES_PATTERN,
    INVENTORY_TMO_INDEX_V2,
    INVENTORY_TPRM_INDEX_V2,
)
from indexes_mapping.inventory.mapping import INVENTORY_PERMISSIONS_FIELD_NAME
from services.inventory_services.elastic.security.configs import (
    INVENTORY_SECURITY_MO_PERMISSION_INDEX,
    INVENTORY_SECURITY_MO_PERMISSION_SETTINGS,
    INVENTORY_SECURITY_TMO_PERMISSION_INDEX,
    INVENTORY_SECURITY_TMO_PERMISSION_SETTINGS,
    INVENTORY_SECURITY_TPRM_PERMISSION_INDEX,
    INVENTORY_SECURITY_TPRM_PERMISSION_SETTINGS,
    INVENTORY_SECURITY_PRM_PERMISSION_INDEX,
    INVENTORY_SECURITY_PRM_PERMISSION_SETTINGS,
)
from services.inventory_services.elastic.security.mapping import (
    INVENTORY_SECURITY_INDEXES_MAPPING,
)
from services.inventory_services.grpc.security_getters.getters_without_channel import (
    get_mo_permissions,
    get_tmo_permissions,
    get_tprm_permissions,
)
from settings.config import INVENTORY_HOST, INVENTORY_GRPC_PORT


class InventorySecurityReloader:
    def __init__(
        self, elastic_client: AsyncElasticsearch, session: AsyncSession
    ):
        self.elastic_client = elastic_client
        self.session = session

    async def __step_1_clear_mo_security_index(self):
        """Deletes current INVENTORY_SECURITY_MO_PERMISSION_INDEX from elastic search and creates
        new INVENTORY_SECURITY_MO_PERMISSION_INDEX index"""
        await self.elastic_client.indices.delete(
            index=INVENTORY_SECURITY_MO_PERMISSION_INDEX,
            ignore_unavailable=True,
        )
        await self.elastic_client.indices.create(
            index=INVENTORY_SECURITY_MO_PERMISSION_INDEX,
            mappings=INVENTORY_SECURITY_INDEXES_MAPPING,
            settings=INVENTORY_SECURITY_MO_PERMISSION_SETTINGS,
        )

        search_query = {"exists": {"field": INVENTORY_PERMISSIONS_FIELD_NAME}}
        update_script = {
            "source": "ctx._source.%(field_name)s = params['new_point_b_name'];"
            % {"field_name": INVENTORY_PERMISSIONS_FIELD_NAME},
            "lang": "painless",
            "params": {"new_point_b_name": None},
        }

        await self.elastic_client.update_by_query(
            index=ALL_MO_OBJ_INDEXES_PATTERN,
            query=search_query,
            script=update_script,
            refresh=True,
        )

    async def __step_1_clear_tmo_security_index(self):
        """Deletes current INVENTORY_SECURITY_TMO_PERMISSION_INDEX from elastic search and creates
        new INVENTORY_SECURITY_TMO_PERMISSION_INDEX index"""
        await self.elastic_client.indices.delete(
            index=INVENTORY_SECURITY_TMO_PERMISSION_INDEX,
            ignore_unavailable=True,
        )
        await self.elastic_client.indices.create(
            index=INVENTORY_SECURITY_TMO_PERMISSION_INDEX,
            mappings=INVENTORY_SECURITY_INDEXES_MAPPING,
            settings=INVENTORY_SECURITY_TMO_PERMISSION_SETTINGS,
        )

        search_query = {"exists": {"field": INVENTORY_PERMISSIONS_FIELD_NAME}}
        update_script = {
            "source": "ctx._source.%(field_name)s = params['new_point_b_name'];"
            % {"field_name": INVENTORY_PERMISSIONS_FIELD_NAME},
            "lang": "painless",
            "params": {"new_point_b_name": None},
        }

        await self.elastic_client.update_by_query(
            index=INVENTORY_TMO_INDEX_V2,
            query=search_query,
            script=update_script,
            refresh=True,
        )

    async def __step_1_clear_tprm_security_index(self):
        """Deletes current INVENTORY_SECURITY_TPRM_PERMISSION_INDEX from elastic search and creates
        new INVENTORY_SECURITY_TPRM_PERMISSION_INDEX index"""
        await self.elastic_client.indices.delete(
            index=INVENTORY_SECURITY_TPRM_PERMISSION_INDEX,
            ignore_unavailable=True,
        )
        await self.elastic_client.indices.create(
            index=INVENTORY_SECURITY_TPRM_PERMISSION_INDEX,
            mappings=INVENTORY_SECURITY_INDEXES_MAPPING,
            settings=INVENTORY_SECURITY_TPRM_PERMISSION_SETTINGS,
        )

        search_query = {"exists": {"field": INVENTORY_PERMISSIONS_FIELD_NAME}}
        update_script = {
            "source": "ctx._source.%(field_name)s = params['new_point_b_name'];"
            % {"field_name": INVENTORY_PERMISSIONS_FIELD_NAME},
            "lang": "painless",
            "params": {"new_point_b_name": None},
        }

        await self.elastic_client.update_by_query(
            index=INVENTORY_TMO_INDEX_V2,
            query=search_query,
            script=update_script,
            refresh=True,
        )

    async def __step_1_clear_prm_security_index(self):
        """Deletes current INVENTORY_SECURITY_PRM_PERMISSION_INDEX from elastic search and creates
        new INVENTORY_SECURITY_PRM_PERMISSION_INDEX index"""
        await self.elastic_client.indices.delete(
            index=INVENTORY_SECURITY_PRM_PERMISSION_INDEX,
            ignore_unavailable=True,
        )
        await self.elastic_client.indices.create(
            index=INVENTORY_SECURITY_PRM_PERMISSION_INDEX,
            mappings=INVENTORY_SECURITY_INDEXES_MAPPING,
            settings=INVENTORY_SECURITY_PRM_PERMISSION_SETTINGS,
        )

    @staticmethod
    def __create_actions(
        list_of_permissions: List[dict], index: str
    ) -> List[dict]:
        """Returns list of actions for further elastic async_bulk"""
        actions = [
            {
                "_index": index,
                "_op_type": "index",
                "_id": item_id,
                "_source": item,
            }
            for item in list_of_permissions
            if (item_id := item.get("id"))
        ]

        return actions

    async def __step_2_load_mo_security_data(self, channel: Channel):
        """Loads MO security data from inventory"""
        async for list_of_mo_permissions in get_mo_permissions(channel):
            actions = list()

            mo_ids_grouped_by_permissions = defaultdict(list)

            for item in list_of_mo_permissions:
                item_id = item.get("id")
                mo_id = item.get("parent_id")
                permission = item.get("permission")
                read = item.get("read")

                if item_id:
                    actions.append(
                        {
                            "_index": INVENTORY_SECURITY_MO_PERMISSION_INDEX,
                            "_op_type": "index",
                            "_id": item_id,
                            "_source": item,
                        }
                    )

                    if all([read, mo_id, permission]):
                        mo_ids_grouped_by_permissions[permission].append(mo_id)

            if actions:
                try:
                    await async_bulk(
                        client=self.elastic_client,
                        refresh="true",
                        actions=actions,
                    )
                except BulkIndexError as e:
                    print(e.errors)
                    raise e

            for k, v in mo_ids_grouped_by_permissions.items():
                search_query = {"terms": {"id": v}}
                update_script = {
                    "source": "def permissions=ctx._source.%(field_name)s;"
                    "if(permissions == null)"
                    "{ctx._source.%(field_name)s = new ArrayList()}"
                    "else if (permissions instanceof String)"
                    "{ctx._source.%(field_name)s = new ArrayList();"
                    "ctx._source.%(field_name)s.add(permissions)}"
                    "ctx._source.%(field_name)s.add(params['perm_name'])"
                    % {"field_name": INVENTORY_PERMISSIONS_FIELD_NAME},
                    "lang": "painless",
                    "params": {"perm_name": k},
                }

                await self.elastic_client.update_by_query(
                    index=ALL_MO_OBJ_INDEXES_PATTERN,
                    query=search_query,
                    script=update_script,
                    refresh=True,
                )

    async def __step_2_load_tmo_security_data(self, channel: Channel):
        """Loads TMO security data from inventory"""
        async for list_of_tmo_permissions in get_tmo_permissions(channel):
            actions = list()

            tmo_ids_grouped_by_permissions = defaultdict(list)

            for item in list_of_tmo_permissions:
                item_id = item.get("id")
                tmo_id = item.get("parent_id")
                permission = item.get("permission")
                read = item.get("read")
                if item_id:
                    action_item = dict(
                        _index=INVENTORY_SECURITY_TMO_PERMISSION_INDEX,
                        _op_type="index",
                        _id=item_id,
                        _source=item,
                    )
                    actions.append(action_item)

                    if all([read, tmo_id, permission]):
                        tmo_ids_grouped_by_permissions[permission].append(
                            tmo_id
                        )

            if actions:
                try:
                    await async_bulk(
                        client=self.elastic_client,
                        refresh="true",
                        actions=actions,
                    )
                except BulkIndexError as e:
                    print(e.errors)
                    raise e

            for k, v in tmo_ids_grouped_by_permissions.items():
                search_query = {"terms": {"id": v}}
                update_script = {
                    "source": "def permissions=ctx._source.%(field_name)s;"
                    "if(permissions == null)"
                    "{ctx._source.%(field_name)s = new ArrayList()}"
                    "else if (permissions instanceof String)"
                    "{ctx._source.%(field_name)s = new ArrayList();"
                    "ctx._source.%(field_name)s.add(permissions)}"
                    "ctx._source.%(field_name)s.add(params['perm_name'])"
                    % {"field_name": INVENTORY_PERMISSIONS_FIELD_NAME},
                    "lang": "painless",
                    "params": {"perm_name": k},
                }

                await self.elastic_client.update_by_query(
                    index=INVENTORY_TMO_INDEX_V2,
                    query=search_query,
                    script=update_script,
                    refresh=True,
                )

    async def __step_2_load_tprm_security_data(self, channel: Channel):
        """Loads TPRM security data from inventory"""
        async for list_of_tprm_permissions in get_tprm_permissions(channel):
            actions = list()

            tprm_ids_grouped_by_permissions = defaultdict(list)

            for item in list_of_tprm_permissions:
                item_id = item.get("id")
                tprm_id = item.get("parent_id")
                permission = item.get("permission")
                read = item.get("read")
                if item_id:
                    action_item = dict(
                        _index=INVENTORY_SECURITY_TPRM_PERMISSION_INDEX,
                        _op_type="index",
                        _id=item_id,
                        _source=item,
                    )
                    actions.append(action_item)

                    if all([read, tprm_id, permission]):
                        tprm_ids_grouped_by_permissions[permission].append(
                            tprm_id
                        )

            if actions:
                try:
                    await async_bulk(
                        client=self.elastic_client,
                        refresh="true",
                        actions=actions,
                    )
                except BulkIndexError as e:
                    print(e.errors)
                    raise e

            for k, v in tprm_ids_grouped_by_permissions.items():
                search_query = {"terms": {"id": v}}
                update_script = {
                    "source": "def permissions=ctx._source.%(field_name)s;"
                    "if(permissions == null)"
                    "{ctx._source.%(field_name)s = new ArrayList()}"
                    "else if (permissions instanceof String)"
                    "{ctx._source.%(field_name)s = new ArrayList();"
                    "ctx._source.%(field_name)s.add(permissions)}"
                    "ctx._source.%(field_name)s.add(params['perm_name'])"
                    % {"field_name": INVENTORY_PERMISSIONS_FIELD_NAME},
                    "lang": "painless",
                    "params": {"perm_name": k},
                }

                await self.elastic_client.update_by_query(
                    index=INVENTORY_TPRM_INDEX_V2,
                    query=search_query,
                    script=update_script,
                    refresh=True,
                )

    async def clear_all_indexes(self):
        """Clears all security indexes from elastic"""
        await self.__step_1_clear_mo_security_index()
        await self.__step_1_clear_tmo_security_index()
        await self.__step_1_clear_tprm_security_index()
        await self.__step_1_clear_prm_security_index()

    async def refresh_all_inventory_security_indexes(self):
        """Refreshes all security indexes"""

        await self.clear_all_indexes()

        async with grpc.aio.insecure_channel(
            f"{INVENTORY_HOST}:{INVENTORY_GRPC_PORT}",
            options=[
                ("grpc.keepalive_time_ms", 20_000),
                ("grpc.keepalive_timeout_ms", 15_000),
                ("grpc.http2.max_pings_without_data", 5),
                ("grpc.keepalive_permit_without_calls", 1),
            ],
        ) as async_channel:
            await self.__step_2_load_mo_security_data(channel=async_channel)
            await self.__step_2_load_tmo_security_data(channel=async_channel)
            await self.__step_2_load_tprm_security_data(channel=async_channel)
