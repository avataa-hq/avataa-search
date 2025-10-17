from typing import Union

import grpc
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk, BulkIndexError
from grpc.aio import Channel

from sqlalchemy.ext.asyncio import AsyncSession

from grpc_clients.hierarchy.getters.getters_without_level import (
    get_hierarchy_as_dict_by_grpc,
    get_hierarchy_permissions_as_dicts_for_spec_hierarchy_by_grpc,
    get_all_levels_for_spec_hierarchy_by_as_dicts_grpc,
    get_all_obj_for_spec_level_as_dicts_by_grpc,
    get_all_node_data_for_spec_level_as_dicts_by_grpc,
    get_all_hierarchies_as_dicts_by_grpc,
)

from services.hierarchy_services.elastic.configs import (
    HIERARCHY_HIERARCHIES_INDEX_SETTINGS,
    HIERARCHY_HIERARCHIES_INDEX,
    HIERARCHY_LEVELS_INDEX,
    HIERARCHY_LEVELS_INDEX_SETTINGS,
    HIERARCHY_NODE_DATA_INDEX_SETTINGS,
    HIERARCHY_NODE_DATA_INDEX,
    HIERARCHY_OBJ_INDEX_SETTINGS,
    HIERARCHY_OBJ_INDEX,
)
from services.hierarchy_services.elastic.mapping import (
    HIERARCHY_HIERARCHIES_INDEX_MAPPING,
    HIERARCHY_LEVEL_INDEX_MAPPING,
    HIERARCHY_NODE_DATA_INDEX_MAPPING,
    HIERARCHY_OBJ_INDEX_MAPPING,
    HIERARCHY_PERMISSIONS_FIELD_NAME,
)

from settings.config import HIERARCHY_GRPC_PORT, HIERARCHY_HOST


class HierarchyIndexesReloader:
    def __init__(
        self, elastic_client: AsyncElasticsearch, session: AsyncSession
    ):
        self.elastic_client = elastic_client
        self.session = session
        self.__hierarchy_levels_data_cache = list()

    async def __stage_1_clear_hierarchies_index(self):
        """Deletes current HIERARCHY_HIERARCHIES_INDEX from elastic search and creates
        new HIERARCHY_HIERARCHIES_INDEX index"""

        await self.elastic_client.indices.delete(
            index=HIERARCHY_HIERARCHIES_INDEX, ignore_unavailable=True
        )
        await self.elastic_client.indices.create(
            index=HIERARCHY_HIERARCHIES_INDEX,
            mappings=HIERARCHY_HIERARCHIES_INDEX_MAPPING,
            settings=HIERARCHY_HIERARCHIES_INDEX_SETTINGS,
        )

    async def __stage_1_clear_hierarchies_level_index(self):
        """Deletes current HIERARCHY_LEVELS_INDEX from elastic search and creates
        new HIERARCHY_LEVELS_INDEX index"""

        await self.elastic_client.indices.delete(
            index=HIERARCHY_LEVELS_INDEX, ignore_unavailable=True
        )
        await self.elastic_client.indices.create(
            index=HIERARCHY_LEVELS_INDEX,
            mappings=HIERARCHY_LEVEL_INDEX_MAPPING,
            settings=HIERARCHY_LEVELS_INDEX_SETTINGS,
        )

    async def __stage_1_clear_hierarchies_obj_index(self):
        """Deletes current HIERARCHY_OBJ_INDEX from elastic search and creates
        new HIERARCHY_OBJ_INDEX index"""
        await self.elastic_client.indices.delete(
            index=HIERARCHY_OBJ_INDEX, ignore_unavailable=True
        )
        await self.elastic_client.indices.create(
            index=HIERARCHY_OBJ_INDEX,
            mappings=HIERARCHY_OBJ_INDEX_MAPPING,
            settings=HIERARCHY_OBJ_INDEX_SETTINGS,
        )

    async def __stage_1_clear_hierarchies_node_data_index(self):
        """Deletes current HIERARCHY_NODE_DATA_INDEX from elastic search and creates
        new HIERARCHY_NODE_DATA_INDEX index"""

        await self.elastic_client.indices.delete(
            index=HIERARCHY_NODE_DATA_INDEX, ignore_unavailable=True
        )
        await self.elastic_client.indices.create(
            index=HIERARCHY_NODE_DATA_INDEX,
            mappings=HIERARCHY_NODE_DATA_INDEX_MAPPING,
            settings=HIERARCHY_NODE_DATA_INDEX_SETTINGS,
        )

    async def __stage_1_clear_special_hierarchy_data_from_hierarchies_index(
        self, hierarchy_id: int
    ):
        """Deletes only data of hierarchy_id = self.hierarchy_id from HIERARCHY_HIERARCHIES_INDEX"""

        search_query = {"match": {"id": hierarchy_id}}
        try:
            await self.elastic_client.delete_by_query(
                index=HIERARCHY_HIERARCHIES_INDEX,
                query=search_query,
                ignore_unavailable=True,
                refresh=True,
            )
        except Exception as e:
            print(e)

    async def __stage_1_clear_levels_data_for_special_hierarchy_from_levels_index(
        self, hierarchy_id: int
    ):
        """Deletes only data of levels with level.hierarchy_id = self.hierarchy_id from HIERARCHY_LEVELS_INDEX"""

        search_query = {"match": {"hierarchy_id": hierarchy_id}}
        try:
            await self.elastic_client.delete_by_query(
                index=HIERARCHY_LEVELS_INDEX,
                query=search_query,
                ignore_unavailable=True,
                refresh=True,
            )
        except Exception as e:
            print(e)

    async def __stage_1_clear_obj_for_special_hierarchy_from_obj_index(
        self, hierarchy_id
    ):
        """Deletes obj index of special hierarchy"""
        search_query = {"match": {"hierarchy_id": hierarchy_id}}
        try:
            await self.elastic_client.delete_by_query(
                index=HIERARCHY_OBJ_INDEX,
                query=search_query,
                ignore_unavailable=True,
                refresh=True,
            )
        except Exception as e:
            print(e)

    async def __stage_1_clear_node_data_for_special_hierarchy_from_node_data_index(
        self, hierarchy_id
    ):
        """Deletes current HIERARCHY_NODE_DATA_INDEX from elastic search and creates
        new HIERARCHY_NODE_DATA_INDEX index"""

        search_query = {"match": {"hierarchy_id": hierarchy_id}}
        sort_cond = {"id": {"order": "asc"}}

        size_per_step = 10_000
        search_args = {
            "query": search_query,
            "size": size_per_step,
            "sort": sort_cond,
            "track_total_hits": True,
            "index": HIERARCHY_LEVELS_INDEX,
        }

        search_after = None
        level_ids = list()
        while True:
            search_res = await self.elastic_client.search(
                **search_args, search_after=search_after, source_includes=["id"]
            )
            total_hits = search_res["hits"]["total"]["value"]

            search_res = search_res["hits"]["hits"]
            step_level_ids = [item["_source"]["id"] for item in search_res]

            if total_hits < size_per_step:
                level_ids.extend(step_level_ids)
                break

            if len(step_level_ids) == size_per_step:
                search_after = search_res[-1]["sort"]
                level_ids.extend(step_level_ids)
            else:
                break

        search_query = {"terms": {"level_id": level_ids}}

        await self.elastic_client.delete_by_query(
            index=HIERARCHY_NODE_DATA_INDEX,
            query=search_query,
            ignore_unavailable=True,
            refresh=True,
        )

    @staticmethod
    async def __check_if_hierarchy_exists(
        hierarchy_id: int, channel: Channel
    ) -> Union[dict, None]:
        """Returns hierarchy data as dict if hierarchy exists, otherwise returns None"""
        hierarchy_as_dict = await get_hierarchy_as_dict_by_grpc(
            hierarchy_id, channel
        )
        if hierarchy_as_dict:
            return hierarchy_as_dict
        else:
            return None

    async def __load_and_save_hierarchy_into_hierarchy_index(
        self, hierarchy_data: dict
    ):
        """Loads and saves hierarchy data into HIERARCHY_HIERARCHIES_INDEX"""
        hierarchy_id = hierarchy_data.get("id")
        if not hierarchy_id:
            raise ValueError("hierarchy_id cannot be empty")
        await self.elastic_client.index(
            index=HIERARCHY_HIERARCHIES_INDEX,
            id=str(hierarchy_id),
            document=hierarchy_data,
            refresh="true",
        )

    async def __load_and_save_hierarchy_permissions(
        self, hierarchy_id: int, channel: Channel
    ):
        """Loads and saves permissions into HIERARCHY_HIERARCHIES_INDEX"""
        hierarchy_permissions = set()
        async for (
            permission_chunk
        ) in get_hierarchy_permissions_as_dicts_for_spec_hierarchy_by_grpc(
            hierarchy_id, channel
        ):
            step_permissions = {
                item.get("permission")
                for item in permission_chunk
                if all([item.get("read"), item.get("permission")])
            }
            hierarchy_permissions.update(step_permissions)

        if not hierarchy_permissions:
            return

        hierarchy_permissions = list(hierarchy_permissions)

        search_query = {"match": {"id": hierarchy_id}}
        hierarchy_exists = await self.elastic_client.search(
            index=HIERARCHY_HIERARCHIES_INDEX, query=search_query, size=1
        )
        hierarchy_exists = hierarchy_exists["hits"]["hits"]

        if hierarchy_exists:
            hierarchy_exists = hierarchy_exists[0]["_source"]

            hierarchy_exists[HIERARCHY_PERMISSIONS_FIELD_NAME] = (
                hierarchy_permissions
            )

            await self.elastic_client.index(
                index=HIERARCHY_HIERARCHIES_INDEX,
                id=str(hierarchy_id),
                document=hierarchy_exists,
                refresh="true",
            )

    async def __load_and_save_hierarchy_levels(
        self, hierarchy_id: int, channel: Channel
    ):
        """Loads and saves levels into HIERARCHY_LEVELS_INDEX"""
        self.__hierarchy_levels_data_cache = list()
        async for (
            level_chunk
        ) in get_all_levels_for_spec_hierarchy_by_as_dicts_grpc(
            hierarchy_id, channel
        ):
            actions = list()
            self.__hierarchy_levels_data_cache.extend(level_chunk)
            for item in level_chunk:
                actions.append(
                    dict(
                        _index=HIERARCHY_LEVELS_INDEX,
                        _op_type="index",
                        _id=item["id"],
                        _source=item,
                    )
                )
            try:
                await async_bulk(
                    client=self.elastic_client, refresh="true", actions=actions
                )
            except BulkIndexError as e:
                self.__hierarchy_levels_data_cache = list()
                print(*actions, sep="\n")
                print(e.errors)
                raise e

    async def __load_and_save_hierarchy_obj(self, channel: Channel):
        """Loads and saves hierarchy objects into HIERARCHY_OBJ_INDEX"""
        for level_data in self.__hierarchy_levels_data_cache:
            level_id = int(level_data.get("id"))
            async for obj_chunk in get_all_obj_for_spec_level_as_dicts_by_grpc(
                level_id, channel
            ):
                actions = list()
                for item in obj_chunk:
                    actions.append(
                        dict(
                            _index=HIERARCHY_OBJ_INDEX,
                            _op_type="index",
                            _id=item["id"],
                            _source=item,
                        )
                    )
                try:
                    await async_bulk(
                        client=self.elastic_client,
                        refresh="true",
                        actions=actions,
                    )
                except BulkIndexError as e:
                    print(e.errors)
                    raise e

    async def __load_and_save_hierarchy_node_data(self, channel: Channel):
        """Loads and saves hierarchy node_data into HIERARCHY_NODE_DATA_INDEX"""

        for level_data in self.__hierarchy_levels_data_cache:
            level_id = int(level_data.get("id"))

            async for (
                node_data_chunk
            ) in get_all_node_data_for_spec_level_as_dicts_by_grpc(
                level_id, channel
            ):
                actions = list()
                for item in node_data_chunk:
                    actions.append(
                        dict(
                            _index=HIERARCHY_NODE_DATA_INDEX,
                            _op_type="index",
                            _id=item["id"],
                            _source=item,
                        )
                    )
                try:
                    await async_bulk(
                        client=self.elastic_client,
                        refresh="true",
                        actions=actions,
                    )
                except BulkIndexError as e:
                    print(e.errors)
                    raise e

    async def _load_and_save_data_for_special_hierarchy(
        self, hierarchy_id: int, async_channel
    ):
        """Loads from MS Hierarchy and saves data into special indexes for special hierarchy_id"""

        # load and save hierarchy data
        hierarchy_as_dict = await self.__check_if_hierarchy_exists(
            hierarchy_id, async_channel
        )
        print(hierarchy_as_dict)
        if not hierarchy_as_dict:
            return
        await self.__load_and_save_hierarchy_into_hierarchy_index(
            hierarchy_as_dict
        )

        # load and save permissions data
        await self.__load_and_save_hierarchy_permissions(
            hierarchy_id, async_channel
        )

        # load and save levels data
        await self.__load_and_save_hierarchy_levels(hierarchy_id, async_channel)

        # load and save obj data
        await self.__load_and_save_hierarchy_obj(async_channel)

        # load and save node_data data
        await self.__load_and_save_hierarchy_node_data(async_channel)

    async def refresh_all_hierarchies_indexes(self):
        await self.__stage_1_clear_hierarchies_index()
        await self.__stage_1_clear_hierarchies_level_index()
        await self.__stage_1_clear_hierarchies_obj_index()
        await self.__stage_1_clear_hierarchies_node_data_index()
        # get all hierarchies

        async with grpc.aio.insecure_channel(
            f"{HIERARCHY_HOST}:{HIERARCHY_GRPC_PORT}"
        ) as async_channel:
            all_hierarchies = []

            # get all hierarchies
            async for hierarchy_chunk in get_all_hierarchies_as_dicts_by_grpc(
                async_channel
            ):
                all_hierarchies.extend(hierarchy_chunk)
            print(all_hierarchies)
            for hierarchy in all_hierarchies:
                hierarchy_id = hierarchy.get("id")
                await self._load_and_save_data_for_special_hierarchy(
                    int(hierarchy_id), async_channel
                )

    async def refresh_index_for_special_hierarchy(self, hierarchy_id: int):
        """Refresh all data for special tmo_id"""
        await (
            self.__stage_1_clear_special_hierarchy_data_from_hierarchies_index(
                hierarchy_id
            )
        )
        await self.__stage_1_clear_node_data_for_special_hierarchy_from_node_data_index(
            hierarchy_id
        )
        await self.__stage_1_clear_levels_data_for_special_hierarchy_from_levels_index(
            hierarchy_id
        )
        await self.__stage_1_clear_obj_for_special_hierarchy_from_obj_index(
            hierarchy_id
        )

        async with grpc.aio.insecure_channel(
            f"{HIERARCHY_HOST}:{HIERARCHY_GRPC_PORT}"
        ) as async_channel:
            await self._load_and_save_data_for_special_hierarchy(
                hierarchy_id, async_channel
            )

    async def clear_all_indexes(self):
        await self.__stage_1_clear_hierarchies_index()
        await self.__stage_1_clear_hierarchies_level_index()
        await self.__stage_1_clear_hierarchies_obj_index()
        await self.__stage_1_clear_hierarchies_node_data_index()
