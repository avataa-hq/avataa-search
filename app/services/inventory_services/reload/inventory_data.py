import math
import pickle

from typing import Callable, Iterable, List

import grpc
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk, BulkIndexError
from grpc.aio import Channel
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from elastic.config import (
    INVENTORY_TMO_INDEX_V2,
    INVENTORY_TPRM_INDEX_V2,
    INVENTORY_OBJ_INDEX_PREFIX,
    DEFAULT_SETTING_FOR_MO_INDEXES,
    DEFAULT_SETTING_FOR_TPRM_INDEX,
    DEFAULT_SETTING_FOR_TMO_INDEX,
    INVENTORY_PRM_INDEX,
    DEFAULT_SETTING_FOR_PRM_INDEX,
    INVENTORY_PRM_LINK_INDEX,
    INVENTORY_MO_LINK_INDEX,
)
from elastic.enum_models import (
    SearchOperator,
    ElasticFieldValType,
    InventoryFieldValType,
)
from elastic.pydantic_models import SearchModel
from elastic.query_builder_service.inventory_index.search_query_builder import (
    InventoryIndexQueryBuilder,
)
from elastic.query_builder_service.inventory_index.utils.convert_types_utils import (
    get_corresponding_elastic_data_type,
)
from elastic.query_builder_service.inventory_index.utils.index_utils import (
    get_index_name_by_tmo,
)
from grpc_clients.inventory.getters.getters_without_channel import (
    get_all_tprms_for_special_tmo_id_async_generator,
    get_raw_prm_data_by_tprm_id,
    get_tprm_data_by_tprm_ids,
    get_mo_data_by_mo_ids,
)
from grpc_clients.inventory.protobuf.mo_info import (
    mo_info_pb2_grpc,
    mo_info_pb2,
)
from grpc_clients.inventory.getters.getters_with_channel import (
    get_all_tmo_data_from_inventory_channel_in,
)
from indexes_mapping.inventory.mapping import (
    INVENTORY_OBJ_INDEX_MAPPING,
    INVENTORY_TMO_INDEX_MAPPING,
    INVENTORY_TPRM_INDEX_MAPPING,
    INVENTORY_PARAMETERS_FIELD_NAME,
    INVENTORY_PRM_INDEX_MAPPING,
    INVENTORY_PRM_AND_MO_LINK_INDEX_MAPPING,
    INVENTORY_FUZZY_FIELD_NAME,
)
from services.inventory_services.converters.val_type_converter import (
    get_convert_function_by_val_type,
    get_convert_function_by_val_type_for_multiple_values,
    get_convert_function_by_val_type_for_multiple_pickled_values,
)
from services.inventory_services.kafka.consumers.inventory_changes.helpers.mo_utils import (
    normalize_geometry,
)
from services.inventory_services.models import InventoryFuzzySearchFields

from settings.config import INVENTORY_HOST, INVENTORY_GRPC_PORT
from v2.database.schema import InventoryObjIndexLoadOrder, LoadStatus


class InventoryIndexesReloader:
    def __init__(
        self, elastic_client: AsyncElasticsearch, session: AsyncSession
    ):
        self.elastic_client = elastic_client
        self.session = session

    async def __stage_1_clear_prm_link_index(self):
        """Deletes current INVENTORY_PRM_LINK_INDEX from elastic search and creates
        new INVENTORY_PRM_LINK_INDEX index"""
        await self.elastic_client.indices.delete(
            index=INVENTORY_PRM_LINK_INDEX, ignore_unavailable=True
        )
        await self.elastic_client.indices.create(
            index=INVENTORY_PRM_LINK_INDEX,
            mappings=INVENTORY_PRM_AND_MO_LINK_INDEX_MAPPING,
            settings=DEFAULT_SETTING_FOR_PRM_INDEX,
        )

    async def __stage_1_clear_mo_link_index(self):
        """Deletes current INVENTORY_MO_LINK_INDEX from elastic search and creates
        new INVENTORY_MO_LINK_INDEX index"""
        await self.elastic_client.indices.delete(
            index=INVENTORY_MO_LINK_INDEX, ignore_unavailable=True
        )
        await self.elastic_client.indices.create(
            index=INVENTORY_MO_LINK_INDEX,
            mappings=INVENTORY_PRM_AND_MO_LINK_INDEX_MAPPING,
            settings=DEFAULT_SETTING_FOR_PRM_INDEX,
        )

    async def __stage_1_clear_prm_index(self):
        """Deletes current INVENTORY_PRM_INDEX from elastic search and creates
        new INVENTORY_PRM_INDEX index"""
        await self.elastic_client.indices.delete(
            index=INVENTORY_PRM_INDEX, ignore_unavailable=True
        )
        await self.elastic_client.indices.create(
            index=INVENTORY_PRM_INDEX,
            mappings=INVENTORY_PRM_INDEX_MAPPING,
            settings=DEFAULT_SETTING_FOR_PRM_INDEX,
        )

    async def __stage_1_clear_tmo_index(self):
        """Deletes current INVENTORY_TMO_INDEX_V2 from elastic search and creates
        new INVENTORY_TMO_INDEX_V2 index"""
        await self.elastic_client.indices.delete(
            index=INVENTORY_TMO_INDEX_V2, ignore_unavailable=True
        )
        await self.elastic_client.indices.create(
            index=INVENTORY_TMO_INDEX_V2,
            mappings=INVENTORY_TMO_INDEX_MAPPING,
            settings=DEFAULT_SETTING_FOR_TMO_INDEX,
        )

    async def __stage_1_clear_tprm_index(self):
        """Deletes current INVENTORY_TPRM_INDEX_V2 from elastic search and creates
        new INVENTORY_TPRM_INDEX_V2 index"""
        await self.elastic_client.indices.delete(
            index=INVENTORY_TPRM_INDEX_V2, ignore_unavailable=True
        )
        await self.elastic_client.indices.create(
            index=INVENTORY_TPRM_INDEX_V2,
            mappings=INVENTORY_TPRM_INDEX_MAPPING,
            settings=DEFAULT_SETTING_FOR_TPRM_INDEX,
        )

    async def __stage_1_delete_all_mo_index(self):
        """Deletes all mo indexes with prefix INVENTORY_OBJ_INDEX_PREFIX"""
        all_indexes = await self.elastic_client.indices.get_alias(index="*")
        indexes_to_delete = [
            index_name
            for index_name in all_indexes
            if index_name.startswith(INVENTORY_OBJ_INDEX_PREFIX)
        ]
        if indexes_to_delete:
            delete_per_step = 10
            count_of_indexes = len(indexes_to_delete)
            steps = math.ceil(count_of_indexes / delete_per_step)
            for step in range(steps):
                start = step * delete_per_step
                end = start + delete_per_step
                to_delete = indexes_to_delete[start:end]
                await self.elastic_client.indices.delete(
                    index=to_delete, ignore_unavailable=True
                )

    async def __stage_2_load_tmo_index_and_create_load_order(self):
        all_tmo = await get_all_tmo_data_from_inventory_channel_in()
        delete_stmt = delete(InventoryObjIndexLoadOrder)
        await self.session.execute(delete_stmt)

        actions = list()
        for item in all_tmo:
            actions.append(
                dict(
                    _index=INVENTORY_TMO_INDEX_V2,
                    _op_type="create",
                    _id=item["id"],
                    _source=item,
                )
            )
            db_item = InventoryObjIndexLoadOrder(
                tmo_id=item["id"], load_status=LoadStatus.NOT_IN_PROGRESS.value
            )
            self.session.add(db_item)
        try:
            await async_bulk(
                client=self.elastic_client, refresh="true", actions=actions
            )
        except BulkIndexError as e:
            print(e.errors)
            raise e
        await self.session.commit()

    async def delete_tmo_data_in_tmo_index(self, tmo_id: int):
        """Deletes data for special tmo_id in INVENTORY_TMO_INDEX_V2"""
        delete_model = SearchModel(
            column_name="id",
            operator=SearchOperator.EQUALS.value,
            column_type=ElasticFieldValType.INTEGER.value,
            multiple=False,
            value=tmo_id,
        )
        delete_query = InventoryIndexQueryBuilder(
            search_list_order=[delete_model]
        )
        await self.elastic_client.delete_by_query(
            index=INVENTORY_TMO_INDEX_V2,
            query=delete_query.create_query_as_dict(),
            ignore_unavailable=True,
            refresh=True,
        )

    async def delete_tmo_data_in_tprm_index(self, tmo_id: int):
        """Deletes data for special tmo_id in INVENTORY_TPRM_INDEX_V2"""
        delete_model = SearchModel(
            column_name="tmo_id",
            operator=SearchOperator.EQUALS.value,
            column_type=ElasticFieldValType.INTEGER.value,
            multiple=False,
            value=tmo_id,
        )
        delete_query = InventoryIndexQueryBuilder(
            search_list_order=[delete_model]
        )
        await self.elastic_client.delete_by_query(
            index=INVENTORY_TPRM_INDEX_V2,
            query=delete_query.create_query_as_dict(),
            ignore_unavailable=True,
            refresh=True,
        )

    async def delete_mo_link_data_in_mo_link_index(self, tmo_id: int):
        """Deletes data for special tmo_id in INVENTORY_MO_LINK_INDEX"""
        search_model_1 = SearchModel(
            column_name="tmo_id",
            operator=SearchOperator.EQUALS.value,
            column_type=ElasticFieldValType.LONG.value,
            multiple=False,
            value=tmo_id,
        )
        search_model_2 = SearchModel(
            column_name="val_type",
            operator=SearchOperator.EQUALS.value,
            column_type=ElasticFieldValType.KEYWORD.value,
            multiple=False,
            value=InventoryFieldValType.MO_LINK.value,
        )
        search_query = InventoryIndexQueryBuilder(
            search_list_order=[search_model_1, search_model_2]
        )
        search_query = search_query.create_query_as_dict()

        all_tprms = await self.elastic_client.search(
            index=INVENTORY_TPRM_INDEX_V2,
            query=search_query,
            track_total_hits=True,
            size=10000,
            source_includes="id",
        )
        all_tprms_ids = [
            item["_source"]["id"] for item in all_tprms["hits"]["hits"]
        ]

        if all_tprms_ids:
            delete_query = {"terms": {"tprm_id": all_tprms_ids}}
            await self.elastic_client.delete_by_query(
                index=INVENTORY_MO_LINK_INDEX,
                query=delete_query,
                ignore_unavailable=True,
                refresh=True,
            )

    async def delete_prm_link_data_in_prm_link_index(self, tmo_id: int):
        """Deletes data for special tmo_id in INVENTORY_PRM_LINK_INDEX"""
        search_model_1 = SearchModel(
            column_name="tmo_id",
            operator=SearchOperator.EQUALS.value,
            column_type=ElasticFieldValType.LONG.value,
            multiple=False,
            value=tmo_id,
        )
        search_model_2 = SearchModel(
            column_name="val_type",
            operator=SearchOperator.EQUALS.value,
            column_type=ElasticFieldValType.KEYWORD.value,
            multiple=False,
            value=InventoryFieldValType.PRM_LINK.value,
        )
        search_query = InventoryIndexQueryBuilder(
            search_list_order=[search_model_1, search_model_2]
        )
        search_query = search_query.create_query_as_dict()

        all_tprms = await self.elastic_client.search(
            index=INVENTORY_TPRM_INDEX_V2,
            query=search_query,
            track_total_hits=True,
            size=10000,
            source_includes="id",
        )
        all_tprms_ids = [
            item["_source"]["id"] for item in all_tprms["hits"]["hits"]
        ]

        if all_tprms_ids:
            delete_query = {"terms": {"tprm_id": all_tprms_ids}}
            await self.elastic_client.delete_by_query(
                index=INVENTORY_PRM_LINK_INDEX,
                query=delete_query,
                ignore_unavailable=True,
                refresh=True,
            )

    async def delete_tmo_data_mo_index(self, tmo_id: int):
        """Deletes mo index for special tmo_id"""
        index_to_delete = get_index_name_by_tmo(tmo_id=tmo_id)
        await self.elastic_client.indices.delete(
            index=index_to_delete, ignore_unavailable=True
        )

    async def __get_not_existing_tprms_in_index_from_array(
        self, list_of_tprms_ids: List[int]
    ):
        pass

    @staticmethod
    async def __get_tprm_data_from_inventory_by_tmo_id(
        tmo_id: int, async_channel: Channel
    ) -> dict:
        """Gets tprm data from inventory and sorts them by temporary_tprm_mo_link_cache,
        temporary_tprm_prm_link_cache,
        temporary_other_tprm_cache"""

        temporary_tprm_mo_link_cache = dict()
        temporary_tprm_prm_link_cache = dict()
        temporary_other_tprm_cache = dict()

        mo_link_and_prm_link_val_types = {
            InventoryFieldValType.PRM_LINK.value,
            InventoryFieldValType.MO_LINK.value,
            InventoryFieldValType.TWO_WAY_MO_LINK.value,
        }
        async for (
            tprm_chunk
        ) in get_all_tprms_for_special_tmo_id_async_generator(
            tmo_id=tmo_id, async_channel=async_channel
        ):
            for tprm_item in tprm_chunk:
                tprm_id_as_str = str(tprm_item["id"])
                tprm_val_type = tprm_item["val_type"]

                if tprm_val_type not in mo_link_and_prm_link_val_types:
                    temporary_other_tprm_cache[tprm_id_as_str] = tprm_item
                elif tprm_val_type == InventoryFieldValType.PRM_LINK.value:
                    temporary_tprm_prm_link_cache[tprm_id_as_str] = tprm_item
                else:
                    # tprm_val_type == InventoryFieldValType.MO_LINK.value
                    temporary_tprm_mo_link_cache[tprm_id_as_str] = tprm_item

        return {
            "temporary_tprm_mo_link_cache": temporary_tprm_mo_link_cache,
            "temporary_tprm_prm_link_cache": temporary_tprm_prm_link_cache,
            "temporary_other_tprm_cache": temporary_other_tprm_cache,
        }

    async def __save_tprms_with_val_type_mo_link_and_their_prms(
        self, tmo_id: int, mo_link_tprms: Iterable[dict], async_channel: Channel
    ):
        """Saves mo_link tprms for special tmo_id"""
        tmo_index_name = get_index_name_by_tmo(tmo_id=tmo_id)
        actions = list()
        delete_before_load = list()
        new_properties_mapping = {}

        for tprm_item in mo_link_tprms:
            delete_before_load.append(tprm_item["id"])
            tprm_id_as_str = str(tprm_item["id"])

            new_properties_mapping[tprm_id_as_str] = {
                "type": get_corresponding_elastic_data_type(
                    InventoryFieldValType.STR.value
                )
            }
            actions.append(
                dict(
                    _index=INVENTORY_TPRM_INDEX_V2,
                    _op_type="index",
                    _id=tprm_item["id"],
                    _source=tprm_item,
                )
            )
        if delete_before_load:
            delete_query = {"terms": {"id": delete_before_load}}
            await self.elastic_client.delete_by_query(
                index=INVENTORY_TPRM_INDEX_V2,
                query=delete_query,
                ignore_unavailable=True,
                refresh=True,
            )
        if actions:
            await async_bulk(
                client=self.elastic_client, refresh="true", actions=actions
            )

        if new_properties_mapping:
            properties = {
                INVENTORY_PARAMETERS_FIELD_NAME: {
                    "type": "object",
                    "properties": new_properties_mapping,
                }
            }

            await self.elastic_client.indices.put_mapping(
                index=tmo_index_name, properties=properties
            )

        # same converted prm values into INVENTORY_MO_LINK_INDEX and not converted values into INVENTORY_PRM_INDEX
        for tprm_item in mo_link_tprms:
            is_multiple = tprm_item["multiple"]
            if is_multiple:
                convert_function = get_convert_function_by_val_type_for_multiple_pickled_values(
                    InventoryFieldValType.INT.value
                )
            else:
                convert_function = get_convert_function_by_val_type(
                    InventoryFieldValType.INT.value
                )

            async for prm_chunk in get_raw_prm_data_by_tprm_id(
                tprm_id=tprm_item["id"], async_channel=async_channel
            ):
                actions = list()
                for item in prm_chunk.prms:
                    prm_action = dict(
                        _index=INVENTORY_PRM_INDEX,
                        _op_type="index",
                        _id=item.id,
                        _source={
                            "id": item.id,
                            "version": item.version,
                            "tprm_id": item.tprm_id,
                            "mo_id": item.mo_id,
                            "value": item.value,
                        },
                    )

                    prm_mo_link_action = dict(
                        _index=INVENTORY_MO_LINK_INDEX,
                        _op_type="index",
                        _id=item.id,
                        _source={
                            "id": item.id,
                            "version": item.version,
                            "tprm_id": item.tprm_id,
                            "mo_id": item.mo_id,
                            "value": convert_function(item.value),
                        },
                    )
                    actions.append(prm_action)
                    actions.append(prm_mo_link_action)

                try:
                    await async_bulk(
                        client=self.elastic_client,
                        refresh="true",
                        actions=actions,
                    )
                except BulkIndexError as e:
                    print(e.errors)
                    raise ImportError(
                        f"Can`t import PRM data for TPRM with id = {tprm_item['id']}"
                    )

    async def __save_tprms_with_val_type_prm_link_and_their_prms(
        self,
        tmo_id: int,
        prm_link_tprms: Iterable[dict],
        async_channel: Channel,
    ):
        """Saves prm_link tprms for special tmo_id"""
        tmo_index_name = get_index_name_by_tmo(tmo_id=tmo_id)
        new_properties_mapping = {}

        tprm_ids_prm_of_which_must_be_loaded_to_prm_link_index = list()
        tprm_ids_prm_of_which_must_be_loaded_to_prm_index = list()
        tprm_ids_info_of_which_must_be_loaded = list()
        prm_link_tprms_ids = list()

        for tprm_item in prm_link_tprms:
            tprm_ids_prm_of_which_must_be_loaded_to_prm_link_index.append(
                tprm_item["id"]
            )
            linked_tprm_from_tprm_constraint = tprm_item["constraint"]
            prm_link_tprms_ids.append(tprm_item["id"])
            if linked_tprm_from_tprm_constraint:
                linked_tprm_in_constraint = int(
                    linked_tprm_from_tprm_constraint
                )
                tprm_ids_prm_of_which_must_be_loaded_to_prm_index.append(
                    linked_tprm_in_constraint
                )
                tprm_ids_info_of_which_must_be_loaded.append(
                    linked_tprm_in_constraint
                )

        # same converted prm values into INVENTORY_PRM_LINK_INDEX and not converted values into INVENTORY_PRM_INDEX
        if prm_link_tprms:
            delete_query = {"terms": {"tprm_id": prm_link_tprms_ids}}
            await self.elastic_client.delete_by_query(
                index=[INVENTORY_PRM_LINK_INDEX, INVENTORY_PRM_INDEX],
                query=delete_query,
                ignore_unavailable=True,
                refresh=True,
            )

            for tprm_item in prm_link_tprms:
                is_multiple = tprm_item["multiple"]
                if is_multiple:
                    convert_function = get_convert_function_by_val_type_for_multiple_pickled_values(
                        InventoryFieldValType.INT.value
                    )
                else:
                    convert_function = get_convert_function_by_val_type(
                        InventoryFieldValType.INT.value
                    )
                async for prm_chunk in get_raw_prm_data_by_tprm_id(
                    tprm_id=tprm_item["id"], async_channel=async_channel
                ):
                    actions = list()
                    for item in prm_chunk.prms:
                        prm_action = dict(
                            _index=INVENTORY_PRM_INDEX,
                            _op_type="index",
                            _id=item.id,
                            _source={
                                "id": item.id,
                                "version": item.version,
                                "tprm_id": item.tprm_id,
                                "mo_id": item.mo_id,
                                "value": item.value,
                            },
                        )

                        prm_mo_link_action = dict(
                            _index=INVENTORY_PRM_LINK_INDEX,
                            _op_type="index",
                            _id=item.id,
                            _source={
                                "id": item.id,
                                "version": item.version,
                                "tprm_id": item.tprm_id,
                                "mo_id": item.mo_id,
                                "value": convert_function(item.value),
                            },
                        )
                        actions.append(prm_action)
                        actions.append(prm_mo_link_action)

                    try:
                        await async_bulk(
                            client=self.elastic_client,
                            refresh="true",
                            actions=actions,
                        )
                    except Exception as e:
                        print(e)
                        raise ImportError(
                            f"Can`t import PRM data for TPRM with id = {tprm_item['id']}"
                        )

        # load data tprm_ids_prm_of_which_must_be_loaded_to_prm_index
        if tprm_ids_prm_of_which_must_be_loaded_to_prm_index:
            delete_query = {
                "terms": {
                    "tprm_id": tprm_ids_prm_of_which_must_be_loaded_to_prm_index
                }
            }
            await self.elastic_client.delete_by_query(
                index=INVENTORY_PRM_INDEX,
                query=delete_query,
                ignore_unavailable=True,
                refresh=True,
            )

            for tprm_id in tprm_ids_prm_of_which_must_be_loaded_to_prm_index:
                async for prm_chunk in get_raw_prm_data_by_tprm_id(
                    tprm_id=tprm_id, async_channel=async_channel
                ):
                    actions = [
                        dict(
                            _index=INVENTORY_PRM_INDEX,
                            _op_type="index",
                            _id=item.id,
                            _source={
                                "id": item.id,
                                "version": item.version,
                                "tprm_id": item.tprm_id,
                                "mo_id": item.mo_id,
                                "value": item.value,
                            },
                        )
                        for item in prm_chunk.prms
                    ]
                    try:
                        await async_bulk(
                            client=self.elastic_client,
                            refresh="true",
                            actions=actions,
                        )
                    except Exception:
                        raise ImportError(
                            f"Can`t import PRM data for TPRM with id = {tprm_id}"
                        )

        # add tprm with val_type prm_link into INVENTORY_TPRM_INDEX_V2
        if prm_link_tprms_ids:
            delete_query = {"terms": {"id": prm_link_tprms_ids}}
            await self.elastic_client.delete_by_query(
                index=INVENTORY_TPRM_INDEX_V2,
                query=delete_query,
                ignore_unavailable=True,
                refresh=True,
            )
            actions = list()
            for tprm_item in prm_link_tprms:
                actions.append(
                    dict(
                        _index=INVENTORY_TPRM_INDEX_V2,
                        _op_type="index",
                        _id=tprm_item["id"],
                        _source=tprm_item,
                    )
                )
            await async_bulk(
                client=self.elastic_client, refresh="true", actions=actions
            )

        # add tprms referenced by tprms with val_type prm_link into INVENTORY_TPRM_INDEX_V2

        referenced_tprms = dict()
        if tprm_ids_info_of_which_must_be_loaded:
            delete_query = {
                "terms": {"id": tprm_ids_info_of_which_must_be_loaded}
            }
            await self.elastic_client.delete_by_query(
                index=INVENTORY_TPRM_INDEX_V2,
                query=delete_query,
                ignore_unavailable=True,
                refresh=True,
            )

            async for tprm_chunk in get_tprm_data_by_tprm_ids(
                tprm_ids=tprm_ids_info_of_which_must_be_loaded,
                async_channel=async_channel,
            ):
                actions = list()
                for tprm_item in tprm_chunk.tprms_data:
                    tprm_item = pickle.loads(bytes.fromhex(tprm_item))
                    referenced_tprms[str(tprm_item["id"])] = tprm_item

                    actions.append(
                        dict(
                            _index=INVENTORY_TPRM_INDEX_V2,
                            _op_type="index",
                            _id=tprm_item["id"],
                            _source=tprm_item,
                        )
                    )
                await async_bulk(
                    client=self.elastic_client, refresh="true", actions=actions
                )

        # add mapping for prm_links
        new_prm_link_properties_mapping = dict()
        for tprm_prm_link_data in prm_link_tprms:
            tprm_from_constrain = tprm_prm_link_data["constraint"]
            if tprm_from_constrain:
                new_val_type = referenced_tprms[tprm_from_constrain]
                new_val_type = new_val_type["val_type"]

                new_prm_link_properties_mapping[
                    str(tprm_prm_link_data["id"])
                ] = {"type": get_corresponding_elastic_data_type(new_val_type)}

        if new_prm_link_properties_mapping:
            new_properties_mapping.update(new_prm_link_properties_mapping)

        if new_properties_mapping:
            properties = {
                INVENTORY_PARAMETERS_FIELD_NAME: {
                    "type": "object",
                    "properties": new_properties_mapping,
                }
            }

            await self.elastic_client.indices.put_mapping(
                index=tmo_index_name, properties=properties
            )

    async def __save_tprms_with_val_type_not_eq_prm_link_or_mo_link(
        self, tmo_id: int, tprms_not_mo_link_no_prm_link: Iterable[dict]
    ):
        tmo_index_name = get_index_name_by_tmo(tmo_id=tmo_id)
        new_properties_mapping = {}
        actions = list()
        delete_before_load = list()
        for tprm_item in tprms_not_mo_link_no_prm_link:
            delete_before_load.append(tprm_item["id"])
            tprm_id_as_str = str(tprm_item["id"])
            tprm_val_type = tprm_item["val_type"]

            corr_val_type = get_corresponding_elastic_data_type(tprm_val_type)

            if corr_val_type == "date":
                new_properties_mapping[tprm_id_as_str] = {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||strict_date_optional_time||epoch_millis",
                }
            else:
                new_properties_mapping[tprm_id_as_str] = {
                    "type": get_corresponding_elastic_data_type(tprm_val_type)
                }

            actions.append(
                dict(
                    _index=INVENTORY_TPRM_INDEX_V2,
                    _op_type="index",
                    _id=tprm_item["id"],
                    _source=tprm_item,
                )
            )
        if actions:
            delete_query = {"terms": {"id": delete_before_load}}
            await self.elastic_client.delete_by_query(
                index=INVENTORY_TPRM_INDEX_V2,
                query=delete_query,
                ignore_unavailable=True,
                refresh=True,
            )

            await async_bulk(
                client=self.elastic_client, refresh="true", actions=actions
            )

        if new_properties_mapping:
            properties = {
                INVENTORY_PARAMETERS_FIELD_NAME: {
                    "type": "object",
                    "properties": new_properties_mapping,
                }
            }

            await self.elastic_client.indices.put_mapping(
                index=tmo_index_name, properties=properties
            )

    async def __mo_tprm_value_updater_for_mo_link_with_multiple_false(
        self,
        mo_data_index_name: str,
        elastic_search_results: dict,
        convert_function: Callable,
        async_channel: Channel,
    ):
        cache_of_changes = []
        mo_ids_to_request_names = set()
        for prm_data in elastic_search_results["hits"]["hits"]:
            prm_data = prm_data["_source"]
            converted_value = convert_function(prm_data["value"])
            mo_ids_to_request_names.add(converted_value)
            prm_data["value"] = converted_value
            cache_of_changes.append(prm_data)
        mo_data_cache = dict()
        async for mo_data_chunk in get_mo_data_by_mo_ids(
            async_channel=async_channel, mo_ids=list(mo_ids_to_request_names)
        ):
            mo_data_to_cache = {
                mo_data.id: mo_data.name for mo_data in mo_data_chunk.list_of_mo
            }
            mo_data_cache.update(mo_data_to_cache)

        new_cache_of_changes = []
        for change_mo_link_value in cache_of_changes:
            new_value = mo_data_cache.get(change_mo_link_value["value"])
            if new_value:
                change_mo_link_value["value"] = new_value
                new_cache_of_changes.append(change_mo_link_value)
        actions = []

        for new_change in new_cache_of_changes:
            data = {
                "parameters": {str(new_change["tprm_id"]): new_change["value"]}
            }
            action_item = dict(
                _index=mo_data_index_name,
                _op_type="update",
                _id=new_change["mo_id"],
                doc=data,
            )
            actions.append(action_item)
        await async_bulk(
            client=self.elastic_client, refresh="true", actions=actions
        )

    async def __mo_tprm_value_updater_for_mo_link_with_multiple_true(
        self,
        mo_data_index_name: str,
        elastic_search_results: dict,
        convert_function: Callable,
        async_channel: Channel,
    ):
        cache_of_changes = []
        mo_ids_to_request_names = set()
        for prm_data in elastic_search_results["hits"]["hits"]:
            prm_data = prm_data["_source"]
            un_pickled_value = pickle.loads(bytes.fromhex(prm_data["value"]))
            converted_value = convert_function(un_pickled_value)
            for value in converted_value:
                mo_ids_to_request_names.add(value)
            prm_data["value"] = converted_value
            cache_of_changes.append(prm_data)
        mo_data_cache = dict()
        async for mo_data_chunk in get_mo_data_by_mo_ids(
            async_channel=async_channel, mo_ids=list(mo_ids_to_request_names)
        ):
            mo_data_to_cache = {
                mo_data.id: mo_data.name for mo_data in mo_data_chunk.list_of_mo
            }
            mo_data_cache.update(mo_data_to_cache)

        new_cache_of_changes = []
        for change_mo_link_value in cache_of_changes:
            new_value = [
                mo_name
                for linked_mo_id in change_mo_link_value["value"]
                if (mo_name := mo_data_cache.get(linked_mo_id))
            ]
            if new_value:
                change_mo_link_value["value"] = new_value
                new_cache_of_changes.append(change_mo_link_value)
        actions = []

        for new_change in new_cache_of_changes:
            data = {
                "parameters": {str(new_change["tprm_id"]): new_change["value"]}
            }
            action_item = dict(
                _index=mo_data_index_name,
                _op_type="update",
                _id=new_change["mo_id"],
                doc=data,
            )
            actions.append(action_item)
        await async_bulk(
            client=self.elastic_client, refresh="true", actions=actions
        )

    @staticmethod
    def __prm_handler_base_prm_multiply_true_corresp_tprm_multiple_true(
        ids_and_values_of_corresponding_prms: dict,
        modifications_of_mo_params: List[dict],
    ):
        """Returns modified mo_data if base prm multiple true and corresponding tprm multiple true"""
        modified_mo_data = list()
        for mo_data_to_modify in modifications_of_mo_params:
            new_value = list()
            for corresp_id in mo_data_to_modify["value"]:
                corresp_value = ids_and_values_of_corresponding_prms.get(
                    corresp_id
                )
                if corresp_value:
                    new_value.extend(corresp_value)
            if new_value:
                mo_data_to_modify["value"] = new_value
                modified_mo_data.append(mo_data_to_modify)
        return modified_mo_data

    @staticmethod
    def __prm_handler_base_prm_multiply_true_corresp_tprm_multiple_false(
        ids_and_values_of_corresponding_prms: dict,
        modifications_of_mo_params: List[dict],
    ):
        """Returns modified mo_data if base prm multiple true and corresponding tprm multiple false"""
        modified_mo_data = list()
        for mo_data_to_modify in modifications_of_mo_params:
            new_value = list()
            for corresp_id in mo_data_to_modify["value"]:
                corresp_value = ids_and_values_of_corresponding_prms.get(
                    corresp_id
                )
                if corresp_value:
                    new_value.append(corresp_value)
            if new_value:
                mo_data_to_modify["value"] = new_value
                modified_mo_data.append(mo_data_to_modify)
        return modified_mo_data

    @staticmethod
    def __prm_handler_base_prm_multiply_false_corresp_tprm_multiple_true_or_false(
        ids_and_values_of_corresponding_prms: dict,
        modifications_of_mo_params: List[dict],
    ):
        """Returns modified mo_data if base prm multiple false and corresponding tprm multiple true or false"""
        modified_mo_data = list()
        for mo_data_to_modify in modifications_of_mo_params:
            corresp_id = mo_data_to_modify["value"]
            new_value = ids_and_values_of_corresponding_prms.get(corresp_id)
            if new_value:
                mo_data_to_modify["value"] = new_value
                modified_mo_data.append(mo_data_to_modify)
        return modified_mo_data

    async def __load_tprm_and_mo_data_by_tmo_id_version2(
        self, tmo_id: int, async_channel: Channel
    ):
        new_index_name = get_index_name_by_tmo(tmo_id=tmo_id)

        await self.elastic_client.indices.create(
            index=new_index_name,
            mappings=INVENTORY_OBJ_INDEX_MAPPING,
            settings=DEFAULT_SETTING_FOR_MO_INDEXES,
        )

        dict_of_loaded_tprms = (
            await self.__get_tprm_data_from_inventory_by_tmo_id(
                tmo_id=tmo_id, async_channel=async_channel
            )
        )

        mo_link_tprms = dict_of_loaded_tprms.get("temporary_tprm_mo_link_cache")
        prm_link_tprms = dict_of_loaded_tprms.get(
            "temporary_tprm_prm_link_cache"
        )
        tprms_not_mo_link_no_prm_link = dict_of_loaded_tprms.get(
            "temporary_other_tprm_cache"
        )

        if mo_link_tprms:
            await self.__save_tprms_with_val_type_mo_link_and_their_prms(
                tmo_id=tmo_id,
                mo_link_tprms=mo_link_tprms.values(),
                async_channel=async_channel,
            )
        if prm_link_tprms:
            await self.__save_tprms_with_val_type_prm_link_and_their_prms(
                tmo_id=tmo_id,
                prm_link_tprms=prm_link_tprms.values(),
                async_channel=async_channel,
            )
        if tprms_not_mo_link_no_prm_link:
            await self.__save_tprms_with_val_type_not_eq_prm_link_or_mo_link(
                tmo_id=tmo_id,
                tprms_not_mo_link_no_prm_link=tprms_not_mo_link_no_prm_link.values(),
            )

        stub = mo_info_pb2_grpc.InformerStub(async_channel)
        msg = mo_info_pb2.GetAllMOWithParamsByTMOIdRequest(tmo_id=tmo_id)
        grpc_response = stub.GetAllMOWithParamsByTMOId(msg)

        ids_of_int_tprms = {
            k: v
            for k, v in tprms_not_mo_link_no_prm_link.items()
            if v["val_type"] == "int"
        }
        LONG_TYPE_MAX = 9223372036854775807

        async for grpc_chunk in grpc_response:
            actions = []
            for item in grpc_chunk.mos_with_params:
                add_to_elastic = True
                parameters_out_of_range = list()

                item = pickle.loads(bytes.fromhex(item))

                params = dict()
                for param in item["params"]:
                    if param["tprm_id"] in tprms_not_mo_link_no_prm_link:
                        param_tprm_id = param["tprm_id"]
                        if param_tprm_id in ids_of_int_tprms:
                            is_multiple = ids_of_int_tprms[param_tprm_id][
                                "multiple"
                            ]

                            if is_multiple:
                                not_greater = list()
                                for v in param["value"]:
                                    if v > LONG_TYPE_MAX:
                                        add_to_elastic = False
                                        parameters_out_of_range.append(param)
                                    else:
                                        not_greater.append(v)
                                if not_greater:
                                    params[param_tprm_id] = not_greater

                            else:
                                if param["value"] > LONG_TYPE_MAX:
                                    add_to_elastic = False
                                    parameters_out_of_range.append(param)
                                else:
                                    params[param_tprm_id] = param["value"]

                        else:
                            params[param_tprm_id] = param["value"]

                if not add_to_elastic:
                    print(
                        f"One or more parameters are outside the range of long: MO.id = {item['id']}, "
                        f"parameters: {parameters_out_of_range}"
                    )

                item[INVENTORY_PARAMETERS_FIELD_NAME] = params

                geometry = item.get("geometry")
                normalized = normalize_geometry(geometry)
                if normalized:
                    item["geometry"] = normalized
                else:
                    item.pop("geometry", None)

                del item["params"]

                # add fields for fuzzy search
                fuzzy_search_data = dict()
                for enum_item in InventoryFuzzySearchFields:
                    field_name = enum_item.value
                    field_value = item.get(field_name)
                    fuzzy_search_data[field_name] = field_value

                if fuzzy_search_data:
                    item[INVENTORY_FUZZY_FIELD_NAME] = fuzzy_search_data

                action_item = dict(
                    _index=new_index_name,
                    _op_type="index",
                    _id=item["id"],
                    _source=item,
                )
                actions.append(action_item)
            try:
                await async_bulk(
                    client=self.elastic_client, refresh="true", actions=actions
                )
            except Exception as e:
                print(str(e))
                raise ImportError(
                    f"Can`t import data for tmo with id = {tmo_id}"
                )

        # if was mo_links update mo params
        if mo_link_tprms:
            for _, tprm_data in mo_link_tprms.items():
                data_per_step = 10000
                search_query = {"match": {"tprm_id": tprm_data["id"]}}
                is_multiple = tprm_data["multiple"]
                if is_multiple:
                    convert_function = (
                        get_convert_function_by_val_type_for_multiple_values(
                            InventoryFieldValType.INT.value
                        )
                    )
                else:
                    convert_function = get_convert_function_by_val_type(
                        InventoryFieldValType.INT.value
                    )

                search_res = await self.elastic_client.search(
                    query=search_query,
                    index=INVENTORY_PRM_INDEX,
                    ignore_unavailable=True,
                    track_total_hits=True,
                    size=0,
                )

                total_hits = search_res["hits"]["total"]["value"]
                if total_hits:
                    steps = math.ceil(total_hits / data_per_step)

                    for step in range(steps):
                        start = step * data_per_step
                        search_res = await self.elastic_client.search(
                            query=search_query,
                            index=INVENTORY_PRM_INDEX,
                            from_=start,
                            ignore_unavailable=True,
                            track_total_hits=True,
                            size=data_per_step,
                        )
                        if search_res["hits"]["total"]["value"]:
                            if not is_multiple:
                                await self.__mo_tprm_value_updater_for_mo_link_with_multiple_false(
                                    mo_data_index_name=new_index_name,
                                    elastic_search_results=search_res,
                                    convert_function=convert_function,
                                    async_channel=async_channel,
                                )
                            else:
                                await self.__mo_tprm_value_updater_for_mo_link_with_multiple_true(
                                    mo_data_index_name=new_index_name,
                                    elastic_search_results=search_res,
                                    convert_function=convert_function,
                                    async_channel=async_channel,
                                )

        # if was prm_links update mo params
        temporary_tprm_prm_link_cache = dict_of_loaded_tprms.get(
            "temporary_tprm_prm_link_cache"
        )
        for _, tprm_data in temporary_tprm_prm_link_cache.items():
            base_tprm_is_multiple = tprm_data["multiple"]
            base_tprm_id = tprm_data["id"]

            # get info of tprm in constraint
            tprm_in_constraint = int(tprm_data["constraint"])

            search_query = {"match": {"id": tprm_in_constraint}}
            tprm_from_search = await self.elastic_client.search(
                index=INVENTORY_TPRM_INDEX_V2, query=search_query
            )

            if not tprm_from_search["hits"]["total"]["value"]:
                continue

            tprm_from_search = tprm_from_search["hits"]["hits"][0]["_source"]
            corresponding_tprm_val_type = tprm_from_search["val_type"]
            corresponding_tprm_is_multiple = tprm_from_search["multiple"]

            # get prm data modification function
            if base_tprm_is_multiple is False:
                prm_mod_function = self.__prm_handler_base_prm_multiply_false_corresp_tprm_multiple_true_or_false
            else:
                if corresponding_tprm_is_multiple:
                    prm_mod_function = self.__prm_handler_base_prm_multiply_true_corresp_tprm_multiple_true
                else:
                    prm_mod_function = self.__prm_handler_base_prm_multiply_true_corresp_tprm_multiple_false

            data_per_step = 10000
            search_query = {"match": {"tprm_id": base_tprm_id}}
            if base_tprm_is_multiple:
                base_convert_function = get_convert_function_by_val_type_for_multiple_pickled_values(
                    InventoryFieldValType.INT.value
                )
            else:
                base_convert_function = get_convert_function_by_val_type(
                    InventoryFieldValType.INT.value
                )

            search_res = await self.elastic_client.search(
                query=search_query,
                index=INVENTORY_PRM_INDEX,
                ignore_unavailable=True,
                track_total_hits=True,
                size=0,
            )

            total_hits = search_res["hits"]["total"]["value"]

            steps = math.ceil(total_hits / data_per_step)
            # ids of corresponding prms
            for step in range(0, steps):
                start = step * data_per_step
                base_prm_search_res = await self.elastic_client.search(
                    query=search_query,
                    index=INVENTORY_PRM_INDEX,
                    ignore_unavailable=True,
                    track_total_hits=True,
                    from_=start,
                    size=data_per_step,
                )

                join_data_stages = []
                if not base_tprm_is_multiple:
                    ids_of_corresponding_prms = set()
                    modifications_of_mo_params = list()
                    for prm_data in base_prm_search_res["hits"]["hits"]:
                        prm_data = prm_data["_source"]
                        prm_id_value = base_convert_function(prm_data["value"])
                        ids_of_corresponding_prms.add(prm_id_value)
                        prm_data["value"] = prm_id_value
                        modifications_of_mo_params.append(prm_data)
                    join_data_stage = {
                        "ids_of_corresponding_prms": ids_of_corresponding_prms,
                        "modifications_of_mo_params": modifications_of_mo_params,
                    }
                    join_data_stages.append(join_data_stage)
                else:
                    counter = 0
                    ids_of_corresponding_prms = set()
                    modifications_of_mo_params = list()
                    for prm_data in base_prm_search_res["hits"]["hits"]:
                        prm_data = prm_data["_source"]
                        prm_id_values = base_convert_function(prm_data["value"])
                        ids_of_corresponding_prms.update(prm_id_values)
                        prm_data["value"] = prm_id_values
                        counter += len(prm_id_values)
                        modifications_of_mo_params.append(prm_data)

                        if counter >= 10000:
                            join_data_stage = {
                                "ids_of_corresponding_prms": ids_of_corresponding_prms,
                                "modifications_of_mo_params": modifications_of_mo_params,
                            }
                            join_data_stages.append(join_data_stage)
                            ids_of_corresponding_prms = set()
                            modifications_of_mo_params = list()
                            counter = 0

                    if ids_of_corresponding_prms:
                        join_data_stage = {
                            "ids_of_corresponding_prms": ids_of_corresponding_prms,
                            "modifications_of_mo_params": modifications_of_mo_params,
                        }
                        join_data_stages.append(join_data_stage)

                if corresponding_tprm_is_multiple:
                    corresponding_convert_function = get_convert_function_by_val_type_for_multiple_pickled_values(
                        corresponding_tprm_val_type
                    )
                else:
                    corresponding_convert_function = (
                        get_convert_function_by_val_type(
                            corresponding_tprm_val_type
                        )
                    )

                for join_stage in join_data_stages:
                    ids_of_corresponding_prms = join_stage[
                        "ids_of_corresponding_prms"
                    ]
                    modifications_of_mo_params = join_stage[
                        "modifications_of_mo_params"
                    ]

                    stage_search_query = {
                        "terms": {"id": list(ids_of_corresponding_prms)}
                    }

                    stage_prm_search_res = await self.elastic_client.search(
                        query=stage_search_query,
                        index=INVENTORY_PRM_INDEX,
                        ignore_unavailable=True,
                        size=len(ids_of_corresponding_prms),
                    )

                    stage_results = {
                        prm_data["_source"][
                            "id"
                        ]: corresponding_convert_function(
                            prm_data["_source"]["value"]
                        )
                        for prm_data in stage_prm_search_res["hits"]["hits"]
                    }

                    updated_mo_data = prm_mod_function(
                        stage_results, modifications_of_mo_params
                    )

                    # UPDATE NEW VALUES
                    actions = []

                    for mo_data in updated_mo_data:
                        data = {
                            "parameters": {
                                str(mo_data["tprm_id"]): mo_data["value"]
                            }
                        }
                        action_item = dict(
                            _index=new_index_name,
                            _op_type="update",
                            _id=mo_data["mo_id"],
                            doc=data,
                        )
                        actions.append(action_item)
                    await async_bulk(
                        client=self.elastic_client,
                        refresh="true",
                        actions=actions,
                    )

    async def __load_prm_index_by_tprm_id(
        self, tprm_id: int, async_channel: Channel
    ):
        """Load prm data for special tmo_id into INVENTORY_PRM_INDEX_MAPPING"""
        async for prm_chunk in get_raw_prm_data_by_tprm_id(
            tprm_id=tprm_id, async_channel=async_channel
        ):
            actions = [
                dict(
                    _index=INVENTORY_PRM_INDEX,
                    _op_type="index",
                    _id=item.id,
                    _source={
                        "id": item.id,
                        "version": item.version,
                        "tprm_id": item.tprm_id,
                        "mo_id": item.mo_id,
                        "value": item.value,
                    },
                )
                for item in prm_chunk.prms
            ]
            try:
                await async_bulk(
                    client=self.elastic_client, refresh="true", actions=actions
                )
            except Exception:
                raise ImportError(
                    f"Can`t import PRM data for TPRM with id = {tprm_id}"
                )

    async def __full_refresh_dataa_for_one_tmo_in_all_indexes(
        self, async_channel: Channel, tmo_from_order: InventoryObjIndexLoadOrder
    ):
        """FULL REFRESH DATA FOR TMO"""
        # clear if LoadStatus.IN_PROGRESS
        if tmo_from_order.load_status == LoadStatus.IN_PROGRESS.value:
            await self.delete_tmo_data_in_tprm_index(
                tmo_id=tmo_from_order.tmo_id
            )
            await self.delete_tmo_data_mo_index(tmo_id=tmo_from_order.tmo_id)

        else:
            tmo_from_order.load_status = LoadStatus.IN_PROGRESS.value
            self.session.add(tmo_from_order)
            await self.session.commit()

        # create index for inventory objects
        await self.__load_tprm_and_mo_data_by_tmo_id_version2(
            tmo_id=tmo_from_order.tmo_id, async_channel=async_channel
        )

        await self.session.delete(tmo_from_order)
        await self.session.commit()

    async def __stage_5_load_tprm_and_mo_data(self):
        stmt = select(InventoryObjIndexLoadOrder).order_by(
            InventoryObjIndexLoadOrder.id
        )
        all_tmo_in_order = await self.session.execute(stmt)
        all_tmo_in_order = all_tmo_in_order.scalars().all()

        async with grpc.aio.insecure_channel(
            f"{INVENTORY_HOST}:{INVENTORY_GRPC_PORT}",
            options=[
                ("grpc.keepalive_time_ms", 20_000),
                ("grpc.keepalive_timeout_ms", 15_000),
                ("grpc.http2.max_pings_without_data", 5),
                ("grpc.keepalive_permit_without_calls", 1),
            ],
        ) as async_channel:
            for tmo_in_order in all_tmo_in_order:
                await self.__full_refresh_dataa_for_one_tmo_in_all_indexes(
                    async_channel=async_channel, tmo_from_order=tmo_in_order
                )

    async def refresh_all_inventory_indexes(self):
        await self.__stage_1_clear_prm_link_index()
        await self.__stage_1_clear_mo_link_index()
        await self.__stage_1_clear_prm_index()
        await self.__stage_1_clear_tmo_index()
        await self.__stage_1_clear_tprm_index()
        await self.__stage_1_delete_all_mo_index()
        await self.__stage_2_load_tmo_index_and_create_load_order()
        await self.__stage_5_load_tprm_and_mo_data()

    async def refresh_index_by_tmo_id(self, tmo_id: int):
        """Refresh all data for special tmo_id"""
        await self.delete_mo_link_data_in_mo_link_index(tmo_id)
        await self.delete_prm_link_data_in_prm_link_index(tmo_id)
        await self.delete_tmo_data_in_tmo_index(tmo_id)
        await self.delete_tmo_data_in_tprm_index(tmo_id)
        await self.delete_tmo_data_mo_index(tmo_id)

        all_tmo = await get_all_tmo_data_from_inventory_channel_in()
        actions = list()
        for item in all_tmo:
            if item["id"] == tmo_id:
                actions.append(
                    dict(
                        _index=INVENTORY_TMO_INDEX_V2,
                        _op_type="index",
                        _id=item["id"],
                        _source=item,
                    )
                )
        if actions:
            await async_bulk(
                client=self.elastic_client, refresh="true", actions=actions
            )
        async with grpc.aio.insecure_channel(
            f"{INVENTORY_HOST}:{INVENTORY_GRPC_PORT}",
            options=[
                ("grpc.keepalive_time_ms", 20_000),
                ("grpc.keepalive_timeout_ms", 15_000),
                ("grpc.http2.max_pings_without_data", 5),
                ("grpc.keepalive_permit_without_calls", 1),
            ],
        ) as async_channel:
            tmo_in_order = InventoryObjIndexLoadOrder(
                tmo_id=tmo_id, load_status=LoadStatus.NOT_IN_PROGRESS.value
            )
            await self.__full_refresh_dataa_for_one_tmo_in_all_indexes(
                async_channel=async_channel, tmo_from_order=tmo_in_order
            )

    async def clear_all_indexes(self):
        await self.__stage_1_clear_prm_link_index()
        await self.__stage_1_clear_mo_link_index()
        await self.__stage_1_clear_prm_index()
        await self.__stage_1_clear_tmo_index()
        await self.__stage_1_clear_tprm_index()
        await self.__stage_1_delete_all_mo_index()
