import copy
from collections import defaultdict

from elasticsearch import AsyncElasticsearch
from elasticsearch._async.helpers import async_bulk
from elasticsearch.helpers import BulkIndexError

from elastic.config import (
    INVENTORY_TPRM_INDEX_V2,
    INVENTORY_PRM_INDEX,
    INVENTORY_MO_LINK_INDEX,
    ALL_MO_OBJ_INDEXES_PATTERN,
    INVENTORY_PRM_LINK_INDEX,
)
from elastic.enum_models import InventoryFieldValType
from elastic.query_builder_service.inventory_index.utils.index_utils import (
    get_index_name_by_tmo,
)
from indexes_mapping.inventory.mapping import INVENTORY_PARAMETERS_FIELD_NAME
from services.inventory_services.converters.val_type_converter import (
    get_convert_function_by_val_type_for_multiple_pickled_values,
    get_convert_function_by_val_type,
)
from services.inventory_services.kafka.consumers.inventory_changes.converters.val_type_converter import (
    get_convert_func_for_inventory_kafka_msg_for_not_multiple_tprm,
    get_convert_func_for_inventory_kafka_msg_for_multiple_pickled_values,
)
from services.inventory_services.kafka.consumers.inventory_changes.helpers.common_utils import (
    get_all_data_from_special_index,
)
from services.inventory_services.kafka.consumers.inventory_changes.helpers.models import (
    PRMLinkChangeData,
)


class PRMCreateHandler:
    def __init__(self, msg: dict, async_client: AsyncElasticsearch):
        self.kafka_msg = msg
        self.async_client = async_client
        self.kafka_msg_tprm_ids = set()
        self.kafka_msg_mo_ids = set()
        self.kafka_msg_prm_ids = set()
        self.prm_link_tprms = dict()
        self.mo_link_tprms = dict()
        self.other_tprms = dict()
        self.mo_link_prms = dict()
        self.prm_link_prms = dict()
        self.other_prms = dict()
        self.all_actions = list()
        self.changed_mo_link_prms = dict()
        self.changed_prm_link_prms = dict()
        self.changed_other_prms = dict()
        self.existing_mos = dict()
        self.mo_id_updated_mo_data = dict()

    async def __stage_1_get_tprm_ids_and_mo_ids_from_kafka_msg(self):
        for prm_data in self.kafka_msg["objects"]:
            self.kafka_msg_tprm_ids.add(prm_data["tprm_id"])
            self.kafka_msg_mo_ids.add(prm_data["mo_id"])
            self.kafka_msg_prm_ids.add(prm_data["id"])

    async def __stage_2_get_tprms_and_group_by_mo_link_and_prm_link_val_types(
        self,
    ):
        search_query = {"terms": {"id": list(self.kafka_msg_tprm_ids)}}
        tprms_search_res = await self.async_client.search(
            index=INVENTORY_TPRM_INDEX_V2,
            query=search_query,
            size=len(self.kafka_msg_tprm_ids),
        )

        mo_link_prm_link_caches = {
            InventoryFieldValType.MO_LINK.value: self.mo_link_tprms,
            InventoryFieldValType.PRM_LINK.value: self.prm_link_tprms,
        }

        for item in tprms_search_res["hits"]["hits"]:
            item_source = item["_source"]
            item_id = item_source["id"]
            mo_link_prm_link_val_type = mo_link_prm_link_caches.get(
                item_source["val_type"]
            )
            if mo_link_prm_link_val_type is not None:
                mo_link_prm_link_val_type[item_id] = item_source
            else:
                self.other_tprms[item_id] = item_source

    async def __stage_3_group_prms_from_msg_by_mo_link_and_prm_link(self):
        for prm_data in self.kafka_msg["objects"]:
            prm_id = prm_data["id"]
            prm_tprm_id = prm_data["tprm_id"]
            if prm_tprm_id in self.other_tprms:
                self.other_prms[prm_id] = prm_data
            elif prm_tprm_id in self.prm_link_tprms:
                self.prm_link_prms[prm_id] = prm_data
            elif prm_tprm_id in self.mo_link_tprms:
                self.mo_link_prms[prm_id] = prm_data

    async def __stage_4_mo_link_type_handler(self):
        if self.mo_link_prms:
            mo_link_tprm_convert_functions = {}

            for tprm_data in self.mo_link_tprms.values():
                val_type = InventoryFieldValType.INT.value
                if tprm_data["multiple"]:
                    mo_link_tprm_convert_functions[tprm_data["id"]] = (
                        get_convert_func_for_inventory_kafka_msg_for_multiple_pickled_values(
                            val_type
                        )
                    )
                else:
                    mo_link_tprm_convert_functions[tprm_data["id"]] = (
                        get_convert_func_for_inventory_kafka_msg_for_not_multiple_tprm(
                            val_type
                        )
                    )

            ids_of_corresponding_mos = set()
            converted_mo_link_data = {}
            for prm_data in self.mo_link_prms.values():
                data = dict()
                data.update(prm_data)

                prm_id = prm_data["id"]

                conv_f = mo_link_tprm_convert_functions.get(prm_data["tprm_id"])
                new_value = conv_f(prm_data["value"])
                data["value"] = new_value
                converted_mo_link_data[prm_id] = data

                data_to_mo_link_index = dict()
                data_to_mo_link_index.update(data)
                if isinstance(new_value, int):
                    ids_of_corresponding_mos.add(new_value)
                else:
                    ids_of_corresponding_mos.update(new_value)

                prm_action = dict(
                    _index=INVENTORY_PRM_INDEX,
                    _op_type="index",
                    _id=prm_id,
                    _source=prm_data,
                )

                prm_mo_link_action = dict(
                    _index=INVENTORY_MO_LINK_INDEX,
                    _op_type="index",
                    _id=prm_id,
                    _source=data_to_mo_link_index,
                )
                self.all_actions.append(prm_action)
                self.all_actions.append(prm_mo_link_action)

            names_of_corresp_mos = dict()
            if ids_of_corresponding_mos:
                # get corresponding mos names
                search_query = {"terms": {"id": list(ids_of_corresponding_mos)}}
                search_res = await self.async_client.search(
                    index=ALL_MO_OBJ_INDEXES_PATTERN,
                    query=search_query,
                    size=len(ids_of_corresponding_mos),
                    ignore_unavailable=True,
                )
                names_of_corresp_mos = {
                    item["_source"]["id"]: item["_source"]["name"]
                    for item in search_res["hits"]["hits"]
                }

            # change values of mo_link with corresponding mo names
            for converted_mo_link_data in converted_mo_link_data.values():
                mo_link_value = converted_mo_link_data["value"]
                if isinstance(mo_link_value, int):
                    new_value = names_of_corresp_mos.get(mo_link_value)
                else:
                    new_value = [
                        corr_name
                        for item_id in mo_link_value
                        if (corr_name := names_of_corresp_mos.get(item_id))
                    ]
                if new_value:
                    converted_mo_link_data["value"] = new_value
                    self.changed_mo_link_prms[converted_mo_link_data["id"]] = (
                        converted_mo_link_data
                    )

    async def __stage_4_prm_link_type_handler(self):
        if self.prm_link_prms:
            external_tprm_id_corresponding_tprms = {}
            # get corresponding tprms

            id_of_corresponding_tprms = {
                item["constraint"] for item in self.prm_link_tprms.values()
            }
            search_query = {"terms": {"id": list(id_of_corresponding_tprms)}}
            search_result = await self.async_client.search(
                index=INVENTORY_TPRM_INDEX_V2,
                query=search_query,
                size=len(id_of_corresponding_tprms),
            )
            corresponding_tprms = {
                item["_source"]["id"]: item["_source"]
                for item in search_result["hits"]["hits"]
            }

            for tprm_data in self.prm_link_tprms.values():
                corr_tprm = corresponding_tprms.get(
                    int(tprm_data["constraint"])
                )
                if corr_tprm:
                    external_tprm_id_corresponding_tprms[tprm_data["id"]] = (
                        corr_tprm
                    )

            prm_link_tprm_convert_functions = {}

            for tprm_data in self.prm_link_tprms.values():
                val_type = InventoryFieldValType.INT.value
                if tprm_data["multiple"]:
                    prm_link_tprm_convert_functions[tprm_data["id"]] = (
                        get_convert_func_for_inventory_kafka_msg_for_multiple_pickled_values(
                            val_type
                        )
                    )
                else:
                    prm_link_tprm_convert_functions[tprm_data["id"]] = (
                        get_convert_func_for_inventory_kafka_msg_for_not_multiple_tprm(
                            val_type
                        )
                    )

            ids_of_corresponding_prms = set()
            converted_prm_link_data = {}
            for prm_data in self.prm_link_prms.values():
                data = dict()
                data.update(prm_data)

                prm_id = prm_data["id"]

                conv_f = prm_link_tprm_convert_functions.get(
                    prm_data["tprm_id"]
                )
                new_value = conv_f(prm_data["value"])
                data["value"] = new_value
                converted_prm_link_data[prm_id] = data

                data_to_prm_link_index = dict()
                data_to_prm_link_index.update(data)
                if isinstance(new_value, int):
                    ids_of_corresponding_prms.add(new_value)
                else:
                    ids_of_corresponding_prms.update(new_value)

                prm_action = dict(
                    _index=INVENTORY_PRM_INDEX,
                    _op_type="index",
                    _id=prm_id,
                    _source=prm_data,
                )

                prm_mo_link_action = dict(
                    _index=INVENTORY_PRM_LINK_INDEX,
                    _op_type="index",
                    _id=prm_id,
                    _source=data_to_prm_link_index,
                )
                self.all_actions.append(prm_action)
                self.all_actions.append(prm_mo_link_action)

            corresp_prms = dict()
            if ids_of_corresponding_prms:
                # get corresponding mos names
                search_query = {
                    "terms": {"id": list(ids_of_corresponding_prms)}
                }
                search_res = await self.async_client.search(
                    index=INVENTORY_PRM_INDEX,
                    query=search_query,
                    size=len(ids_of_corresponding_prms),
                    ignore_unavailable=True,
                )
                corresp_prms = {
                    item["_source"]["id"]: item["_source"]
                    for item in search_res["hits"]["hits"]
                }

            # change values of mo_link with corresponding mo names
            for prm_link_data in converted_prm_link_data.values():
                prm_link_value = prm_link_data["value"]
                corresp_tprm = external_tprm_id_corresponding_tprms.get(
                    prm_link_data["tprm_id"]
                )
                if corresp_tprm:
                    corresp_val_type = corresp_tprm["val_type"]
                    corresp_tprm_is_multiple = corresp_tprm["multiple"]

                    if corresp_tprm_is_multiple:
                        conv_func = get_convert_function_by_val_type_for_multiple_pickled_values(
                            corresp_val_type
                        )
                    else:
                        conv_func = get_convert_function_by_val_type(
                            corresp_val_type
                        )

                    if conv_func:
                        new_value = None
                        if isinstance(prm_link_value, int):
                            corr_data = corresp_prms.get(prm_link_value)
                            if corr_data:
                                new_value = conv_func(corr_data["value"])
                        else:
                            new_value = [
                                conv_func(corr_data["value"])
                                for item_id in prm_link_value
                                if (corr_data := corresp_prms.get(item_id))
                            ]
                        if new_value:
                            prm_link_data["value"] = new_value
                            self.changed_prm_link_prms[prm_link_data["id"]] = (
                                prm_link_data
                            )

    async def __stage_4_other_prm_types_handler(self):
        conv_f_by_tprm_id_for_other = dict()

        for tprm_data in self.other_tprms.values():
            val_type = tprm_data["val_type"]
            is_multiple = tprm_data.get("multiple")
            if is_multiple:
                conv_f_by_tprm_id_for_other[tprm_data["id"]] = (
                    get_convert_func_for_inventory_kafka_msg_for_multiple_pickled_values(
                        val_type
                    )
                )
            else:
                conv_f_by_tprm_id_for_other[tprm_data["id"]] = (
                    get_convert_func_for_inventory_kafka_msg_for_not_multiple_tprm(
                        val_type
                    )
                )

        for prm_item in self.other_prms.values():
            tprm_id_of_prm = prm_item["tprm_id"]

            prm_action = dict(
                _index=INVENTORY_PRM_INDEX,
                _op_type="index",
                _id=prm_item["id"],
                _source=prm_item,
            )
            self.all_actions.append(prm_action)

            if tprm_id_of_prm in self.other_tprms:
                prm_copy = dict()
                prm_copy.update(prm_item)
                convert_func = conv_f_by_tprm_id_for_other[tprm_id_of_prm]
                prm_copy["value"] = convert_func(prm_copy["value"])
                self.changed_other_prms[prm_copy["id"]] = prm_copy

    async def __stage_5_create_update_actions_for_existing_mos(self):
        # existing mo
        search_query = {"terms": {"id": list(self.kafka_msg_mo_ids)}}
        all_mo_indexes = ALL_MO_OBJ_INDEXES_PATTERN
        mo_search_res = await self.async_client.search(
            index=all_mo_indexes,
            query=search_query,
            size=len(self.kafka_msg_mo_ids),
        )
        self.existing_mos = {
            item["_source"]["id"]: item["_source"]
            for item in mo_search_res["hits"]["hits"]
        }

        self.mo_id_updated_mo_data = defaultdict(dict)

        all_changes = [
            self.changed_mo_link_prms,
            self.changed_prm_link_prms,
            self.changed_other_prms,
        ]

        for data in all_changes:
            for prm_item in data.values():
                tprm_id_of_prm = prm_item["tprm_id"]
                mo_id_of_prm = prm_item["mo_id"]

                if mo_id_of_prm in self.existing_mos:
                    parameters = self.mo_id_updated_mo_data[mo_id_of_prm]
                    if parameters:
                        parameters[INVENTORY_PARAMETERS_FIELD_NAME][
                            str(tprm_id_of_prm)
                        ] = prm_item["value"]
                    else:
                        parameters[INVENTORY_PARAMETERS_FIELD_NAME] = {
                            str(tprm_id_of_prm): prm_item["value"]
                        }

    async def __stage_6_commit_all_changes(self):
        # create actions
        for mo_id, data_to_update in self.mo_id_updated_mo_data.items():
            mo_data = self.existing_mos.get(mo_id)
            if not mo_data:
                continue

            index_name = get_index_name_by_tmo(mo_data["tmo_id"])

            action_item = dict(
                _index=index_name,
                _op_type="update",
                _id=mo_id,
                doc=data_to_update,
            )
            self.all_actions.append(action_item)
        if self.all_actions:
            try:
                await async_bulk(
                    client=self.async_client,
                    refresh="true",
                    actions=self.all_actions,
                )
            except BulkIndexError as e:
                print(e.errors)
                raise e
            except Exception as ex:
                print(type(ex))
                print(self.all_actions)

    async def __stage_2_1_only_for_update_process_if_prm_in_prm_links_value_change_it(
        self,
    ):
        """If updated prm.id in prm_links values - changes value of mo parameter"""

        if not self.kafka_msg_prm_ids:
            return

        search_body = {
            "query": {"terms": {"value": list(self.kafka_msg_prm_ids)}}
        }

        prm_links_results = await get_all_data_from_special_index(
            index=INVENTORY_PRM_LINK_INDEX,
            body=search_body,
            results_as_dict=False,
            async_client=self.async_client,
        )

        if not prm_links_results:
            return

        all_prm_links_values = set()
        for prm_link in prm_links_results:
            value = prm_link.get("value")
            if isinstance(value, list):
                all_prm_links_values.update(value)
            elif isinstance(value, int):
                all_prm_links_values.add(value)

        tprms_data = self.mo_link_tprms | self.other_tprms | self.prm_link_tprms

        for prm_data in self.kafka_msg["objects"]:
            prm_id = prm_data.get("id")
            if prm_id in all_prm_links_values:
                tprm_data = tprms_data.get(prm_data.get("tprm_id"))
                val_type = tprm_data["val_type"]
                if tprm_data["multiple"]:
                    conv_f = get_convert_func_for_inventory_kafka_msg_for_multiple_pickled_values(
                        val_type
                    )
                else:
                    conv_f = get_convert_func_for_inventory_kafka_msg_for_not_multiple_tprm(
                        val_type
                    )

                prm_copy = copy.deepcopy(prm_data)
                prm_copy["value"] = conv_f(prm_copy["value"])

                await self.__only_for_update_process_update_value_of_one_prm_if_it_in_prm_link_values(
                    prm_data_with_converted_value=prm_copy
                )

    async def __only_for_update_process_update_value_of_one_prm_if_it_in_prm_link_values(
        self, prm_data_with_converted_value: dict
    ):
        """Update the mo value of one prm if its id is contained in the prm_links values."""
        prm_id = prm_data_with_converted_value.get("id")
        prm_value = prm_data_with_converted_value.get("value")
        prm_mo_id = prm_data_with_converted_value.get("mo_id")
        prm_tprm_id = prm_data_with_converted_value.get("tprm_id")

        if all([prm_id, prm_value, prm_mo_id, prm_tprm_id]) is False:
            return

        search_body = {"query": {"terms": {"value": [prm_id]}}}

        prm_link_prms = await get_all_data_from_special_index(
            index=INVENTORY_PRM_LINK_INDEX,
            body=search_body,
            results_as_dict=False,
            async_client=self.async_client,
        )

        if not prm_link_prms:
            return

        data_grouped_by_mo_id = defaultdict(list)

        for prm_link_prm in prm_link_prms:
            prm_link_prm_mo_id = prm_link_prm.get("mo_id")
            prm_link_prm_tprm_id = prm_link_prm.get("tprm_id")
            prm_link_prm_value = prm_link_prm.get("value")

            data_for_instance = {
                "tprm_id": prm_link_prm_tprm_id,
                "multiple": False,
            }

            if isinstance(prm_link_prm_value, list):
                data_for_instance["multiple"] = True

                data_for_instance["index_of_prm_in_array"] = (
                    prm_link_prm_value.index(prm_id)
                )

            data_grouped_by_mo_id[prm_link_prm_mo_id].append(
                PRMLinkChangeData(**data_for_instance)
            )

        search_body = {"query": {"terms": {"id": list(data_grouped_by_mo_id)}}}

        all_mo = await self.async_client.search(
            index=ALL_MO_OBJ_INDEXES_PATTERN,
            body=search_body,
            ignore_unavailable=True,
            size=len(data_grouped_by_mo_id),
        )

        all_mo = all_mo["hits"]["hits"]

        actions = []

        for mo in all_mo:
            mo = mo["_source"]
            mo_id = mo.get("id")
            mo_parameters = mo.get(INVENTORY_PARAMETERS_FIELD_NAME)
            if mo_parameters is None:
                mo_parameters = dict()
            parameters_updated_data = dict()
            data_to_update = data_grouped_by_mo_id.get(mo_id)

            # if not mo_parameters:
            #     mo[INVENTORY_PARAMETERS_FIELD_NAME] = dict()
            #     mo_parameters = mo.get(INVENTORY_PARAMETERS_FIELD_NAME)
            if not data_to_update:
                continue

            for update_tprm_data in data_to_update:
                if update_tprm_data.multiple is False:
                    parameters_updated_data[str(update_tprm_data.tprm_id)] = (
                        prm_value
                    )

                else:
                    parameter_from_mo = mo_parameters.get(
                        str(update_tprm_data.tprm_id)
                    )
                    if not isinstance(parameter_from_mo, list):
                        parameters_updated_data[
                            str(update_tprm_data.tprm_id)
                        ] = prm_value
                    else:
                        if (
                            len(parameter_from_mo)
                            >= update_tprm_data.index_of_prm_in_array
                        ):
                            parameter_from_mo[
                                update_tprm_data.index_of_prm_in_array
                            ] = prm_value
                        else:
                            parameter_from_mo.append(prm_value)
                        parameters_updated_data[
                            str(update_tprm_data.tprm_id)
                        ] = copy.deepcopy(parameter_from_mo)

            index_name = get_index_name_by_tmo(mo["tmo_id"])
            if parameters_updated_data:
                data_to_update = {
                    INVENTORY_PARAMETERS_FIELD_NAME: parameters_updated_data
                }

                action_item = dict(
                    _index=index_name,
                    _op_type="update",
                    _id=mo_id,
                    doc=data_to_update,
                )
                actions.append(action_item)

        if actions:
            try:
                await async_bulk(
                    client=self.async_client, refresh="true", actions=actions
                )
            except BulkIndexError as e:
                print(e.errors)
                raise e

    async def on_prm_create(self):
        await self.__stage_1_get_tprm_ids_and_mo_ids_from_kafka_msg()
        await self.__stage_2_get_tprms_and_group_by_mo_link_and_prm_link_val_types()
        await self.__stage_3_group_prms_from_msg_by_mo_link_and_prm_link()
        await self.__stage_4_mo_link_type_handler()
        await self.__stage_4_prm_link_type_handler()
        await self.__stage_4_other_prm_types_handler()
        await self.__stage_5_create_update_actions_for_existing_mos()
        await self.__stage_6_commit_all_changes()

    async def on_prm_update(self):
        await self.__stage_1_get_tprm_ids_and_mo_ids_from_kafka_msg()
        await self.__stage_2_get_tprms_and_group_by_mo_link_and_prm_link_val_types()
        await self.__stage_2_1_only_for_update_process_if_prm_in_prm_links_value_change_it()
        await self.__stage_3_group_prms_from_msg_by_mo_link_and_prm_link()
        await self.__stage_4_mo_link_type_handler()
        await self.__stage_4_prm_link_type_handler()
        await self.__stage_4_other_prm_types_handler()
        await self.__stage_5_create_update_actions_for_existing_mos()
        await self.__stage_6_commit_all_changes()
