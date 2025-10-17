from typing import List

from elasticsearch import AsyncElasticsearch

from elastic.config import INVENTORY_TMO_INDEX_V2, DEFAULT_SETTING_FOR_TMO_INDEX
from elastic.query_builder_service.inventory_index.utils.index_utils import (
    get_index_name_by_tmo,
)
from indexes_mapping.inventory.mapping import INVENTORY_OBJ_INDEX_MAPPING
from kafka_config.config import KAFKA_INVENTORY_CHANGES_TOPIC
from services.inventory_services.protobuf_files.obj_proto import (
    inventory_instances_pb2,
)
from services.inventory_services.kafka.consumers.inventory_changes.utils import (
    InventoryChangesHandler,
)
from tests.kafka.utils import KafkaMSGMock


async def create_default_tmo_in_elastic(
    async_elastic_session: AsyncElasticsearch, default_tmo_data: dict
):
    """Creates default tmo"""
    existing_tmo_elastic = dict()
    existing_tmo_elastic.update(default_tmo_data)

    await async_elastic_session.index(
        index=INVENTORY_TMO_INDEX_V2,
        id=default_tmo_data["id"],
        document=existing_tmo_elastic,
        refresh="true",
    )

    index_name = get_index_name_by_tmo(default_tmo_data["id"])

    await async_elastic_session.indices.create(
        index=index_name,
        mappings=INVENTORY_OBJ_INDEX_MAPPING,
        settings=DEFAULT_SETTING_FOR_TMO_INDEX,
    )


def create_cleared_kafka_tmo_msg(list_of_tmo_data: List[dict], msg_event: str):
    list_of_tmo_proto_models = list()
    for tmo_data in list_of_tmo_data:
        list_of_tmo_proto_models.append(inventory_instances_pb2.TMO(**tmo_data))

    msg_value = inventory_instances_pb2.ListTMO(
        objects=list_of_tmo_proto_models
    )
    msg_value = msg_value.SerializeToString()

    mocked_msg = KafkaMSGMock(
        msg_key=f"TMO:{msg_event}",
        msg_topic=f"{KAFKA_INVENTORY_CHANGES_TOPIC}",
        msg_value=msg_value,
    )

    return mocked_msg


async def create_default_mo_in_elastic(
    async_elastic_session: AsyncElasticsearch, default_mo_data: dict
):
    """Creates default mo"""

    index_name = get_index_name_by_tmo(default_mo_data["tmo_id"])

    await async_elastic_session.index(
        index=index_name,
        document=default_mo_data,
        refresh="true",
        id=default_mo_data["id"],
    )


def create_cleared_kafka_mo_msg(list_of_mo_data: List[dict], msg_event: str):
    list_of_mo_proto_models = list()
    for mo_data in list_of_mo_data:
        list_of_mo_proto_models.append(inventory_instances_pb2.MO(**mo_data))

    msg_value = inventory_instances_pb2.ListMO(objects=list_of_mo_proto_models)
    msg_value = msg_value.SerializeToString()

    mocked_msg = KafkaMSGMock(
        msg_key=f"MO:{msg_event}",
        msg_topic=f"{KAFKA_INVENTORY_CHANGES_TOPIC}",
        msg_value=msg_value,
    )

    return mocked_msg


async def create_default_tprm_in_elastic(default_tprm_data_kafka_format: dict):
    "Creates record and updates mapping"
    kafka_msg = create_cleared_kafka_tprm_msg(
        list_of_tprm_data=[default_tprm_data_kafka_format], msg_event="created"
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()


def create_cleared_kafka_tprm_msg(
    list_of_tprm_data: List[dict], msg_event: str
):
    list_of_tprm_proto_models = list()
    for tprm_data in list_of_tprm_data:
        list_of_tprm_proto_models.append(
            inventory_instances_pb2.TPRM(**tprm_data)
        )

    msg_value = inventory_instances_pb2.ListTPRM(
        objects=list_of_tprm_proto_models
    )
    msg_value = msg_value.SerializeToString()

    mocked_msg = KafkaMSGMock(
        msg_key=f"TPRM:{msg_event}",
        msg_topic=f"{KAFKA_INVENTORY_CHANGES_TOPIC}",
        msg_value=msg_value,
    )

    return mocked_msg


def create_cleared_kafka_prm_msg(list_of_prm_data: List[dict], msg_event: str):
    list_of_prm_proto_models = list()
    for prm_data in list_of_prm_data:
        list_of_prm_proto_models.append(inventory_instances_pb2.PRM(**prm_data))

    msg_value = inventory_instances_pb2.ListPRM(
        objects=list_of_prm_proto_models
    )
    msg_value = msg_value.SerializeToString()

    mocked_msg = KafkaMSGMock(
        msg_key=f"PRM:{msg_event}",
        msg_topic=f"{KAFKA_INVENTORY_CHANGES_TOPIC}",
        msg_value=msg_value,
    )

    return mocked_msg


async def create_all_default_data_inventory_changes_topic(
    async_elastic_session: AsyncElasticsearch,
    list_of_tmo_data: List[dict] = None,
    list_of_tprm_data_kafka_format: List[dict] = None,
    list_of_mo_data_kafka_format: List[dict] = None,
    list_of_prm_data_kafka_format: List[dict] = None,
):
    """Creates Defaults data for PRM tests"""
    if list_of_tmo_data:
        for tmo_data in list_of_tmo_data:
            await create_default_tmo_in_elastic(
                async_elastic_session, default_tmo_data=tmo_data
            )

    if list_of_tprm_data_kafka_format:
        kafka_msg = create_cleared_kafka_tprm_msg(
            list_of_tprm_data=list_of_tprm_data_kafka_format,
            msg_event="created",
        )
        handler = InventoryChangesHandler(kafka_msg=kafka_msg)
        await handler.process_the_message()

    if list_of_mo_data_kafka_format:
        kafka_msg = create_cleared_kafka_mo_msg(
            list_of_mo_data=list_of_mo_data_kafka_format, msg_event="created"
        )
        handler = InventoryChangesHandler(kafka_msg=kafka_msg)
        await handler.process_the_message()

    if list_of_prm_data_kafka_format:
        kafka_msg = create_cleared_kafka_prm_msg(
            list_of_prm_data=list_of_prm_data_kafka_format, msg_event="created"
        )
        handler = InventoryChangesHandler(kafka_msg=kafka_msg)
        await handler.process_the_message()
