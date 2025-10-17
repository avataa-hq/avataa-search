import pytest
from elasticsearch import AsyncElasticsearch

from elastic.config import ALL_MO_OBJ_INDEXES_PATTERN

from indexes_mapping.inventory.zeebe_enums import ZeebeProcessInstanceFields
from kafka_config.config import KAFKA_ZEEBE_CHANGES_TOPIC
from services.inventory_services.kafka.consumers.inventory_changes.events.mo_msg import (
    on_create_mo,
)
from services.inventory_services.kafka.consumers.inventory_changes.events.tmo_msg import (
    on_create_tmo,
)
from services.zeebe_services.kafka.consumers.process_changes.protobuf import (
    kafka_process_pb2,
)
from services.zeebe_services.kafka.consumers.process_changes.utils import (
    ProcessChangesHandler,
)
from tests.kafka.utils import KafkaMSGMock

DEFAULT_TMO_DATA = {
    "id": 1,
    "name": "DEFAULT TMO",
    "p_id": None,
    "icon": None,
    "description": "SOME DESCRIPTION OF DEFAULT TMO",
    "virtual": False,
    "global_uniqueness": False,
    "lifecycle_process_definition": None,
    "severity_id": None,
    "geometry_type": None,
    "materialize": False,
    "version": 1,
    "latitude": None,
    "longitude": None,
    "status": 1,
    "created_by": "admin",
    "modified_by": "None",
    "creation_date": "2000-12-12",
    "modification_date": "2000-12-12",
    "primary": None,
    "points_constraint_by_tmo": None,
}

DEFAULT_MO_DATA = {
    "id": 3,
    "name": "DEFAULT MO DATA",
    "active": True,
    "tmo_id": DEFAULT_TMO_DATA["id"],
    "latitude": 0.0,
    "longitude": 0.0,
    "p_id": 0,
    "point_a_id": 0,
    "point_b_id": 0,
    "model": "",
    "version": 0,
    "status": "",
}

DEFAULT_PROCESS_DATA = {
    "mo_id": DEFAULT_MO_DATA["id"],
    "process_instance_key": 123456789,
}

MSG_CLASS_NAME = "Process"
MSG_EVENT = "created"


def create_cleared_kafka_msg(
    mo_id: int, process_instance_key: int, msg_class_name: str, msg_event: str
):
    msg_value = kafka_process_pb2.Process(
        mo_id=mo_id, process_instance_key=process_instance_key
    )
    msg_value = msg_value.SerializeToString()

    mocked_msg = KafkaMSGMock(
        msg_key=f"{msg_class_name}:{msg_event}",
        msg_topic=f"{KAFKA_ZEEBE_CHANGES_TOPIC}",
        msg_value=msg_value,
    )

    return mocked_msg


async def create_all_default_data(
    async_elastic_session: AsyncElasticsearch,
    tmo_data: dict = None,
    mo_data: dict = None,
):
    """Creates TMO, TPRM, MO records"""
    if tmo_data:
        await on_create_tmo(
            msg={"objects": [tmo_data]}, async_client=async_elastic_session
        )

    if mo_data:
        await on_create_mo(
            msg={"objects": [mo_data]}, async_client=async_elastic_session
        )


@pytest.mark.skip(reason="Not corrected implementing")
async def test_process_handler_with_create_event_case_1(async_elastic_session):
    """TEST With receiving Process:created msg - if MO does not exist:
    - does not raise error,
    - does update MO"""

    search_query = {"match": {"id": DEFAULT_MO_DATA["id"]}}
    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )
    assert search_res["hits"]["total"]["value"] == 0

    kafka_msg = create_cleared_kafka_msg(
        mo_id=DEFAULT_MO_DATA["id"],
        process_instance_key=12123423543,
        msg_class_name="Process",
        msg_event="created",
    )

    handler = ProcessChangesHandler(
        kafka_msg=kafka_msg, elastic_client=async_elastic_session
    )
    await handler.process_the_message()

    # record with matched data does not exist

    search_query = {
        "exists": {
            "field": ZeebeProcessInstanceFields.PROCESS_INSTANCE_ID.value
        }
    }
    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )
    assert search_res["hits"]["total"]["value"] == 0


@pytest.mark.skip(reason="Not corrected implementing")
async def test_with_create_process_case_2(async_elastic_session):
    """TEST With receiving Process:created msg - if MO exists:
    - updates MO,
    - the new parameter value is available for search"""

    await create_all_default_data(
        async_elastic_session=async_elastic_session,
        tmo_data=DEFAULT_TMO_DATA,
        mo_data=DEFAULT_MO_DATA,
    )

    search_query = {"match": {"id": DEFAULT_MO_DATA["id"]}}
    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )
    assert search_res["hits"]["total"]["value"] == 1

    kafka_msg = create_cleared_kafka_msg(
        mo_id=DEFAULT_PROCESS_DATA["mo_id"],
        process_instance_key=DEFAULT_PROCESS_DATA["process_instance_key"],
        msg_class_name="Process",
        msg_event="created",
    )

    handler = ProcessChangesHandler(
        kafka_msg=kafka_msg, elastic_client=async_elastic_session
    )
    await handler.process_the_message()

    # record with matched data does not exist

    search_query = {
        "exists": {
            "field": ZeebeProcessInstanceFields.PROCESS_INSTANCE_ID.value
        }
    }
    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )
    assert search_res["hits"]["total"]["value"] == 1

    # the new parameter value is available for search
    new_value = DEFAULT_PROCESS_DATA["process_instance_key"]
    search_query = {
        "bool": {
            "must": [
                {
                    "exists": {
                        "field": ZeebeProcessInstanceFields.PROCESS_INSTANCE_ID.value
                    }
                },
                {
                    "match": {
                        ZeebeProcessInstanceFields.PROCESS_INSTANCE_ID.value: new_value
                    }
                },
            ]
        }
    }
    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )
    assert search_res["hits"]["total"]["value"] == 1

    item_data = search_res["hits"]["hits"][0]["_source"]
    assert item_data["id"] == DEFAULT_MO_DATA["id"]
