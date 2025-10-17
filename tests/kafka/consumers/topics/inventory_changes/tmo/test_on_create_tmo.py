import datetime

import pytest
from google.protobuf import json_format
from google.protobuf.timestamp_pb2 import Timestamp

from elastic.config import INVENTORY_TMO_INDEX_V2
from elastic.query_builder_service.inventory_index.utils.index_utils import (
    get_index_name_by_tmo,
)
from services.inventory_services.kafka.consumers.inventory_changes.utils import (
    InventoryChangesHandler,
)
from tests.kafka.consumers.topics.inventory_changes.utils import (
    create_cleared_kafka_tmo_msg,
)

datetime_value = datetime.datetime.now()
proto_timestamp = Timestamp()
proto_timestamp.FromDatetime(datetime_value)

MSG_EVENT = "created"
MSG_CLASS_NAME = "TMO"
NEW_TMO_DATA_KAFKA = {
    "id": 1,
    "name": "NEW TMO",
    "p_id": None,
    "icon": None,
    "description": "SOME DESCRIPTION OF NEW TMO",
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
    "creation_date": proto_timestamp,
    "modification_date": proto_timestamp,
    "primary": None,
    "points_constraint_by_tmo": None,
}

NEW_TMO_DATA_ELASTIC = dict()
NEW_TMO_DATA_ELASTIC.update(NEW_TMO_DATA_KAFKA)
NEW_TMO_DATA_ELASTIC["creation_date"] = json_format.MessageToDict(
    proto_timestamp
).split("Z")[0]
NEW_TMO_DATA_ELASTIC["modification_date"] = json_format.MessageToDict(
    proto_timestamp
).split("Z")[0]

MSG_AS_DICT = {"objects": [NEW_TMO_DATA_KAFKA]}


@pytest.mark.asyncio(loop_scope="session")
async def test_on_create_tmo_case_1(async_elastic_session):
    """TEST On receiving TMO:created msg - if tmo does not exist - create new record in
    the INVENTORY_TMO_INDEX_V2 index."""

    search_query = {"match": {"id": NEW_TMO_DATA_KAFKA["id"]}}
    result_before = await async_elastic_session.search(
        index=INVENTORY_TMO_INDEX_V2, query=search_query, track_total_hits=True
    )
    assert result_before["hits"]["total"]["value"] == 0

    kafka_msg = create_cleared_kafka_tmo_msg(
        list_of_tmo_data=[NEW_TMO_DATA_KAFKA], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    result_after = await async_elastic_session.search(
        index=INVENTORY_TMO_INDEX_V2, query=search_query, track_total_hits=True
    )
    assert result_after["hits"]["total"]["value"] != 0
    assert (
        result_after["hits"]["hits"][0]["_source"]["id"]
        == NEW_TMO_DATA_KAFKA["id"]
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_on_create_tmo_case_2(async_elastic_session):
    """TEST On receiving TMO:created msg - if tmo does not exist - create new index
    with the INVENTORY_OBJ_INDEX_PREFIX as prefix ."""

    index_name = get_index_name_by_tmo(NEW_TMO_DATA_KAFKA["id"])
    index_exists = await async_elastic_session.indices.exists(index=index_name)
    assert bool(index_exists) is False

    kafka_msg = create_cleared_kafka_tmo_msg(
        list_of_tmo_data=[NEW_TMO_DATA_KAFKA], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()
    # await adapter_function(msg_class_name=MSG_CLASS_NAME, msg_event=MSG_EVENT, message_as_dict=MSG_AS_DICT)

    index_exists = await async_elastic_session.indices.exists(index=index_name)

    assert bool(index_exists)


@pytest.mark.asyncio(loop_scope="session")
async def test_on_create_tmo_case_3(async_elastic_session):
    """TEST On receiving TMO:created msg - if tmo exists - does not raise error and does not create new record."""

    search_query = {"match": {"id": NEW_TMO_DATA_KAFKA["id"]}}
    result_before = await async_elastic_session.search(
        index=INVENTORY_TMO_INDEX_V2, query=search_query, track_total_hits=True
    )
    assert result_before["hits"]["total"]["value"] == 0

    kafka_msg = create_cleared_kafka_tmo_msg(
        list_of_tmo_data=[NEW_TMO_DATA_KAFKA], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    result_after_step1 = await async_elastic_session.search(
        index=INVENTORY_TMO_INDEX_V2, query=search_query, track_total_hits=True
    )
    assert result_after_step1["hits"]["total"]["value"] != 0
    assert (
        result_after_step1["hits"]["hits"][0]["_source"]["id"]
        == NEW_TMO_DATA_KAFKA["id"]
    )

    kafka_msg = create_cleared_kafka_tmo_msg(
        list_of_tmo_data=[NEW_TMO_DATA_KAFKA], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    try:
        await handler.process_the_message()
    except Exception:
        assert False

    result_after_step2 = await async_elastic_session.search(
        index=INVENTORY_TMO_INDEX_V2, query=search_query, track_total_hits=True
    )
    assert result_after_step2["hits"]["total"]["value"] == 1
    assert (
        result_after_step2["hits"]["hits"][0]["_source"]["id"]
        == NEW_TMO_DATA_KAFKA["id"]
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_on_create_tmo_case_4(async_elastic_session):
    """TEST On receiving TMO:created msg - if tmo not exists - creates new record.
    New record available in the search results by all attrs."""

    search_query = {"match": {"id": NEW_TMO_DATA_KAFKA["id"]}}
    result_before = await async_elastic_session.search(
        index=INVENTORY_TMO_INDEX_V2, query=search_query, track_total_hits=True
    )
    assert result_before["hits"]["total"]["value"] == 0

    kafka_msg = create_cleared_kafka_tmo_msg(
        list_of_tmo_data=[NEW_TMO_DATA_KAFKA], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    result_after_step1 = await async_elastic_session.search(
        index=INVENTORY_TMO_INDEX_V2, query=search_query, track_total_hits=True
    )
    assert result_after_step1["hits"]["total"]["value"] != 0
    assert (
        result_after_step1["hits"]["hits"][0]["_source"]["id"]
        == NEW_TMO_DATA_KAFKA["id"]
    )

    attrs_not_none = {
        attr: value for attr, value in NEW_TMO_DATA_ELASTIC.items() if value
    }
    assert len(attrs_not_none) > 1

    for attr, value in attrs_not_none.items():
        search_query = {"match": {attr: value}}
        search_res = await async_elastic_session.search(
            index=INVENTORY_TMO_INDEX_V2,
            query=search_query,
            track_total_hits=True,
        )
        assert search_res["hits"]["total"]["value"] == 1
        assert (
            search_res["hits"]["hits"][0]["_source"]["id"]
            == NEW_TMO_DATA_KAFKA["id"]
        )
