import datetime

import pytest
from elasticsearch import AsyncElasticsearch
from google.protobuf import json_format
from google.protobuf.timestamp_pb2 import Timestamp
from elastic.config import INVENTORY_TMO_INDEX_V2, DEFAULT_SETTING_FOR_TMO_INDEX
from elastic.query_builder_service.inventory_index.utils.index_utils import (
    get_index_name_by_tmo,
)
from indexes_mapping.inventory.mapping import INVENTORY_OBJ_INDEX_MAPPING

from services.inventory_services.kafka.consumers.inventory_changes.utils import (
    InventoryChangesHandler,
)
from tests.kafka.consumers.topics.inventory_changes.utils import (
    create_cleared_kafka_tmo_msg,
)

datetime_value = datetime.datetime.now()
proto_timestamp = Timestamp()
proto_timestamp.FromDatetime(datetime_value)

MSG_EVENT = "updated"
MSG_CLASS_NAME = "TMO"
EXISTING_TMO_DATA_KAFKA = {
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

EXISTING_TMO_UPDATE_DATA_KAFKA = {
    "id": 1,
    "name": "NEW TMO UPDATED",
    "p_id": None,
    "icon": None,
    "description": "NEW SOME DESCRIPTION OF NEW TMO",
    "virtual": False,
    "global_uniqueness": False,
    "lifecycle_process_definition": None,
    "severity_id": None,
    "geometry_type": None,
    "materialize": False,
    "version": 25,
    "latitude": None,
    "longitude": None,
    "status": 2,
    "created_by": "admin NEW",
    "modified_by": "NEW",
    "creation_date": proto_timestamp,
    "modification_date": proto_timestamp,
    "primary": None,
    "points_constraint_by_tmo": None,
}

EXISTING_TMO_UPDATE_DATA_ELASTIC = dict()
EXISTING_TMO_UPDATE_DATA_ELASTIC.update(EXISTING_TMO_UPDATE_DATA_KAFKA)
EXISTING_TMO_UPDATE_DATA_ELASTIC["creation_date"] = json_format.MessageToDict(
    proto_timestamp
).split("Z")[0]
EXISTING_TMO_UPDATE_DATA_ELASTIC["modification_date"] = (
    json_format.MessageToDict(proto_timestamp).split("Z")[0]
)

NOT_EXISTING_TMO_UPDATE_DATA = {
    "id": 25,
    "name": "NEW TMO UPDATED",
    "p_id": None,
    "icon": None,
    "description": "NEW SOME DESCRIPTION OF NEW TMO",
    "virtual": False,
    "global_uniqueness": False,
    "lifecycle_process_definition": None,
    "severity_id": None,
    "geometry_type": None,
    "materialize": False,
    "version": 25,
    "latitude": None,
    "longitude": None,
    "status": 2,
    "created_by": "admin NEW",
    "modified_by": "NEW",
    "creation_date": proto_timestamp,
    "modification_date": proto_timestamp,
    "primary": None,
    "points_constraint_by_tmo": None,
}

# MSG_AS_DICT = {'objects': [EXISTING_TMO_DATA]}
# MSG_AS_DICT_WITH_UPDATE_DATA = {'objects': [EXISTING_TMO_UPDATE_DATA]}
MSG_AS_DICT_WITH_TMO_DOES_NOT_EXIST = {
    "objects": [NOT_EXISTING_TMO_UPDATE_DATA]
}


async def create_default_tmo(async_elastic_session: AsyncElasticsearch):
    """Creates default tmo"""
    existing_tmo_elastic = dict()
    existing_tmo_elastic.update(EXISTING_TMO_DATA_KAFKA)
    existing_tmo_elastic["creation_date"] = datetime_value
    existing_tmo_elastic["modification_date"] = datetime_value

    await async_elastic_session.index(
        index=INVENTORY_TMO_INDEX_V2,
        id=EXISTING_TMO_DATA_KAFKA["id"],
        document=existing_tmo_elastic,
        refresh="true",
    )

    index_name = get_index_name_by_tmo(EXISTING_TMO_DATA_KAFKA["id"])

    await async_elastic_session.indices.create(
        index=index_name,
        mappings=INVENTORY_OBJ_INDEX_MAPPING,
        settings=DEFAULT_SETTING_FOR_TMO_INDEX,
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_tmo_case_1(async_elastic_session):
    """TEST On receiving TMO:updated msg - if tmo does not exist:
    - does not create new record,
    - does not raise error,
    - does not create new index with INVENTORY_OBJ_INDEX_PREFIX as prefix."""

    search_query = {"match": {"id": NOT_EXISTING_TMO_UPDATE_DATA["id"]}}
    result_before = await async_elastic_session.search(
        index=INVENTORY_TMO_INDEX_V2, query=search_query, track_total_hits=True
    )
    # tmo does not exist
    assert result_before["hits"]["total"]["value"] == 0

    kafka_msg = create_cleared_kafka_tmo_msg(
        list_of_tmo_data=[NOT_EXISTING_TMO_UPDATE_DATA], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    try:
        await handler.process_the_message()
    except Exception:
        assert False

    result_after = await async_elastic_session.search(
        index=INVENTORY_TMO_INDEX_V2, query=search_query, track_total_hits=True
    )
    # does not create new tmo record
    assert result_after["hits"]["total"]["value"] == 0

    # does not create new index
    index_name = get_index_name_by_tmo(NOT_EXISTING_TMO_UPDATE_DATA["id"])
    index_exists = await async_elastic_session.indices.exists(index=index_name)
    assert bool(index_exists) is False


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_tmo_case_2(async_elastic_session):
    """TEST On receiving TMO:updated msg - if tmo exists:
    - update existing record
    """

    # create record
    await create_default_tmo(async_elastic_session)

    search_query = {"match": {"id": EXISTING_TMO_DATA_KAFKA["id"]}}
    result_before_update = await async_elastic_session.search(
        index=INVENTORY_TMO_INDEX_V2, query=search_query, track_total_hits=True
    )

    result_before_update = result_before_update["hits"]["hits"][0]["_source"]

    not_match_values = 0
    for k, old_value in result_before_update.items():
        value_will_be_update_to = EXISTING_TMO_UPDATE_DATA_KAFKA.get(k)
        if old_value != value_will_be_update_to:
            not_match_values += 1

    assert not_match_values > 1

    kafka_msg = create_cleared_kafka_tmo_msg(
        list_of_tmo_data=[EXISTING_TMO_UPDATE_DATA_KAFKA], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)

    await handler.process_the_message()

    result_after_update = await async_elastic_session.search(
        index=INVENTORY_TMO_INDEX_V2, query=search_query, track_total_hits=True
    )

    result_after_update = result_after_update["hits"]["hits"][0]["_source"]

    # update is successful
    not_match_values = 0
    for k, new_value in result_after_update.items():
        old_value = result_before_update.get(k)
        if old_value != new_value:
            not_match_values += 1

    assert not_match_values > 1


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_tmo_case_3(async_elastic_session):
    """TEST On receiving TMO:updated msg - if tmo exists:
    - updated record available in the search results by all attrs."""

    # create record
    await create_default_tmo(async_elastic_session)

    # update record
    kafka_msg = create_cleared_kafka_tmo_msg(
        list_of_tmo_data=[EXISTING_TMO_UPDATE_DATA_KAFKA], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)

    await handler.process_the_message()

    attrs_not_none = {
        attr: value
        for attr, value in EXISTING_TMO_UPDATE_DATA_ELASTIC.items()
        if value
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
            == EXISTING_TMO_UPDATE_DATA_KAFKA["id"]
        )
