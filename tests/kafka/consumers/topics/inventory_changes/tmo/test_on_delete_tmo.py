import datetime

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from elasticsearch import AsyncElasticsearch

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

MSG_EVENT = "deleted"
MSG_CLASS_NAME = "TMO"
EXISTING_TMO_DATA = {
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

NOT_EXISTING_TMO_DATA = {
    "id": 10,
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


async def create_default_tmo(async_elastic_session: AsyncElasticsearch):
    """Creates default tmo"""
    existing_tmo_elastic = dict()
    existing_tmo_elastic.update(EXISTING_TMO_DATA)
    existing_tmo_elastic["creation_date"] = datetime_value
    existing_tmo_elastic["modification_date"] = datetime_value

    await async_elastic_session.index(
        index=INVENTORY_TMO_INDEX_V2,
        id=EXISTING_TMO_DATA["id"],
        document=existing_tmo_elastic,
        refresh="true",
    )

    index_name = get_index_name_by_tmo(EXISTING_TMO_DATA["id"])

    await async_elastic_session.indices.create(
        index=index_name,
        mappings=INVENTORY_OBJ_INDEX_MAPPING,
        settings=DEFAULT_SETTING_FOR_TMO_INDEX,
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_on_delete_tmo_case_1(async_elastic_session):
    """TEST On receiving TMO:deleted msg - if tmo does not exist - does not raise error"""

    search_query = {"match": {"id": NOT_EXISTING_TMO_DATA["id"]}}
    result_before = await async_elastic_session.search(
        index=INVENTORY_TMO_INDEX_V2, query=search_query, track_total_hits=True
    )
    assert result_before["hits"]["total"]["value"] == 0

    kafka_msg = create_cleared_kafka_tmo_msg(
        list_of_tmo_data=[NOT_EXISTING_TMO_DATA], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    try:
        await handler.process_the_message()
    except Exception:
        assert False


@pytest.mark.asyncio(loop_scope="session")
async def test_on_delete_tmo_case_2(async_elastic_session):
    """TEST On receiving TMO:deleted msg - if tmo exists:
    - deletes record from INVENTORY_TMO_INDEX_V2
    - deletes index for mo data of this tmo (with INVENTORY_OBJ_INDEX_PREFIX as prefix).
    - refreshes index !important"""

    await create_default_tmo(async_elastic_session)

    # before
    index_name = get_index_name_by_tmo(EXISTING_TMO_DATA["id"])
    index_exists = await async_elastic_session.indices.exists(index=index_name)
    assert bool(index_exists)

    # record was deleted
    search_query = {"match": {"id": EXISTING_TMO_DATA["id"]}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TMO_INDEX_V2, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1

    kafka_msg = create_cleared_kafka_tmo_msg(
        list_of_tmo_data=[EXISTING_TMO_DATA], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)

    await handler.process_the_message()

    # index was deleted
    index_exists = await async_elastic_session.indices.exists(index=index_name)
    assert bool(index_exists) is False

    # record was deleted
    search_res = await async_elastic_session.search(
        index=INVENTORY_TMO_INDEX_V2, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 0

    # check_than index refreshed
    delete_query = {"match_all": {}}
    await async_elastic_session.delete_by_query(
        index=INVENTORY_TMO_INDEX_V2, query=delete_query
    )
