import pytest
from elasticsearch import AsyncElasticsearch

from elastic.config import (
    ALL_MO_OBJ_INDEXES_PATTERN,
    DEFAULT_SETTING_FOR_MO_INDEXES,
)
from elastic.query_builder_service.inventory_index.utils.index_utils import (
    get_index_name_by_tmo,
)
from indexes_mapping.inventory.mapping import INVENTORY_OBJ_INDEX_MAPPING

from services.group_builder.kafka.consumers.group.utils import GroupTopicHandler
from tests.kafka.consumers.topics.group_builder_group.process_handler.utils import (
    create_cleared_kafka_msg_for_group_topic,
)

DEFAULT_MO_DATA = {
    "id": 111,
    "name": "566_LF-314_U_169500647943",
    "active": True,
    "tmo_id": 11111,
    "latitude": 0.0,
    "longitude": 0.0,
    "p_id": 0,
    "point_a_id": 0,
    "point_b_id": 0,
    "model": "",
    "version": 0,
    "status": "",
}

TEST_GROUP_NAME = "TEst Group Name"


async def create_default_data(
    async_elastic_session: AsyncElasticsearch, mo_data: dict
):
    new_index_name = get_index_name_by_tmo(tmo_id=mo_data["tmo_id"])

    # create index
    await async_elastic_session.indices.create(
        index=new_index_name,
        mappings=INVENTORY_OBJ_INDEX_MAPPING,
        settings=DEFAULT_SETTING_FOR_MO_INDEXES,
    )

    # create default mo parent record
    await async_elastic_session.index(
        index=new_index_name, id=mo_data["id"], document=mo_data, refresh="true"
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_group_handler_with_add_event_case_1(async_elastic_session):
    """TEST With receiving group:add msg - if MO does not exist:
    - does not raise error,
    - does update MO"""
    search_query = {"match": {"id": DEFAULT_MO_DATA["id"]}}
    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )
    assert search_res["hits"]["total"]["value"] == 0

    kafka_msg = create_cleared_kafka_msg_for_group_topic(
        mo_ids=[DEFAULT_MO_DATA["id"]],
        tmo_id=DEFAULT_MO_DATA["tmo_id"],
        group_name=TEST_GROUP_NAME,
        msg_class_name="group",
        msg_event="add",
    )
    try:
        handler = GroupTopicHandler(kafka_msg=kafka_msg)
        await handler.process_the_message()
    except Exception:
        assert False

    # record with matched data does not exist
    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )
    assert search_res["hits"]["total"]["value"] == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_group_handler_with_add_event_case_2(async_elastic_session):
    """TEST With receiving group:add msg - if MO exists and MO.groups = None:
    - updates MO.groups = new_group_name,
    - New value is in index (available for search)"""

    await create_default_data(
        mo_data=DEFAULT_MO_DATA, async_elastic_session=async_elastic_session
    )

    search_query = {"match": {"id": DEFAULT_MO_DATA["id"]}}
    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )
    assert search_res["hits"]["total"]["value"] == 1
    assert search_res["hits"]["hits"][0]["_source"].get("groups") is None

    kafka_msg = create_cleared_kafka_msg_for_group_topic(
        mo_ids=[DEFAULT_MO_DATA["id"]],
        tmo_id=DEFAULT_MO_DATA["tmo_id"],
        group_name=TEST_GROUP_NAME,
        msg_class_name="group",
        msg_event="add",
    )

    handler = GroupTopicHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # new mo.groups value available in search
    search_query = {"terms": {"groups": [TEST_GROUP_NAME]}}
    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )
    assert search_res["hits"]["total"]["value"] == 1
    assert search_res["hits"]["hits"][0]["_source"]["groups"] == [
        TEST_GROUP_NAME
    ]


@pytest.mark.asyncio(loop_scope="session")
async def test_group_handler_with_add_event_case_3(async_elastic_session):
    """TEST With receiving group:add msg - if MO exists and MO.groups = SomeGroup:
    - append new group into MO.groups,
    - New value is in index (available for search)"""
    existing_group_name = "SomeGroup"
    new_mo_data = dict()
    new_mo_data.update(DEFAULT_MO_DATA)
    new_mo_data["groups"] = "SomeGroup"

    await create_default_data(
        mo_data=new_mo_data, async_elastic_session=async_elastic_session
    )

    search_query = {"match": {"id": DEFAULT_MO_DATA["id"]}}
    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )
    assert search_res["hits"]["total"]["value"] == 1
    assert (
        search_res["hits"]["hits"][0]["_source"].get("groups")
        == existing_group_name
    )

    kafka_msg = create_cleared_kafka_msg_for_group_topic(
        mo_ids=[DEFAULT_MO_DATA["id"]],
        tmo_id=DEFAULT_MO_DATA["tmo_id"],
        group_name=TEST_GROUP_NAME,
        msg_class_name="group",
        msg_event="add",
    )

    handler = GroupTopicHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # new mo.groups value available in search
    search_query = {"terms": {"groups": [TEST_GROUP_NAME]}}
    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )
    assert search_res["hits"]["total"]["value"] == 1
    assert search_res["hits"]["hits"][0]["_source"]["groups"] == [
        existing_group_name,
        TEST_GROUP_NAME,
    ]
