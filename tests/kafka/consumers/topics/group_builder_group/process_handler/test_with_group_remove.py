from typing import List

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

DEFAULT_MO_DATA_WITH_ANOTHER_MO = {
    "id": 1110,
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

MO_DATA_WITH_ONE_GROUP = dict()
MO_DATA_WITH_ONE_GROUP.update(DEFAULT_MO_DATA)
MO_DATA_WITH_ONE_GROUP["groups"] = TEST_GROUP_NAME

MO_DATA_WITH_TMO = dict()
MO_DATA_WITH_TMO.update(DEFAULT_MO_DATA_WITH_ANOTHER_MO)
MO_DATA_WITH_TMO["groups"] = TEST_GROUP_NAME

MO_DATA_WITH_TWO_GROUPS = dict()
MO_DATA_WITH_TWO_GROUPS.update(MO_DATA_WITH_ONE_GROUP)
MO_DATA_WITH_TWO_GROUPS["groups"] = [TEST_GROUP_NAME, "Some Group 2"]


async def create_default_data(
    async_elastic_session: AsyncElasticsearch,
    list_of_mo_data: List[dict],
    tmo_id: int,
):
    new_index_name = get_index_name_by_tmo(tmo_id=tmo_id)

    # create index
    await async_elastic_session.indices.create(
        index=new_index_name,
        mappings=INVENTORY_OBJ_INDEX_MAPPING,
        settings=DEFAULT_SETTING_FOR_MO_INDEXES,
    )

    # create default mo parent record
    for mo_data in list_of_mo_data:
        await async_elastic_session.index(
            index=new_index_name,
            id=mo_data["id"],
            document=mo_data,
            refresh="true",
        )


@pytest.mark.asyncio(loop_scope="session")
async def test_group_handler_with_remove_event_case_1(async_elastic_session):
    """TEST With receiving group:remove msg - if MO does not exist:
    - does not raise error,
    - does update MO"""

    await create_default_data(
        list_of_mo_data=[MO_DATA_WITH_TMO],
        async_elastic_session=async_elastic_session,
        tmo_id=MO_DATA_WITH_TMO["tmo_id"],
    )

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
        msg_event="remove",
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
async def test_group_handler_with_remove_event_case_2(async_elastic_session):
    """TEST With receiving group:remove msg - if MO exists and MO.groups = group name from msg:
    - updates MO.groups = None,
    - New value is in index (available for search)"""

    await create_default_data(
        list_of_mo_data=[MO_DATA_WITH_ONE_GROUP],
        async_elastic_session=async_elastic_session,
        tmo_id=MO_DATA_WITH_ONE_GROUP["tmo_id"],
    )

    search_query = {"match": {"id": MO_DATA_WITH_ONE_GROUP["id"]}}
    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )
    assert search_res["hits"]["total"]["value"] == 1
    assert (
        search_res["hits"]["hits"][0]["_source"].get("groups")
        == TEST_GROUP_NAME
    )

    kafka_msg = create_cleared_kafka_msg_for_group_topic(
        mo_ids=[DEFAULT_MO_DATA["id"]],
        tmo_id=DEFAULT_MO_DATA["tmo_id"],
        group_name=TEST_GROUP_NAME,
        msg_class_name="group",
        msg_event="remove",
    )

    handler = GroupTopicHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # new mo.groups is None
    search_query = {"match": {"id": MO_DATA_WITH_ONE_GROUP["id"]}}
    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )
    assert search_res["hits"]["total"]["value"] == 1
    assert search_res["hits"]["hits"][0]["_source"].get("groups") is None

    # new mo.groups value available in search

    search_query = {"bool": {"must_not": [{"exists": {"field": "groups"}}]}}
    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )
    assert search_res["hits"]["total"]["value"] == 1
    assert (
        search_res["hits"]["hits"][0]["_source"]["id"]
        == MO_DATA_WITH_ONE_GROUP["id"]
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_group_handler_with_remove_event_case_3(async_elastic_session):
    """TEST With receiving group:remove msg - if MO exists and MO.groups =
    two or more groups names and one or more equal group name from msg:
    - updates MO.groups = deletes only one group from array,
    - New value is in index (available for search)"""

    await create_default_data(
        list_of_mo_data=[MO_DATA_WITH_TWO_GROUPS],
        async_elastic_session=async_elastic_session,
        tmo_id=MO_DATA_WITH_TWO_GROUPS["tmo_id"],
    )

    search_query = {"match": {"id": MO_DATA_WITH_TWO_GROUPS["id"]}}
    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )
    assert search_res["hits"]["total"]["value"] == 1
    assert search_res["hits"]["hits"][0]["_source"].get("groups") == [
        TEST_GROUP_NAME,
        "Some Group 2",
    ]

    kafka_msg = create_cleared_kafka_msg_for_group_topic(
        mo_ids=[DEFAULT_MO_DATA["id"]],
        tmo_id=DEFAULT_MO_DATA["tmo_id"],
        group_name=TEST_GROUP_NAME,
        msg_class_name="group",
        msg_event="remove",
    )

    handler = GroupTopicHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # new mo.groups is None
    search_query = {"match": {"id": MO_DATA_WITH_TWO_GROUPS["id"]}}
    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )
    assert search_res["hits"]["total"]["value"] == 1
    assert (
        search_res["hits"]["hits"][0]["_source"].get("groups") == "Some Group 2"
    )

    # new mo.groups value available in search

    search_query = {"match": {"groups": "Some Group 2"}}
    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )
    assert search_res["hits"]["total"]["value"] == 1
    assert (
        search_res["hits"]["hits"][0]["_source"]["id"]
        == MO_DATA_WITH_TWO_GROUPS["id"]
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_group_handler_with_remove_event_case_4(async_elastic_session):
    """TEST With receiving group:remove msg - if exist two or more MO with the same group and group name =
    group name from msg:
    - updates MO.groups only for one MO, the id of which is specified in the message"""
    one_more_mo = dict()
    one_more_mo.update(MO_DATA_WITH_ONE_GROUP)
    one_more_mo["id"] += 1

    await create_default_data(
        list_of_mo_data=[MO_DATA_WITH_ONE_GROUP, one_more_mo],
        async_elastic_session=async_elastic_session,
        tmo_id=MO_DATA_WITH_ONE_GROUP["tmo_id"],
    )

    search_query = {"match": {"groups": MO_DATA_WITH_ONE_GROUP["groups"]}}
    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )
    assert search_res["hits"]["total"]["value"] == 2
    assert (
        search_res["hits"]["hits"][0]["_source"].get("groups")
        == TEST_GROUP_NAME
    )

    kafka_msg = create_cleared_kafka_msg_for_group_topic(
        mo_ids=[MO_DATA_WITH_ONE_GROUP["id"]],
        tmo_id=DEFAULT_MO_DATA["tmo_id"],
        group_name=TEST_GROUP_NAME,
        msg_class_name="group",
        msg_event="remove",
    )

    handler = GroupTopicHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # new mo.groups is None
    search_query = {"match": {"id": MO_DATA_WITH_ONE_GROUP["id"]}}
    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )
    assert search_res["hits"]["total"]["value"] == 1
    assert search_res["hits"]["hits"][0]["_source"].get("groups") is None

    search_query = {"match": {"id": one_more_mo["id"]}}
    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )
    assert search_res["hits"]["total"]["value"] == 1
    assert (
        search_res["hits"]["hits"][0]["_source"].get("groups")
        == TEST_GROUP_NAME
    )
