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
async def test_group_handler_with_delete_event_case_1(async_elastic_session):
    """TEST With receiving group:delete msg - if MO does not exist:
    - does not raise error,
    - does update MO"""

    await create_default_data(
        mo_data=MO_DATA_WITH_TMO,
        async_elastic_session=async_elastic_session,
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
        msg_event="delete",
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
async def test_group_handler_with_delete_event_case_2(async_elastic_session):
    """TEST With receiving group:delete msg - if MO exists and MO.groups = deleted group name:
    - updates MO.groups = None,
    - New value is in index (available for search)"""

    await create_default_data(
        mo_data=MO_DATA_WITH_ONE_GROUP,
        async_elastic_session=async_elastic_session,
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
        msg_event="delete",
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
async def test_group_handler_with_delete_event_case_3(async_elastic_session):
    """TEST With receiving group:delete msg - if MO exists and MO.groups = two or more groups names:
    - updates MO.groups = deletes only one group from array,
    - New value is in index (available for search)"""

    await create_default_data(
        mo_data=MO_DATA_WITH_TWO_GROUPS,
        async_elastic_session=async_elastic_session,
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
        msg_event="delete",
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
