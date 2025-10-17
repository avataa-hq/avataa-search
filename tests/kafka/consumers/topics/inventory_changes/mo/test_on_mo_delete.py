import pytest
from elasticsearch import AsyncElasticsearch

from elastic.config import (
    INVENTORY_TMO_INDEX_V2,
    DEFAULT_SETTING_FOR_MO_INDEXES,
    INVENTORY_OBJ_INDEX_PREFIX,
)
from elastic.query_builder_service.inventory_index.utils.index_utils import (
    get_index_name_by_tmo,
)
from indexes_mapping.inventory.mapping import (
    INVENTORY_OBJ_INDEX_MAPPING,
    INVENTORY_PARAMETERS_FIELD_NAME,
)
from services.inventory_services.kafka.consumers.inventory_changes.utils import (
    InventoryChangesHandler,
)
from tests.kafka.consumers.topics.inventory_changes.utils import (
    create_cleared_kafka_mo_msg,
)

TMO_DATA = {
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
    "creation_date": "2000-12-12",
    "modification_date": "2000-12-12",
    "primary": None,
    "points_constraint_by_tmo": None,
}

EXISTING_PARENT_MO_DATA_KAFKA = {
    "id": 10,
    "name": "Existing Parent",
    "active": True,
    "tmo_id": TMO_DATA["id"],
    "latitude": 0.0,
    "longitude": 0.0,
    "p_id": 0,
    "point_a_id": 0,
    "point_b_id": 0,
    "model": "",
    "version": 1,
    "status": "25",
}

EXISTING_PARENT_MO_DATA_ELASTIC = dict()
EXISTING_PARENT_MO_DATA_ELASTIC.update(EXISTING_PARENT_MO_DATA_KAFKA)
EXISTING_PARENT_MO_DATA_ELASTIC[INVENTORY_PARAMETERS_FIELD_NAME] = {"1": 1}

EXISTING_CHILD_MO_DATA_KAFKA = {
    "id": 11,
    "name": "Existing Child",
    "active": True,
    "tmo_id": TMO_DATA["id"],
    "latitude": 0.0,
    "longitude": 0.0,
    "p_id": EXISTING_PARENT_MO_DATA_KAFKA["id"],
    "point_a_id": 0,
    "point_b_id": 0,
    "model": "",
    "version": 1,
    "status": "25",
}

EXISTING_CHILD_MO_DATA_ELASTIC = dict()
EXISTING_CHILD_MO_DATA_ELASTIC.update(EXISTING_CHILD_MO_DATA_KAFKA)
EXISTING_CHILD_MO_DATA_ELASTIC["parent_name"] = EXISTING_PARENT_MO_DATA_KAFKA[
    "name"
]
EXISTING_CHILD_MO_DATA_ELASTIC[INVENTORY_PARAMETERS_FIELD_NAME] = {"1": 1}

NOT_EXISTING_MO_DATA = {
    "id": 30000,
    "name": "NOT Existing MO",
    "active": False,
    "tmo_id": TMO_DATA["id"],
    "latitude": 0.0,
    "longitude": 0.0,
    "p_id": 0,
    "point_a_id": 0,
    "point_b_id": 0,
    "model": "",
    "version": 25,
    "status": "24",
}

MSG_CLASS_NAME = "MO"
MSG_EVENT = "deleted"

ALL_MO_INDEXES_PATTERN = f"{INVENTORY_OBJ_INDEX_PREFIX}*"


async def create_default_data(
    tmo_id: int, async_elastic_session: AsyncElasticsearch
):
    new_index_name = get_index_name_by_tmo(tmo_id=tmo_id)
    # create record
    await async_elastic_session.index(
        index=INVENTORY_TMO_INDEX_V2, id=tmo_id, document=TMO_DATA, refresh=True
    )

    # create index
    await async_elastic_session.indices.create(
        index=new_index_name,
        mappings=INVENTORY_OBJ_INDEX_MAPPING,
        settings=DEFAULT_SETTING_FOR_MO_INDEXES,
    )

    # create default mo parent record
    await async_elastic_session.index(
        index=new_index_name,
        id=EXISTING_PARENT_MO_DATA_ELASTIC["id"],
        document=EXISTING_PARENT_MO_DATA_ELASTIC,
        refresh="true",
    )

    # create default mo child record
    await async_elastic_session.index(
        index=new_index_name,
        id=EXISTING_CHILD_MO_DATA_ELASTIC["id"],
        document=EXISTING_CHILD_MO_DATA_ELASTIC,
        refresh="true",
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_on_delete_mo_case_1(async_elastic_session):
    """TEST On receiving MO:deleted msg - if mo does not exist - does not raise error."""
    search_query = {"match": {"id": NOT_EXISTING_MO_DATA["id"]}}
    search_res = await async_elastic_session.search(
        index=ALL_MO_INDEXES_PATTERN, query=search_query, track_total_hits=True
    )

    assert search_res["hits"]["total"]["value"] == 0

    try:
        kafka_msg = create_cleared_kafka_mo_msg(
            list_of_mo_data=[NOT_EXISTING_MO_DATA], msg_event=MSG_EVENT
        )

        handler = InventoryChangesHandler(kafka_msg=kafka_msg)
        await handler.process_the_message()
    except Exception:
        assert False


@pytest.mark.asyncio(loop_scope="session")
async def test_on_delete_mo_case_2(async_elastic_session):
    """TEST On receiving MO:deleted msg - if mo exists - deletes it."""
    await create_default_data(
        EXISTING_PARENT_MO_DATA_KAFKA["tmo_id"], async_elastic_session
    )

    search_query = {"match": {"id": EXISTING_PARENT_MO_DATA_KAFKA["id"]}}
    search_res = await async_elastic_session.search(
        index=ALL_MO_INDEXES_PATTERN, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[EXISTING_PARENT_MO_DATA_KAFKA], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    search_res = await async_elastic_session.search(
        index=ALL_MO_INDEXES_PATTERN, query=search_query, track_total_hits=True
    )

    assert search_res["hits"]["total"]["value"] == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_on_delete_mo_case_3(async_elastic_session):
    """TEST On receiving MO:deleted msg - if mo exists:
    - deletes it
    - if mo has children - changes parent_name and p_id on children mo"""
    await create_default_data(
        EXISTING_PARENT_MO_DATA_KAFKA["tmo_id"], async_elastic_session
    )

    search_query = {"match": {"p_id": EXISTING_PARENT_MO_DATA_KAFKA["id"]}}
    search_res = await async_elastic_session.search(
        index=ALL_MO_INDEXES_PATTERN, query=search_query, track_total_hits=True
    )

    assert (
        search_res["hits"]["hits"][0]["_source"]["id"]
        == EXISTING_CHILD_MO_DATA_KAFKA["id"]
    )
    assert (
        search_res["hits"]["hits"][0]["_source"]["parent_name"]
        == EXISTING_PARENT_MO_DATA_KAFKA["name"]
    )
    assert (
        search_res["hits"]["hits"][0]["_source"]["p_id"]
        == EXISTING_PARENT_MO_DATA_KAFKA["id"]
    )
    assert search_res["hits"]["total"]["value"] == 1

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[EXISTING_PARENT_MO_DATA_KAFKA], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    search_query = {"match": {"id": EXISTING_CHILD_MO_DATA_KAFKA["id"]}}
    search_res = await async_elastic_session.search(
        index=ALL_MO_INDEXES_PATTERN, query=search_query, track_total_hits=True
    )

    assert search_res["hits"]["hits"][0]["_source"]["parent_name"] is None
    assert search_res["hits"]["hits"][0]["_source"]["p_id"] is None
    assert search_res["hits"]["total"]["value"] == 1

    # is index refreshed
    search_query = {"bool": {"must_not": [{"exists": {"field": "p_id"}}]}}
    search_res = await async_elastic_session.search(
        index=ALL_MO_INDEXES_PATTERN, query=search_query, track_total_hits=True
    )

    assert (
        search_res["hits"]["hits"][0]["_source"]["id"]
        == EXISTING_CHILD_MO_DATA_KAFKA["id"]
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_on_delete_mo_case_4(async_elastic_session):
    """TEST On receiving MO:deleted msg - if mo exists:
    - deletes it
    - if mo exists in point_a_id of other records - changes point_a_name of this records to None"""
    await create_default_data(
        EXISTING_PARENT_MO_DATA_KAFKA["tmo_id"], async_elastic_session
    )

    mo_with_point_a = dict()
    mo_with_point_a.update(EXISTING_CHILD_MO_DATA_KAFKA)
    mo_with_point_a["point_a_id"] = EXISTING_CHILD_MO_DATA_KAFKA["id"]
    mo_with_point_a["point_a_name"] = EXISTING_CHILD_MO_DATA_KAFKA["name"]
    mo_with_point_a["id"] += EXISTING_CHILD_MO_DATA_KAFKA["id"]

    new_index_name = get_index_name_by_tmo(tmo_id=mo_with_point_a["tmo_id"])

    await async_elastic_session.index(
        index=new_index_name,
        id=mo_with_point_a["id"],
        document=mo_with_point_a,
        refresh="true",
    )

    search_query = {"match": {"id": mo_with_point_a["id"]}}
    search_res = await async_elastic_session.search(
        index=ALL_MO_INDEXES_PATTERN, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1
    assert (
        search_res["hits"]["hits"][0]["_source"]["point_a_name"]
        == mo_with_point_a["point_a_name"]
    )

    del mo_with_point_a["point_a_name"]
    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[EXISTING_CHILD_MO_DATA_KAFKA], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    search_query = {"match": {"id": mo_with_point_a["id"]}}
    search_res = await async_elastic_session.search(
        index=ALL_MO_INDEXES_PATTERN, query=search_query, track_total_hits=True
    )

    assert search_res["hits"]["total"]["value"] == 1
    assert search_res["hits"]["hits"][0]["_source"]["point_a_name"] is None

    # is index refreshed
    search_query = {
        "bool": {
            "must_not": {"exists": {"field": "point_a_name"}},
            "must": {"match": {"id": mo_with_point_a["id"]}},
        }
    }
    search_res = await async_elastic_session.search(
        index=ALL_MO_INDEXES_PATTERN, query=search_query, track_total_hits=True
    )

    assert search_res["hits"]["total"]["value"] == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_on_delete_mo_case_5(async_elastic_session):
    """TEST On receiving MO:deleted msg - if mo exists:
    - deletes it
    - if mo exists in point_a_id of other records - changes point_a_name of this records to None"""
    await create_default_data(
        EXISTING_PARENT_MO_DATA_KAFKA["tmo_id"], async_elastic_session
    )

    mo_with_point_b = dict()
    mo_with_point_b.update(EXISTING_CHILD_MO_DATA_KAFKA)
    mo_with_point_b["point_b_id"] = EXISTING_CHILD_MO_DATA_KAFKA["id"]
    mo_with_point_b["point_b_name"] = EXISTING_CHILD_MO_DATA_KAFKA["name"]
    mo_with_point_b["id"] += EXISTING_CHILD_MO_DATA_KAFKA["id"]

    new_index_name = get_index_name_by_tmo(tmo_id=mo_with_point_b["tmo_id"])

    await async_elastic_session.index(
        index=new_index_name,
        id=mo_with_point_b["id"],
        document=mo_with_point_b,
        refresh="true",
    )

    search_query = {"match": {"id": mo_with_point_b["id"]}}
    search_res = await async_elastic_session.search(
        index=ALL_MO_INDEXES_PATTERN, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1
    assert (
        search_res["hits"]["hits"][0]["_source"]["point_b_name"]
        == mo_with_point_b["point_b_name"]
    )
    del mo_with_point_b["point_b_name"]
    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[EXISTING_CHILD_MO_DATA_KAFKA], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    search_query = {"match": {"id": mo_with_point_b["id"]}}
    search_res = await async_elastic_session.search(
        index=ALL_MO_INDEXES_PATTERN, query=search_query, track_total_hits=True
    )

    assert search_res["hits"]["total"]["value"] == 1
    assert search_res["hits"]["hits"][0]["_source"]["point_b_name"] is None

    # is index refreshed
    search_query = {
        "bool": {
            "must_not": [{"exists": {"field": "point_b_name"}}],
            "must": [{"match": {"id": mo_with_point_b["id"]}}],
        }
    }
    search_res = await async_elastic_session.search(
        index=ALL_MO_INDEXES_PATTERN, query=search_query, track_total_hits=True
    )

    assert search_res["hits"]["total"]["value"] == 1
