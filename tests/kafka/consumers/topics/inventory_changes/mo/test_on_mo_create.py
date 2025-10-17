import pytest

from elastic.config import INVENTORY_OBJ_INDEX_PREFIX
from indexes_mapping.inventory.mapping import INVENTORY_PARAMETERS_FIELD_NAME
from services.inventory_services.kafka.consumers.inventory_changes.utils import (
    InventoryChangesHandler,
)
from tests.kafka.consumers.topics.inventory_changes.utils import (
    create_cleared_kafka_mo_msg,
    create_default_tmo_in_elastic,
)

MSG_EVENT = "created"
MSG_CLASS_NAME = "MO"

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

MO_DATA_TMO_DOES_NOT_EXIST = {
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

MO_DATA_TMO_EXISTS = {
    "id": 1,
    "name": "566_LF-314_U_169500647943",
    "active": True,
    "tmo_id": TMO_DATA["id"],
    "latitude": 0.0,
    "longitude": 0.0,
    "p_id": 0,
    "point_a_id": 0,
    "point_b_id": 0,
    "model": "",
    "version": 0,
    "status": "",
}

MSG_AS_DICT_TMO_DOES_NOT_EXIST = {"objects": [MO_DATA_TMO_DOES_NOT_EXIST]}

MSG_AS_DICT_TMO_EXIST = {"objects": [MO_DATA_TMO_EXISTS]}


@pytest.mark.asyncio(loop_scope="session")
async def test_on_create_mo_case_1(async_elastic_session):
    """TEST On receiving MO:created msg - if mo.tmo does not exist - does not raise error."""

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[MO_DATA_TMO_DOES_NOT_EXIST], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    try:
        await handler.process_the_message()
    except Exception:
        assert False


@pytest.mark.asyncio(loop_scope="session")
async def test_on_create_mo_case_2(async_elastic_session):
    """TEST On receiving MO:created msg - if mo.tmo does not exist - does not create mo record."""

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[MO_DATA_TMO_DOES_NOT_EXIST], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    tmo_id_of_mo_with_not_existing_tmo = MO_DATA_TMO_DOES_NOT_EXIST["id"]
    search_query = {"match": {"id": tmo_id_of_mo_with_not_existing_tmo}}

    res = await async_elastic_session.search(
        index=f"{INVENTORY_OBJ_INDEX_PREFIX}*",
        query=search_query,
        track_total_hits=True,
    )

    assert res["hits"]["total"]["value"] == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_on_create_mo_case_3(async_elastic_session):
    """TEST On receiving MO:created msg - if mo.tmo exists - creates mo record."""
    id_of_mo_with_existing_tmo = MO_DATA_TMO_EXISTS["id"]

    await create_default_tmo_in_elastic(
        async_elastic_session=async_elastic_session, default_tmo_data=TMO_DATA
    )

    search_query = {"match": {"id": id_of_mo_with_existing_tmo}}
    # before
    before_res = await async_elastic_session.search(
        index=f"{INVENTORY_OBJ_INDEX_PREFIX}*",
        query=search_query,
        track_total_hits=True,
    )
    assert before_res["hits"]["total"]["value"] == 0

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[MO_DATA_TMO_EXISTS], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    search_query = {"match": {"id": id_of_mo_with_existing_tmo}}
    after_res = await async_elastic_session.search(
        index=f"{INVENTORY_OBJ_INDEX_PREFIX}*",
        query=search_query,
        track_total_hits=True,
    )

    assert after_res["hits"]["total"]["value"] == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_on_create_mo_case_4(async_elastic_session):
    """TEST On receiving MO:created msg - if mo.tmo exists - creates mo record and all attrs available in search."""

    await create_default_tmo_in_elastic(
        async_elastic_session=async_elastic_session, default_tmo_data=TMO_DATA
    )

    search_query = {"match": {"id": MO_DATA_TMO_EXISTS["id"]}}
    # before
    before_res = await async_elastic_session.search(
        index=f"{INVENTORY_OBJ_INDEX_PREFIX}*",
        query=search_query,
        track_total_hits=True,
    )
    assert before_res["hits"]["total"]["value"] == 0

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[MO_DATA_TMO_EXISTS], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    not_none_values = {k: v for k, v in MO_DATA_TMO_EXISTS.items() if v}

    assert len(not_none_values) > 1

    for k, v in not_none_values.items():
        search_query = {"match": {k: v}}
        res = await async_elastic_session.search(
            index=f"{INVENTORY_OBJ_INDEX_PREFIX}*",
            query=search_query,
            track_total_hits=True,
        )

        assert res["hits"]["total"]["value"] == 1
        assert (
            res["hits"]["hits"][0]["_source"]["id"] == MO_DATA_TMO_EXISTS["id"]
        )


@pytest.mark.asyncio(loop_scope="session")
async def test_on_create_mo_case_5(async_elastic_session):
    """TEST On receiving MO:created msg - if mo.tmo exists - creates mo record and
    if mo has parent_id in index adds attr parent_name."""
    await create_default_tmo_in_elastic(
        async_elastic_session=async_elastic_session, default_tmo_data=TMO_DATA
    )

    search_query = {"match": {"id": MO_DATA_TMO_EXISTS["id"]}}
    # before
    before_res = await async_elastic_session.search(
        index=f"{INVENTORY_OBJ_INDEX_PREFIX}*",
        query=search_query,
        track_total_hits=True,
    )
    assert before_res["hits"]["total"]["value"] == 0

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[MO_DATA_TMO_EXISTS], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    mo_with_parent = dict()
    mo_with_parent.update(MO_DATA_TMO_EXISTS)
    mo_with_parent["p_id"] = MO_DATA_TMO_EXISTS["id"]
    mo_with_parent["id"] += 1
    mo_with_parent["name"] += "MO with parent"

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[mo_with_parent], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    search_query = {"match": {"id": mo_with_parent["id"]}}

    res = await async_elastic_session.search(
        index=f"{INVENTORY_OBJ_INDEX_PREFIX}*",
        query=search_query,
        track_total_hits=True,
    )

    assert (
        res["hits"]["hits"][0]["_source"]["parent_name"]
        == MO_DATA_TMO_EXISTS["name"]
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_on_create_mo_case_6(async_elastic_session):
    """TEST On receiving MO:created msg - if mo.tmo exists - creates mo record and
    if mo has no p_id adds attr parent_name None."""
    await create_default_tmo_in_elastic(
        async_elastic_session=async_elastic_session, default_tmo_data=TMO_DATA
    )

    search_query = {"match": {"id": MO_DATA_TMO_EXISTS["id"]}}
    # before
    before_res = await async_elastic_session.search(
        index=f"{INVENTORY_OBJ_INDEX_PREFIX}*",
        query=search_query,
        track_total_hits=True,
    )
    assert before_res["hits"]["total"]["value"] == 0
    new_mo_data = dict()
    new_mo_data.update(MO_DATA_TMO_EXISTS)
    new_mo_data["p_id"] = None

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[new_mo_data], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    res = await async_elastic_session.search(
        index=f"{INVENTORY_OBJ_INDEX_PREFIX}*",
        query=search_query,
        track_total_hits=True,
    )

    assert res["hits"]["hits"][0]["_source"]["parent_name"] is None


@pytest.mark.asyncio(loop_scope="session")
async def test_on_create_mo_case_7(async_elastic_session):
    """TEST On receiving MO:created msg - if mo.tmo exists:
    - creates mo record
    - new record must has parameters field (INVENTORY_PARAMETERS_FIELD_NAME)"""
    id_of_mo_with_existing_tmo = MO_DATA_TMO_EXISTS["id"]
    await create_default_tmo_in_elastic(
        async_elastic_session=async_elastic_session, default_tmo_data=TMO_DATA
    )

    search_query = {"match": {"id": id_of_mo_with_existing_tmo}}
    # before
    before_res = await async_elastic_session.search(
        index=f"{INVENTORY_OBJ_INDEX_PREFIX}*",
        query=search_query,
        track_total_hits=True,
    )
    assert before_res["hits"]["total"]["value"] == 0

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[MO_DATA_TMO_EXISTS], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    search_query = {"match": {"id": id_of_mo_with_existing_tmo}}
    after_res = await async_elastic_session.search(
        index=f"{INVENTORY_OBJ_INDEX_PREFIX}*",
        query=search_query,
        track_total_hits=True,
    )

    assert after_res["hits"]["total"]["value"] == 1
    assert isinstance(
        after_res["hits"]["hits"][0]["_source"].get(
            INVENTORY_PARAMETERS_FIELD_NAME
        ),
        dict,
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_on_create_mo_case_8(async_elastic_session):
    """TEST On receiving MO:created msg - if mo.tmo exists and:
     mo.point_a_id is not Null and mo with id = mo.point_a_id exists:
    - creates mo record
    - point_a_name of new record = mo.name where id = new_record.point_a_id"""
    await create_default_tmo_in_elastic(
        async_elastic_session=async_elastic_session, default_tmo_data=TMO_DATA
    )

    search_query = {"match": {"id": MO_DATA_TMO_EXISTS["id"]}}
    # before
    before_res = await async_elastic_session.search(
        index=f"{INVENTORY_OBJ_INDEX_PREFIX}*",
        query=search_query,
        track_total_hits=True,
    )
    assert before_res["hits"]["total"]["value"] == 0

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[MO_DATA_TMO_EXISTS], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    mo_with_point_a = dict()
    mo_with_point_a.update(MO_DATA_TMO_EXISTS)
    mo_with_point_a["point_a_id"] = MO_DATA_TMO_EXISTS["id"]
    mo_with_point_a["id"] += 1
    mo_with_point_a["name"] += "MO with parent"

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[mo_with_point_a], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    search_query = {"match": {"id": mo_with_point_a["id"]}}

    res = await async_elastic_session.search(
        index=f"{INVENTORY_OBJ_INDEX_PREFIX}*",
        query=search_query,
        track_total_hits=True,
    )
    mo_data = res["hits"]["hits"][0]["_source"]
    assert mo_data.get("point_a_name") is not None
    assert mo_data.get("point_a_name") == MO_DATA_TMO_EXISTS["name"]


@pytest.mark.asyncio(loop_scope="session")
async def test_on_create_mo_case_9(async_elastic_session):
    """TEST On receiving MO:created msg - if mo.tmo exists and:
     mo.point_a_id is not Null and mo with id = mo.point_b_id exists:
    - creates mo record
    - point_b_name of new record = mo.name where id = new_record.point_b_id"""
    await create_default_tmo_in_elastic(
        async_elastic_session=async_elastic_session, default_tmo_data=TMO_DATA
    )

    search_query = {"match": {"id": MO_DATA_TMO_EXISTS["id"]}}
    # before
    before_res = await async_elastic_session.search(
        index=f"{INVENTORY_OBJ_INDEX_PREFIX}*",
        query=search_query,
        track_total_hits=True,
    )
    assert before_res["hits"]["total"]["value"] == 0

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[MO_DATA_TMO_EXISTS], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    mo_with_point_a = dict()
    mo_with_point_a.update(MO_DATA_TMO_EXISTS)
    mo_with_point_a["point_b_id"] = MO_DATA_TMO_EXISTS["id"]
    mo_with_point_a["id"] += 1
    mo_with_point_a["name"] += "MO with parent"

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[mo_with_point_a], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    search_query = {"match": {"id": mo_with_point_a["id"]}}

    res = await async_elastic_session.search(
        index=f"{INVENTORY_OBJ_INDEX_PREFIX}*",
        query=search_query,
        track_total_hits=True,
    )
    mo_data = res["hits"]["hits"][0]["_source"]
    assert mo_data.get("point_b_name") is not None
    assert mo_data.get("point_b_name") == MO_DATA_TMO_EXISTS["name"]
