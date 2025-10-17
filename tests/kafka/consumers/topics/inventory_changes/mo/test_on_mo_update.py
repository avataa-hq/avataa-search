import pytest
from elasticsearch import AsyncElasticsearch

from elastic.config import (
    INVENTORY_TMO_INDEX_V2,
    DEFAULT_SETTING_FOR_MO_INDEXES,
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

DEFAULT_MO_DATA = {
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
    "version": 1,
    "status": "25",
    INVENTORY_PARAMETERS_FIELD_NAME: {"1": 1},
}

UPDATED_MO_DATA = {
    "id": 1,
    "name": "Updated name",
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

NOT_EXISTING_MO_DATA = {
    "id": 10000,
    "name": "Updated name",
    "active": False,
    "tmo_id": TMO_DATA["id"],
    "latitude": 0.0,
    "longitude": 0.0,
    "p_id": 0,
    "point_a_id": 0,
    "point_b_id": 0,
    "model": "",
    "version": 25,
    "status": "",
}

MSG_CLASS_NAME = "MO"
MSG_EVENT = "updated"

# MSG_AS_DICT = {'objects': [UPDATED_MO_DATA]}
MO_INDEX_NAME = get_index_name_by_tmo(TMO_DATA["id"])


async def add_tmo_data_and_create_tmo_index(
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
    # create default mo record
    await async_elastic_session.index(
        index=new_index_name,
        id=DEFAULT_MO_DATA["id"],
        document=DEFAULT_MO_DATA,
        refresh="true",
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_mo_case_1(async_elastic_session):
    """TEST On receiving MO:updated msg - if tmo does not exist - does not raise error"""

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[UPDATED_MO_DATA], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    try:
        await handler.process_the_message()
    except Exception:
        assert False


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_mo_case_2(async_elastic_session):
    """TEST On receiving MO:updated msg - if mo does not exist - does not raise error"""

    await add_tmo_data_and_create_tmo_index(
        DEFAULT_MO_DATA["tmo_id"], async_elastic_session
    )

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[NOT_EXISTING_MO_DATA], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    try:
        await handler.process_the_message()
    except Exception:
        assert False


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_mo_case_3(async_elastic_session):
    """TEST On receiving MO:updated msg - if mo exists - updates record"""

    await add_tmo_data_and_create_tmo_index(
        DEFAULT_MO_DATA["tmo_id"], async_elastic_session
    )

    search_query = {"match": {"id": DEFAULT_MO_DATA["id"]}}
    mo_data_before = await async_elastic_session.search(
        index=MO_INDEX_NAME, query=search_query, size=1
    )

    mo_data_before = mo_data_before["hits"]["hits"][0]["_source"]
    assert mo_data_before["name"] != UPDATED_MO_DATA["name"]

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[UPDATED_MO_DATA], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    mo_data_after = await async_elastic_session.search(
        index=MO_INDEX_NAME, query=search_query, size=1
    )

    mo_data_after = mo_data_after["hits"]["hits"][0]["_source"]
    assert mo_data_after["name"] == UPDATED_MO_DATA["name"]


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_mo_case_4(async_elastic_session):
    """TEST On receiving MO:updated msg - if mo exists - partial updates record, does not erase params"""

    await add_tmo_data_and_create_tmo_index(
        DEFAULT_MO_DATA["tmo_id"], async_elastic_session
    )

    search_query = {"match": {"id": DEFAULT_MO_DATA["id"]}}
    mo_data_before = await async_elastic_session.search(
        index=MO_INDEX_NAME, query=search_query, size=1
    )

    mo_data_before = mo_data_before["hits"]["hits"][0]["_source"]
    assert mo_data_before["name"] != UPDATED_MO_DATA["name"]
    assert mo_data_before[INVENTORY_PARAMETERS_FIELD_NAME]

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[UPDATED_MO_DATA], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    mo_data_after = await async_elastic_session.search(
        index=MO_INDEX_NAME, query=search_query, size=1
    )

    mo_data_after = mo_data_after["hits"]["hits"][0]["_source"]
    assert mo_data_after["name"] == UPDATED_MO_DATA["name"]
    assert (
        mo_data_after[INVENTORY_PARAMETERS_FIELD_NAME]
        == DEFAULT_MO_DATA[INVENTORY_PARAMETERS_FIELD_NAME]
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_mo_case_5(async_elastic_session):
    """TEST On receiving MO:updated msg - if mo exists - updates record, all updated attrs available in search"""

    await add_tmo_data_and_create_tmo_index(
        DEFAULT_MO_DATA["tmo_id"], async_elastic_session
    )

    search_query = {"match": {"id": DEFAULT_MO_DATA["id"]}}
    mo_data_before = await async_elastic_session.search(
        index=MO_INDEX_NAME, query=search_query, size=1
    )

    mo_data_before = mo_data_before["hits"]["hits"][0]["_source"]

    changes_more_than_2 = 0
    for k, v in mo_data_before.items():
        if v:
            value_will_be = UPDATED_MO_DATA.get(k)
            if value_will_be:
                if v != value_will_be:
                    changes_more_than_2 += 1

    assert changes_more_than_2 > 2

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[UPDATED_MO_DATA], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    for k, v in UPDATED_MO_DATA.items():
        if v:
            search_query = {"match": {k: v}}
            search_res = await async_elastic_session.search(
                index=MO_INDEX_NAME, query=search_query
            )
            assert search_res["hits"]["total"]["value"] == 1
            assert (
                search_res["hits"]["hits"][0]["_source"]["id"]
                == UPDATED_MO_DATA["id"]
            )


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_mo_case_6(async_elastic_session):
    """TEST On receiving MO:updated msg - if mo exists - updates record,
    if name was changed - changes parent_name in all children"""

    await add_tmo_data_and_create_tmo_index(
        DEFAULT_MO_DATA["tmo_id"], async_elastic_session
    )

    # create data
    child_mo_id = 25555555
    new_mo_data = dict()
    new_mo_data.update(DEFAULT_MO_DATA)
    del new_mo_data[INVENTORY_PARAMETERS_FIELD_NAME]
    new_mo_data["name"] = "Child MO name"
    new_mo_data["id"] = child_mo_id
    new_mo_data["p_id"] = DEFAULT_MO_DATA["id"]

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[new_mo_data], msg_event="created"
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    search_query = {"match": {"id": child_mo_id}}
    mo_data_before = await async_elastic_session.search(
        index=MO_INDEX_NAME, query=search_query, size=1
    )

    mo_data_before = mo_data_before["hits"]["hits"][0]["_source"]

    assert mo_data_before.get("parent_name") != UPDATED_MO_DATA["name"]

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[UPDATED_MO_DATA], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    mo_data_after = await async_elastic_session.search(
        index=MO_INDEX_NAME, query=search_query, size=1
    )

    mo_data_after = mo_data_after["hits"]["hits"][0]["_source"]

    assert mo_data_after.get("parent_name") == UPDATED_MO_DATA["name"]


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_mo_case_7(async_elastic_session):
    """TEST On receiving MO:updated msg - if mo exists - updates record,
    if p_id was changed (was None become id of some existing mo) - changes name"""

    await add_tmo_data_and_create_tmo_index(
        DEFAULT_MO_DATA["tmo_id"], async_elastic_session
    )

    # create data DEFAULT_MO_DATA['id']
    child_mo_id = 25555555
    new_mo_data = dict()
    new_mo_data.update(DEFAULT_MO_DATA)
    new_mo_data["name"] = "Child MO name"
    new_mo_data["id"] = child_mo_id
    new_mo_data["p_id"] = None
    del new_mo_data[INVENTORY_PARAMETERS_FIELD_NAME]

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[new_mo_data], msg_event="created"
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    search_query = {"match": {"id": child_mo_id}}
    mo_data_before = await async_elastic_session.search(
        index=MO_INDEX_NAME, query=search_query, size=1
    )

    assert mo_data_before["hits"]["hits"][0]["_source"]["parent_name"] is None

    # update p_id
    new_mo_data["p_id"] = DEFAULT_MO_DATA["id"]

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[new_mo_data], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    mo_data_after = await async_elastic_session.search(
        index=MO_INDEX_NAME, query=search_query, size=1
    )

    assert (
        mo_data_after["hits"]["hits"][0]["_source"]["parent_name"]
        == DEFAULT_MO_DATA["name"]
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_mo_case_8(async_elastic_session):
    """TEST On receiving MO:updated msg - if mo exists - updates record,
    if p_id was changed (was some existing mo become none) - changes name"""

    await add_tmo_data_and_create_tmo_index(
        DEFAULT_MO_DATA["tmo_id"], async_elastic_session
    )

    # create data DEFAULT_MO_DATA['id']
    child_mo_id = 25555555
    new_mo_data = dict()
    new_mo_data.update(DEFAULT_MO_DATA)
    new_mo_data["name"] = "Child MO name"
    new_mo_data["id"] = child_mo_id
    new_mo_data["p_id"] = DEFAULT_MO_DATA["id"]
    del new_mo_data[INVENTORY_PARAMETERS_FIELD_NAME]

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[new_mo_data], msg_event="created"
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    search_query = {"match": {"id": child_mo_id}}
    mo_data_before = await async_elastic_session.search(
        index=MO_INDEX_NAME, query=search_query, size=1
    )

    assert (
        mo_data_before["hits"]["hits"][0]["_source"]["parent_name"]
        == DEFAULT_MO_DATA["name"]
    )

    # update p_id
    new_mo_data["p_id"] = None

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[new_mo_data], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    mo_data_after = await async_elastic_session.search(
        index=MO_INDEX_NAME, query=search_query, size=1
    )

    assert mo_data_after["hits"]["hits"][0]["_source"]["parent_name"] is None


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_mo_case_9(async_elastic_session):
    """TEST On receiving MO:updated msg - if mo exists - updates record,
    if p_id was changed (was some existing mo become some not existing) - changes name"""

    await add_tmo_data_and_create_tmo_index(
        DEFAULT_MO_DATA["tmo_id"], async_elastic_session
    )

    # create data DEFAULT_MO_DATA['id']
    child_mo_id = 25555555
    new_mo_data = dict()
    new_mo_data.update(DEFAULT_MO_DATA)
    new_mo_data["name"] = "Child MO name"
    new_mo_data["id"] = child_mo_id
    new_mo_data["p_id"] = DEFAULT_MO_DATA["id"]
    del new_mo_data[INVENTORY_PARAMETERS_FIELD_NAME]

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[new_mo_data], msg_event="created"
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    search_query = {"match": {"id": child_mo_id}}
    mo_data_before = await async_elastic_session.search(
        index=MO_INDEX_NAME, query=search_query, size=1
    )

    assert (
        mo_data_before["hits"]["hits"][0]["_source"]["parent_name"]
        == DEFAULT_MO_DATA["name"]
    )

    # update p_id
    new_mo_data["p_id"] = 444444444

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[new_mo_data], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    mo_data_after = await async_elastic_session.search(
        index=MO_INDEX_NAME, query=search_query, size=1
    )

    assert mo_data_after["hits"]["hits"][0]["_source"]["parent_name"] is None


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_mo_case_10(async_elastic_session):
    """TEST On receiving MO:updated msg - if mo exists - updates record,
    if p_id was changed (was not existing mo become some not existing mo) - changes name"""

    await add_tmo_data_and_create_tmo_index(
        DEFAULT_MO_DATA["tmo_id"], async_elastic_session
    )

    # create data DEFAULT_MO_DATA['id']
    child_mo_id = 25555555
    new_mo_data = dict()
    new_mo_data.update(DEFAULT_MO_DATA)
    new_mo_data["name"] = "Child MO name"
    new_mo_data["id"] = child_mo_id
    new_mo_data["p_id"] = 1212121
    del new_mo_data[INVENTORY_PARAMETERS_FIELD_NAME]

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[new_mo_data], msg_event="created"
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    search_query = {"match": {"id": child_mo_id}}
    mo_data_before = await async_elastic_session.search(
        index=MO_INDEX_NAME, query=search_query, size=1
    )

    assert mo_data_before["hits"]["hits"][0]["_source"]["parent_name"] is None

    # update p_id
    new_mo_data["p_id"] = 444444444

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[new_mo_data], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    mo_data_after = await async_elastic_session.search(
        index=MO_INDEX_NAME, query=search_query, size=1
    )

    assert mo_data_after["hits"]["hits"][0]["_source"]["parent_name"] is None


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_mo_case_11(async_elastic_session):
    """TEST On receiving MO:updated msg - if mo exists - updates record,
    if point_a_id was changed - changes point_a_name"""

    await add_tmo_data_and_create_tmo_index(
        DEFAULT_MO_DATA["tmo_id"], async_elastic_session
    )

    corresp_mo = dict()
    corresp_mo.update(UPDATED_MO_DATA)
    corresp_mo["id"] += 1
    corresp_mo["name"] = "Unique mo name"

    new_index = get_index_name_by_tmo(tmo_id=int(corresp_mo["tmo_id"]))

    await async_elastic_session.index(
        index=new_index,
        id=corresp_mo["id"],
        document=corresp_mo,
        refresh="true",
    )

    point_a_mo = dict()
    point_a_mo.update(corresp_mo)
    point_a_mo["id"] += 1
    point_a_mo["name"] = "A name"
    point_a_mo["point_a_id"] = None

    await async_elastic_session.index(
        index=new_index,
        id=point_a_mo["id"],
        document=point_a_mo,
        refresh="true",
    )

    # before
    search_query = {"match": {"id": point_a_mo["id"]}}
    mo_data_before = await async_elastic_session.search(
        index=MO_INDEX_NAME, query=search_query, size=1
    )

    mo_data_before = mo_data_before["hits"]["hits"][0]["_source"]

    assert mo_data_before.get("point_a_id") is None
    assert mo_data_before.get("point_a_name") is None

    point_a_mo["point_a_id"] = corresp_mo["id"]
    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[point_a_mo], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    mo_data_after = await async_elastic_session.search(
        index=MO_INDEX_NAME, query=search_query, size=1
    )

    mo_data_after = mo_data_after["hits"]["hits"][0]["_source"]

    assert mo_data_after.get("point_a_id") == corresp_mo["id"]
    assert mo_data_after.get("point_a_name") == corresp_mo["name"]


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_mo_case_12(async_elastic_session):
    """TEST On receiving MO:updated msg - if mo exists - updates record,
    if point_b_id was changed - changes point_b_name"""

    await add_tmo_data_and_create_tmo_index(
        DEFAULT_MO_DATA["tmo_id"], async_elastic_session
    )

    corresp_mo = dict()
    corresp_mo.update(UPDATED_MO_DATA)
    corresp_mo["id"] += 1
    corresp_mo["name"] = "Unique mo name"

    new_index = get_index_name_by_tmo(tmo_id=int(corresp_mo["tmo_id"]))

    await async_elastic_session.index(
        index=new_index,
        id=corresp_mo["id"],
        document=corresp_mo,
        refresh="true",
    )

    point_b_mo = dict()
    point_b_mo.update(corresp_mo)
    point_b_mo["id"] += 1
    point_b_mo["name"] = "A name"
    point_b_mo["point_b_id"] = None

    await async_elastic_session.index(
        index=new_index,
        id=point_b_mo["id"],
        document=point_b_mo,
        refresh="true",
    )

    # before
    search_query = {"match": {"id": point_b_mo["id"]}}
    mo_data_before = await async_elastic_session.search(
        index=MO_INDEX_NAME, query=search_query, size=1
    )

    mo_data_before = mo_data_before["hits"]["hits"][0]["_source"]

    assert mo_data_before.get("point_b_id") is None
    assert mo_data_before.get("point_b_name") is None

    point_b_mo["point_b_id"] = corresp_mo["id"]
    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[point_b_mo], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    mo_data_after = await async_elastic_session.search(
        index=MO_INDEX_NAME, query=search_query, size=1
    )

    mo_data_after = mo_data_after["hits"]["hits"][0]["_source"]

    assert mo_data_after.get("point_b_id") == corresp_mo["id"]
    assert mo_data_after.get("point_b_name") == corresp_mo["name"]


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_mo_case_13(async_elastic_session):
    """TEST On receiving MO:updated msg - if mo exists - updates record,
    if name was changed and MO.id exists in point_a_id of other objects - changes point_a_name for this objects"""

    await add_tmo_data_and_create_tmo_index(
        DEFAULT_MO_DATA["tmo_id"], async_elastic_session
    )
    updated_corr_name = "Updated corresp name"
    corresp_mo = dict()
    corresp_mo.update(UPDATED_MO_DATA)
    corresp_mo["id"] += 1
    corresp_mo["name"] = "Unique mo name"

    new_index = get_index_name_by_tmo(tmo_id=int(corresp_mo["tmo_id"]))

    await async_elastic_session.index(
        index=new_index,
        id=corresp_mo["id"],
        document=corresp_mo,
        refresh="true",
    )

    point_a_mo = dict()
    point_a_mo.update(corresp_mo)
    point_a_mo["id"] += 1
    point_a_mo["name"] = "A name"
    point_a_mo["point_a_id"] = corresp_mo["id"]

    await async_elastic_session.index(
        index=new_index,
        id=point_a_mo["id"],
        document=point_a_mo,
        refresh="true",
    )

    # before
    search_query = {"match": {"id": point_a_mo["id"]}}
    mo_data_before = await async_elastic_session.search(
        index=MO_INDEX_NAME, query=search_query, size=1
    )

    mo_data_before = mo_data_before["hits"]["hits"][0]["_source"]

    assert mo_data_before.get("point_a_id") == corresp_mo["id"]
    assert mo_data_before.get("point_a_name") != updated_corr_name

    corresp_mo["name"] = updated_corr_name
    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[corresp_mo], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    mo_data_after = await async_elastic_session.search(
        index=MO_INDEX_NAME, query=search_query, size=1
    )

    mo_data_after = mo_data_after["hits"]["hits"][0]["_source"]

    assert mo_data_after.get("point_a_id") == corresp_mo["id"]
    assert mo_data_after.get("point_a_name") == updated_corr_name


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_mo_case_14(async_elastic_session):
    """TEST On receiving MO:updated msg - if mo exists - updates record,
    if name was changed and MO.id exists in point_b_id of other objects - changes point_b_id for this objects"""

    await add_tmo_data_and_create_tmo_index(
        DEFAULT_MO_DATA["tmo_id"], async_elastic_session
    )
    updated_corr_name = "Updated corresp name"
    corresp_mo = dict()
    corresp_mo.update(UPDATED_MO_DATA)
    corresp_mo["id"] += 1
    corresp_mo["name"] = "Unique mo name"

    new_index = get_index_name_by_tmo(tmo_id=int(corresp_mo["tmo_id"]))

    await async_elastic_session.index(
        index=new_index,
        id=corresp_mo["id"],
        document=corresp_mo,
        refresh="true",
    )

    point_b_mo = dict()
    point_b_mo.update(corresp_mo)
    point_b_mo["id"] += 1
    point_b_mo["name"] = "A name"
    point_b_mo["point_b_id"] = corresp_mo["id"]

    await async_elastic_session.index(
        index=new_index,
        id=point_b_mo["id"],
        document=point_b_mo,
        refresh="true",
    )

    # before
    search_query = {"match": {"id": point_b_mo["id"]}}
    mo_data_before = await async_elastic_session.search(
        index=MO_INDEX_NAME, query=search_query, size=1
    )

    mo_data_before = mo_data_before["hits"]["hits"][0]["_source"]

    assert mo_data_before.get("point_b_id") == corresp_mo["id"]
    assert mo_data_before.get("point_b_name") != updated_corr_name

    corresp_mo["name"] = updated_corr_name
    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[corresp_mo], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    mo_data_after = await async_elastic_session.search(
        index=MO_INDEX_NAME, query=search_query, size=1
    )

    mo_data_after = mo_data_after["hits"]["hits"][0]["_source"]

    assert mo_data_after.get("point_b_id") == corresp_mo["id"]
    assert mo_data_after.get("point_b_name") == updated_corr_name
