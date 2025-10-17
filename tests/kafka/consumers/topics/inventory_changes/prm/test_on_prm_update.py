import datetime
import pickle

import pytest
from dateutil.parser import parse
from google.protobuf.timestamp_pb2 import Timestamp
from elastic.config import INVENTORY_OBJ_INDEX_PREFIX, INVENTORY_TPRM_INDEX_V2
from elastic.enum_models import InventoryFieldValType
from indexes_mapping.inventory.mapping import INVENTORY_PARAMETERS_FIELD_NAME
from services.inventory_services.kafka.consumers.inventory_changes.utils import (
    InventoryChangesHandler,
)
from tests.kafka.consumers.topics.inventory_changes.utils import (
    create_cleared_kafka_prm_msg,
    create_all_default_data_inventory_changes_topic,
)

datetime_value = datetime.datetime.now()
proto_timestamp = Timestamp()
proto_timestamp.FromDatetime(datetime_value)

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
    "updated_by": "admin",
    "modified_by": "None",
    "creation_date": "2000-12-12",
    "modification_date": "2000-12-12",
    "primary": None,
    "points_constraint_by_tmo": None,
}

DEFAULT_TPRM_DATA_KAFKA = {
    "id": 2,
    "name": "DEFAULT TPRM",
    "val_type": "bool",
    "required": True,
    "returnable": True,
    "tmo_id": DEFAULT_TMO_DATA["id"],
    "creation_date": proto_timestamp,
    "modification_date": proto_timestamp,
    "description": "",
    "multiple": False,
    "constraint": "",
    "prm_link_filter": "",
    "group": "",
    "modified_by": "",
    "version": 25,
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

DEFAULT_PRM_DATA = {
    "id": 4,
    "value": "True",
    "tprm_id": DEFAULT_TPRM_DATA_KAFKA["id"],
    "mo_id": DEFAULT_MO_DATA["id"],
    "version": 0,
}

MSG_CLASS_NAME = "PRM"
MSG_EVENT = "updated"
MSG_AS_DICT = {"objects": [DEFAULT_PRM_DATA]}


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_prm_case_2(async_elastic_session):
    """TEST On receiving PRM:updated msg - if tprm does not exist:
    - does not raise error,
    - does not update record"""

    search_query = {"match": {"id": DEFAULT_TPRM_DATA_KAFKA["id"]}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TPRM_INDEX_V2, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 0

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[DEFAULT_PRM_DATA], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    try:
        await handler.process_the_message()
    except Exception:
        assert False

    # record with matched data does not exist
    all_mo_indexes = f"{INVENTORY_OBJ_INDEX_PREFIX}*"
    search_query = {
        "exists": {
            "field": f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_TPRM_DATA_KAFKA['id']}"
        }
    }
    search_res = await async_elastic_session.search(
        index=all_mo_indexes, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_prm_case_3(async_elastic_session):
    """TEST On receiving PRM:updated msg - if tprm exists but mo does not exist:
    - does not raise error,
    - does not update record"""

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[DEFAULT_TMO_DATA],
        list_of_tprm_data_kafka_format=[DEFAULT_TPRM_DATA_KAFKA],
    )

    search_query = {"match": {"id": DEFAULT_TPRM_DATA_KAFKA["id"]}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TPRM_INDEX_V2, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[DEFAULT_PRM_DATA], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # record with matched data does not exist
    all_mo_indexes = f"{INVENTORY_OBJ_INDEX_PREFIX}*"
    search_query = {
        "exists": {
            "field": f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_TPRM_DATA_KAFKA['id']}"
        }
    }
    search_res = await async_elastic_session.search(
        index=all_mo_indexes, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_prm_case_4(async_elastic_session):
    """TEST On receiving PRM:updated msg - if tprm and mo exist:
    - updates record,
    - new value available in search"""
    default_prm_data = dict()
    default_prm_data.update(DEFAULT_PRM_DATA)
    old_value = "True"
    default_prm_data["value"] = old_value

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[DEFAULT_TMO_DATA],
        list_of_tprm_data_kafka_format=[DEFAULT_TPRM_DATA_KAFKA],
        list_of_mo_data_kafka_format=[DEFAULT_MO_DATA],
        list_of_prm_data_kafka_format=[default_prm_data],
    )
    # before

    new_value = False
    default_prm_data["value"] = str(new_value)

    all_mo_indexes = f"{INVENTORY_OBJ_INDEX_PREFIX}*"
    search_query = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_TPRM_DATA_KAFKA['id']}": new_value
        }
    }
    search_res = await async_elastic_session.search(
        index=all_mo_indexes, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 0

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[default_prm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # record with matched data exists

    search_query = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_TPRM_DATA_KAFKA['id']}": new_value
        }
    }
    search_res = await async_elastic_session.search(
        index=all_mo_indexes, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_prm_case_5(async_elastic_session):
    """TEST On receiving PRM:updated msg - if msg contains two ot more prm with same mo_id:
    - updates record,
    - all new prms available in search"""

    first_tprm = DEFAULT_TPRM_DATA_KAFKA
    second_tprm = dict()
    second_tprm.update(DEFAULT_TPRM_DATA_KAFKA)
    second_tprm["id"] += 1

    first_prm = dict()
    first_prm.update(DEFAULT_PRM_DATA)
    first_prm["tprm_id"] = first_tprm["id"]
    first_prm_old_value = "True"
    first_prm["value"] = first_prm_old_value

    second_prm = dict()
    second_prm.update(DEFAULT_PRM_DATA)
    second_prm["tprm_id"] = second_tprm["id"]
    second_prm_old_value = "True"
    second_prm["value"] = second_prm_old_value
    second_prm["id"] += 1

    # create tmo first tprm and default mo data
    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[DEFAULT_TMO_DATA],
        list_of_tprm_data_kafka_format=[first_tprm],
        list_of_mo_data_kafka_format=[DEFAULT_MO_DATA],
        list_of_prm_data_kafka_format=[first_prm],
    )

    # create second tprm with second prm
    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tprm_data_kafka_format=[second_tprm],
        list_of_prm_data_kafka_format=[second_prm],
    )

    # before

    new_value = False

    all_mo_indexes = f"{INVENTORY_OBJ_INDEX_PREFIX}*"
    search_query_1_prm = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{first_tprm['id']}": new_value
        }
    }
    search_query_2_prm = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{second_tprm['id']}": new_value
        }
    }
    search_query = {"bool": {"must": [search_query_1_prm, search_query_2_prm]}}
    search_res = await async_elastic_session.search(
        index=all_mo_indexes, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 0

    first_prm["value"] = str(new_value)
    second_prm["value"] = str(new_value)

    assert first_prm["value"] != first_prm_old_value
    assert second_prm["value"] != second_prm_old_value

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[first_prm, second_prm], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # record with matched data exists
    all_mo_indexes = f"{INVENTORY_OBJ_INDEX_PREFIX}*"

    search_query_1_prm = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{first_tprm['id']}": new_value
        }
    }
    search_query_2_prm = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{second_tprm['id']}": new_value
        }
    }
    search_query = {"bool": {"must": [search_query_1_prm, search_query_2_prm]}}
    search_res = await async_elastic_session.search(
        index=all_mo_indexes, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_prm_case_6(async_elastic_session):
    """TEST On receiving PRM:updated msg - if tprm and mo exist:
    - updates record with correct data for inventory val_type InventoryFieldValType.STR.value and multiple is False,
    - new value available in search"""
    new_tprm = dict()
    new_tprm.update(DEFAULT_TPRM_DATA_KAFKA)
    new_tprm["val_type"] = InventoryFieldValType.STR.value
    new_tprm["multiple"] = False

    old_value = "some old value"
    prm_data = dict()
    prm_data.update(DEFAULT_PRM_DATA)
    prm_data["value"] = old_value

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[DEFAULT_TMO_DATA],
        list_of_tprm_data_kafka_format=[new_tprm],
        list_of_mo_data_kafka_format=[DEFAULT_MO_DATA],
        list_of_prm_data_kafka_format=[prm_data],
    )

    new_value = "some new value"
    prm_data["value"] = new_value

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # record with matched data exists
    all_mo_indexes = f"{INVENTORY_OBJ_INDEX_PREFIX}*"
    search_query = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_TPRM_DATA_KAFKA['id']}": new_value
        }
    }
    search_res = await async_elastic_session.search(
        index=all_mo_indexes, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_prm_case_7(async_elastic_session):
    """TEST On receiving PRM:updated msg - if tprm and mo exist:
    - updates record with correct data for inventory val_type InventoryFieldValType.DATE.value and multiple is False,
    - new value available in search"""

    new_tprm = dict()
    new_tprm.update(DEFAULT_TPRM_DATA_KAFKA)
    new_tprm["val_type"] = InventoryFieldValType.DATE.value
    new_tprm["multiple"] = False

    old_value = "2000-11-10"
    prm_data = dict()
    prm_data.update(DEFAULT_PRM_DATA)
    prm_data["value"] = old_value
    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[DEFAULT_TMO_DATA],
        list_of_tprm_data_kafka_format=[new_tprm],
        list_of_mo_data_kafka_format=[DEFAULT_MO_DATA],
        list_of_prm_data_kafka_format=[prm_data],
    )

    new_value = "2000-10-10"
    prm_data["value"] = new_value

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # record with matched data exists
    all_mo_indexes = f"{INVENTORY_OBJ_INDEX_PREFIX}*"
    search_query = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_TPRM_DATA_KAFKA['id']}": new_value
        }
    }
    search_res = await async_elastic_session.search(
        index=all_mo_indexes, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_prm_case_8(async_elastic_session):
    """TEST On receiving PRM:updated msg - if tprm and mo exist:
    - updates record with correct data for inventory val_type InventoryFieldValType.DATETIME.value
    and multiple is False,
    - new value available in search"""
    new_tprm = dict()
    new_tprm.update(DEFAULT_TPRM_DATA_KAFKA)
    new_tprm["val_type"] = InventoryFieldValType.DATETIME.value
    new_tprm["multiple"] = False

    old_value = "2020-01-25T12:35:14"
    prm_data = dict()
    prm_data.update(DEFAULT_PRM_DATA)
    prm_data["value"] = old_value

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[DEFAULT_TMO_DATA],
        list_of_tprm_data_kafka_format=[new_tprm],
        list_of_mo_data_kafka_format=[DEFAULT_MO_DATA],
        list_of_prm_data_kafka_format=[prm_data],
    )

    new_value = parse("2024-01-25T12:35:14").strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    prm_data["value"] = new_value

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # record with matched data exists
    all_mo_indexes = f"{INVENTORY_OBJ_INDEX_PREFIX}*"
    search_query = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_TPRM_DATA_KAFKA['id']}": new_value
        }
    }
    search_res = await async_elastic_session.search(
        index=all_mo_indexes, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_prm_case_9(async_elastic_session):
    """TEST On receiving PRM:updated msg - if tprm and mo exist:
    - updates record with correct data for inventory val_type InventoryFieldValType.FLOAT.value and multiple is False,
    - new value available in search"""
    new_tprm = dict()
    new_tprm.update(DEFAULT_TPRM_DATA_KAFKA)
    new_tprm["val_type"] = InventoryFieldValType.FLOAT.value
    new_tprm["multiple"] = False

    old_value = 25.58
    prm_data = dict()
    prm_data.update(DEFAULT_PRM_DATA)
    prm_data["value"] = str(old_value)

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[DEFAULT_TMO_DATA],
        list_of_tprm_data_kafka_format=[new_tprm],
        list_of_mo_data_kafka_format=[DEFAULT_MO_DATA],
        list_of_prm_data_kafka_format=[prm_data],
    )

    new_value = 40.52
    prm_data["value"] = str(new_value)

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # record with matched data exists
    all_mo_indexes = f"{INVENTORY_OBJ_INDEX_PREFIX}*"
    search_query = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_TPRM_DATA_KAFKA['id']}": new_value
        }
    }
    search_res = await async_elastic_session.search(
        index=all_mo_indexes, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_prm_case_10(async_elastic_session):
    """TEST On receiving PRM:updated msg - if tprm and mo exist:
    - updates record with correct data for inventory val_type InventoryFieldValType.INT.value and multiple is False,
    - new value available in search"""
    new_tprm = dict()
    new_tprm.update(DEFAULT_TPRM_DATA_KAFKA)
    new_tprm["val_type"] = InventoryFieldValType.INT.value
    new_tprm["multiple"] = False

    old_value = 25
    prm_data = dict()
    prm_data.update(DEFAULT_PRM_DATA)
    prm_data["value"] = str(old_value)

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[DEFAULT_TMO_DATA],
        list_of_tprm_data_kafka_format=[new_tprm],
        list_of_mo_data_kafka_format=[DEFAULT_MO_DATA],
        list_of_prm_data_kafka_format=[prm_data],
    )

    new_value = 40
    prm_data["value"] = str(new_value)

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # record with matched data exists
    all_mo_indexes = f"{INVENTORY_OBJ_INDEX_PREFIX}*"
    search_query = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_TPRM_DATA_KAFKA['id']}": new_value
        }
    }
    search_res = await async_elastic_session.search(
        index=all_mo_indexes, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_prm_case_11(async_elastic_session):
    """TEST On receiving PRM:updated msg - if tprm and mo exist:
    - updates record with correct data for inventory val_type InventoryFieldValType.BOOL.value and multiple is False,
    - new value available in search"""
    new_tprm = dict()
    new_tprm.update(DEFAULT_TPRM_DATA_KAFKA)
    new_tprm["val_type"] = InventoryFieldValType.BOOL.value
    new_tprm["multiple"] = False

    old_value = True
    prm_data = dict()
    prm_data.update(DEFAULT_PRM_DATA)
    prm_data["value"] = str(old_value)

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[DEFAULT_TMO_DATA],
        list_of_tprm_data_kafka_format=[new_tprm],
        list_of_mo_data_kafka_format=[DEFAULT_MO_DATA],
        list_of_prm_data_kafka_format=[prm_data],
    )

    new_value = False
    prm_data["value"] = str(new_value)

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # record with matched data exists
    all_mo_indexes = f"{INVENTORY_OBJ_INDEX_PREFIX}*"
    search_query = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_TPRM_DATA_KAFKA['id']}": new_value
        }
    }
    search_res = await async_elastic_session.search(
        index=all_mo_indexes, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_prm_case_12(async_elastic_session):
    """TEST On receiving PRM:updated msg - if tprm and mo exist:
    - updates record with correct data for inventory val_type InventoryFieldValType.USER_LINK.value
    and multiple is False,
    - new value available in search"""
    new_tprm = dict()
    new_tprm.update(DEFAULT_TPRM_DATA_KAFKA)
    new_tprm["val_type"] = InventoryFieldValType.USER_LINK.value
    new_tprm["multiple"] = False

    old_value = "admin"
    prm_data = dict()
    prm_data.update(DEFAULT_PRM_DATA)
    prm_data["value"] = str(old_value)

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[DEFAULT_TMO_DATA],
        list_of_tprm_data_kafka_format=[new_tprm],
        list_of_mo_data_kafka_format=[DEFAULT_MO_DATA],
        list_of_prm_data_kafka_format=[prm_data],
    )

    new_value = "new admin"
    prm_data["value"] = str(new_value)

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # record with matched data exists
    all_mo_indexes = f"{INVENTORY_OBJ_INDEX_PREFIX}*"
    search_query = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_TPRM_DATA_KAFKA['id']}": new_value
        }
    }
    search_res = await async_elastic_session.search(
        index=all_mo_indexes, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_prm_case_13(async_elastic_session):
    """TEST On receiving PRM:updated msg - if tprm and mo exist:
    - updates record with correct data for inventory val_type InventoryFieldValType.FORMULA.value and multiple is False,
    - new value available in search"""
    new_tprm = dict()
    new_tprm.update(DEFAULT_TPRM_DATA_KAFKA)
    new_tprm["val_type"] = InventoryFieldValType.FORMULA.value
    new_tprm["multiple"] = False

    old_value = "2514"
    prm_data = dict()
    prm_data.update(DEFAULT_PRM_DATA)
    prm_data["value"] = str(old_value)

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[DEFAULT_TMO_DATA],
        list_of_tprm_data_kafka_format=[new_tprm],
        list_of_mo_data_kafka_format=[DEFAULT_MO_DATA],
        list_of_prm_data_kafka_format=[prm_data],
    )

    new_value = "25"
    prm_data["value"] = str(new_value)

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # record with matched data exists
    all_mo_indexes = f"{INVENTORY_OBJ_INDEX_PREFIX}*"
    search_query = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_TPRM_DATA_KAFKA['id']}": new_value
        }
    }
    search_res = await async_elastic_session.search(
        index=all_mo_indexes, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_prm_case_14(async_elastic_session):
    """TEST On receiving PRM:updated msg - if tprm and mo exist:
    - updates record with correct data for inventory val_type InventoryFieldValType.STR.value and multiple is True,
    - new value available in search"""
    new_tprm = dict()
    new_tprm.update(DEFAULT_TPRM_DATA_KAFKA)
    new_tprm["val_type"] = InventoryFieldValType.STR.value
    new_tprm["multiple"] = True

    old_value = ["some_str_value", "some_str_value2"]
    prm_data = dict()
    prm_data.update(DEFAULT_PRM_DATA)
    prm_data["value"] = pickle.dumps(old_value).hex()

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[DEFAULT_TMO_DATA],
        list_of_tprm_data_kafka_format=[new_tprm],
        list_of_mo_data_kafka_format=[DEFAULT_MO_DATA],
        list_of_prm_data_kafka_format=[prm_data],
    )

    new_value = ["some_str_value_new", "some_2str_value_new"]
    prm_data["value"] = pickle.dumps(new_value).hex()

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # record with matched data exists
    all_mo_indexes = f"{INVENTORY_OBJ_INDEX_PREFIX}*"
    search_value_1 = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_TPRM_DATA_KAFKA['id']}": new_value[
                0
            ]
        }
    }
    search_value_2 = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_TPRM_DATA_KAFKA['id']}": new_value[
                1
            ]
        }
    }
    search_query = {"bool": {"must": [search_value_1, search_value_2]}}
    search_res = await async_elastic_session.search(
        index=all_mo_indexes, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_prm_case_15(async_elastic_session):
    """TEST On receiving PRM:updated msg - if tprm and mo exist:
    - updates record with correct data for inventory val_type InventoryFieldValType.DATE.value and multiple is True,
    - new value available in search"""
    new_tprm = dict()
    new_tprm.update(DEFAULT_TPRM_DATA_KAFKA)
    new_tprm["val_type"] = InventoryFieldValType.DATE.value
    new_tprm["multiple"] = True

    old_value = ["2000-10-10", "2000-11-10"]
    prm_data = dict()
    prm_data.update(DEFAULT_PRM_DATA)
    prm_data["value"] = pickle.dumps(old_value).hex()

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[DEFAULT_TMO_DATA],
        list_of_tprm_data_kafka_format=[new_tprm],
        list_of_mo_data_kafka_format=[DEFAULT_MO_DATA],
        list_of_prm_data_kafka_format=[prm_data],
    )

    new_value = ["2000-10-11", "2000-11-11"]
    prm_data["value"] = pickle.dumps(new_value).hex()

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # record with matched data exists
    all_mo_indexes = f"{INVENTORY_OBJ_INDEX_PREFIX}*"
    search_value_1 = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_TPRM_DATA_KAFKA['id']}": new_value[
                0
            ]
        }
    }
    search_value_2 = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_TPRM_DATA_KAFKA['id']}": new_value[
                1
            ]
        }
    }
    search_query = {"bool": {"must": [search_value_1, search_value_2]}}
    search_res = await async_elastic_session.search(
        index=all_mo_indexes, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_prm_case_16(async_elastic_session):
    """TEST On receiving PRM:updated msg - if tprm and mo exist:
    - updates record with correct data for inventory val_type InventoryFieldValType.DATETIME.value
    and multiple is True,
    - new value available in search"""
    new_tprm = dict()
    new_tprm.update(DEFAULT_TPRM_DATA_KAFKA)
    new_tprm["val_type"] = InventoryFieldValType.DATETIME.value
    new_tprm["multiple"] = True

    parse("2024-01-25T12:35:14").strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    old_value = [
        str(parse("2024-01-25T12:35:14")),
        str(parse("2020-01-25T12:35:14")),
    ]
    prm_data = dict()
    prm_data.update(DEFAULT_PRM_DATA)
    prm_data["value"] = pickle.dumps(old_value).hex()

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[DEFAULT_TMO_DATA],
        list_of_tprm_data_kafka_format=[new_tprm],
        list_of_mo_data_kafka_format=[DEFAULT_MO_DATA],
        list_of_prm_data_kafka_format=[prm_data],
    )

    new_value = [
        parse("2018-01-25T12:35:14").strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        parse("2017-01-25T12:35:14").strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    ]
    prm_data["value"] = pickle.dumps(new_value).hex()

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # record with matched data exists
    all_mo_indexes = f"{INVENTORY_OBJ_INDEX_PREFIX}*"
    search_value_1 = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_TPRM_DATA_KAFKA['id']}": new_value[
                0
            ]
        }
    }
    search_value_2 = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_TPRM_DATA_KAFKA['id']}": new_value[
                1
            ]
        }
    }
    search_query = {"bool": {"must": [search_value_1, search_value_2]}}
    search_res = await async_elastic_session.search(
        index=all_mo_indexes, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_prm_case_17(async_elastic_session):
    """TEST On receiving PRM:updated msg - if tprm and mo exist:
    - updates record with correct data for inventory val_type InventoryFieldValType.FLOAT.value and multiple is True,
    - new value available in search"""
    new_tprm = dict()
    new_tprm.update(DEFAULT_TPRM_DATA_KAFKA)
    new_tprm["val_type"] = InventoryFieldValType.FLOAT.value
    new_tprm["multiple"] = True

    old_value = [40.52, 25.025]
    prm_data = dict()
    prm_data.update(DEFAULT_PRM_DATA)
    prm_data["value"] = pickle.dumps(old_value).hex()

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[DEFAULT_TMO_DATA],
        list_of_tprm_data_kafka_format=[new_tprm],
        list_of_mo_data_kafka_format=[DEFAULT_MO_DATA],
        list_of_prm_data_kafka_format=[prm_data],
    )

    new_value = [41.52, 21.025]
    prm_data["value"] = pickle.dumps(new_value).hex()

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # record with matched data exists
    all_mo_indexes = f"{INVENTORY_OBJ_INDEX_PREFIX}*"
    search_value_1 = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_TPRM_DATA_KAFKA['id']}": new_value[
                0
            ]
        }
    }
    search_value_2 = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_TPRM_DATA_KAFKA['id']}": new_value[
                1
            ]
        }
    }
    search_query = {"bool": {"must": [search_value_1, search_value_2]}}
    search_res = await async_elastic_session.search(
        index=all_mo_indexes, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_prm_case_18(async_elastic_session):
    """TEST On receiving PRM:updated msg - if tprm and mo exist:
    - updates record with correct data for inventory val_type InventoryFieldValType.INT.value and multiple is True,
    - new value available in search"""
    new_tprm = dict()
    new_tprm.update(DEFAULT_TPRM_DATA_KAFKA)
    new_tprm["val_type"] = InventoryFieldValType.INT.value
    new_tprm["multiple"] = True

    old_value = [40, 25]
    prm_data = dict()
    prm_data.update(DEFAULT_PRM_DATA)
    prm_data["value"] = pickle.dumps(old_value).hex()

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[DEFAULT_TMO_DATA],
        list_of_tprm_data_kafka_format=[new_tprm],
        list_of_mo_data_kafka_format=[DEFAULT_MO_DATA],
        list_of_prm_data_kafka_format=[prm_data],
    )

    new_value = [10, 2]
    prm_data["value"] = pickle.dumps(new_value).hex()

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # record with matched data exists
    all_mo_indexes = f"{INVENTORY_OBJ_INDEX_PREFIX}*"
    search_value_1 = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_TPRM_DATA_KAFKA['id']}": new_value[
                0
            ]
        }
    }
    search_value_2 = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_TPRM_DATA_KAFKA['id']}": new_value[
                1
            ]
        }
    }
    search_query = {"bool": {"must": [search_value_1, search_value_2]}}
    search_res = await async_elastic_session.search(
        index=all_mo_indexes, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_prm_case_19(async_elastic_session):
    """TEST On receiving PRM:updated msg - if tprm and mo exist:
    - updates record with correct data for inventory val_type InventoryFieldValType.BOOL.value and multiple is True,
    - new value available in search"""
    new_tprm = dict()
    new_tprm.update(DEFAULT_TPRM_DATA_KAFKA)
    new_tprm["val_type"] = InventoryFieldValType.BOOL.value
    new_tprm["multiple"] = True

    old_value = [False, False]
    prm_data = dict()
    prm_data.update(DEFAULT_PRM_DATA)
    prm_data["value"] = pickle.dumps(old_value).hex()

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[DEFAULT_TMO_DATA],
        list_of_tprm_data_kafka_format=[new_tprm],
        list_of_mo_data_kafka_format=[DEFAULT_MO_DATA],
        list_of_prm_data_kafka_format=[prm_data],
    )

    new_value = [True, True]
    prm_data["value"] = pickle.dumps(new_value).hex()

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # record with matched data exists
    all_mo_indexes = f"{INVENTORY_OBJ_INDEX_PREFIX}*"
    search_value_1 = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_TPRM_DATA_KAFKA['id']}": new_value[
                0
            ]
        }
    }
    search_value_2 = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_TPRM_DATA_KAFKA['id']}": new_value[
                1
            ]
        }
    }
    search_query = {"bool": {"must": [search_value_1, search_value_2]}}
    search_res = await async_elastic_session.search(
        index=all_mo_indexes, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_prm_case_20(async_elastic_session):
    """TEST On receiving PRM:updated msg - if tprm and mo exist:
    - updates record with correct data for inventory val_type InventoryFieldValType.USER_LINK.value
    and multiple is True,
    - new value available in search"""
    new_tprm = dict()
    new_tprm.update(DEFAULT_TPRM_DATA_KAFKA)
    new_tprm["val_type"] = InventoryFieldValType.USER_LINK.value
    new_tprm["multiple"] = True

    old_value = ["admin1", "admin2"]
    prm_data = dict()
    prm_data.update(DEFAULT_PRM_DATA)
    prm_data["value"] = pickle.dumps(old_value).hex()

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[DEFAULT_TMO_DATA],
        list_of_tprm_data_kafka_format=[new_tprm],
        list_of_mo_data_kafka_format=[DEFAULT_MO_DATA],
        list_of_prm_data_kafka_format=[prm_data],
    )

    new_value = ["admin3", "admin4"]
    prm_data["value"] = pickle.dumps(new_value).hex()

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # record with matched data exists
    all_mo_indexes = f"{INVENTORY_OBJ_INDEX_PREFIX}*"
    search_value_1 = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_TPRM_DATA_KAFKA['id']}": new_value[
                0
            ]
        }
    }
    search_value_2 = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_TPRM_DATA_KAFKA['id']}": new_value[
                1
            ]
        }
    }
    search_query = {"bool": {"must": [search_value_1, search_value_2]}}
    search_res = await async_elastic_session.search(
        index=all_mo_indexes, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_prm_case_21(async_elastic_session):
    """TEST On receiving PRM:updated msg - if tprm and mo exist:
    - updates record with correct data for inventory val_type InventoryFieldValType.FORMULA.value and multiple is True,
    - new value available in search"""
    new_tprm = dict()
    new_tprm.update(DEFAULT_TPRM_DATA_KAFKA)
    new_tprm["val_type"] = InventoryFieldValType.FORMULA.value
    new_tprm["multiple"] = True

    old_value = ["25", "35"]
    prm_data = dict()
    prm_data.update(DEFAULT_PRM_DATA)
    prm_data["value"] = pickle.dumps(old_value).hex()

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[DEFAULT_TMO_DATA],
        list_of_tprm_data_kafka_format=[new_tprm],
        list_of_mo_data_kafka_format=[DEFAULT_MO_DATA],
        list_of_prm_data_kafka_format=[prm_data],
    )

    new_value = ["45", "55"]
    prm_data["value"] = pickle.dumps(new_value).hex()

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # record with matched data exists
    all_mo_indexes = f"{INVENTORY_OBJ_INDEX_PREFIX}*"
    search_value_1 = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_TPRM_DATA_KAFKA['id']}": new_value[
                0
            ]
        }
    }
    search_value_2 = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_TPRM_DATA_KAFKA['id']}": new_value[
                1
            ]
        }
    }
    search_query = {"bool": {"must": [search_value_1, search_value_2]}}
    search_res = await async_elastic_session.search(
        index=all_mo_indexes, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1
