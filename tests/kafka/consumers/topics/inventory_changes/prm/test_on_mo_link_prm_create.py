import datetime
import pickle

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from elastic.config import (
    INVENTORY_PRM_INDEX,
    INVENTORY_MO_LINK_INDEX,
    ALL_MO_OBJ_INDEXES_PATTERN,
)
from elastic.enum_models import InventoryFieldValType
from indexes_mapping.inventory.mapping import INVENTORY_PARAMETERS_FIELD_NAME
from services.inventory_services.kafka.consumers.inventory_changes.utils import (
    InventoryChangesHandler,
)
from tests.kafka.consumers.topics.inventory_changes.utils import (
    create_all_default_data_inventory_changes_topic,
    create_cleared_kafka_prm_msg,
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
    "created_by": "admin",
    "modified_by": "None",
    "creation_date": "2000-12-12",
    "modification_date": "2000-12-12",
    "primary": None,
    "points_constraint_by_tmo": None,
}

DEFAULT_TPRM_DATA_KAFKA = {
    "id": 2,
    "name": "DEFAULT TPRM",
    "val_type": InventoryFieldValType.MO_LINK.value,
    "required": False,
    "returnable": True,
    "tmo_id": DEFAULT_TMO_DATA["id"],
    "creation_date": proto_timestamp,
    "modification_date": proto_timestamp,
    "description": "",
    "multiple": False,
    "constraint": "",
    "prm_link_filter": "",
    "group": "",
    "created_by": "",
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
    "value": f"{DEFAULT_MO_DATA['id']}",
    "tprm_id": DEFAULT_TPRM_DATA_KAFKA["id"],
    "mo_id": DEFAULT_MO_DATA["id"],
    "version": 0,
}

MSG_CLASS_NAME = "PRM"
MSG_EVENT = "created"
MSG_AS_DICT = {"objects": [DEFAULT_PRM_DATA]}


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_2(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists:
    - creates record in INVENTORY_PRM_INDEX
    - value is instance of str
    - value equals new prm value"""

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[DEFAULT_TMO_DATA],
        list_of_tprm_data_kafka_format=[DEFAULT_TPRM_DATA_KAFKA],
        list_of_mo_data_kafka_format=[DEFAULT_MO_DATA],
    )

    search_query = {"match": {"id": DEFAULT_PRM_DATA["id"]}}

    res_before = await async_elastic_session.search(
        index=INVENTORY_PRM_INDEX,
        query=search_query,
        size=1,
        track_total_hits=True,
    )

    assert res_before["hits"]["total"]["value"] == 0

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[DEFAULT_PRM_DATA], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    try:
        await handler.process_the_message()
    except Exception:
        assert False

    res_after = await async_elastic_session.search(
        index=INVENTORY_PRM_INDEX,
        query=search_query,
        size=1,
        track_total_hits=True,
    )

    assert res_after["hits"]["total"]["value"] == 1
    value = res_after["hits"]["hits"][0]["_source"]["value"]
    assert isinstance(value, str)
    assert value == DEFAULT_PRM_DATA["value"]


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_3(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists
    and TPRM.multiple = False:
    - creates record in INVENTORY_MO_LINK_INDEX
    - value is instance of int
    - value equals new prm value"""

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[DEFAULT_TMO_DATA],
        list_of_tprm_data_kafka_format=[DEFAULT_TPRM_DATA_KAFKA],
        list_of_mo_data_kafka_format=[DEFAULT_MO_DATA],
    )

    search_query = {"match": {"id": DEFAULT_PRM_DATA["id"]}}

    res_before = await async_elastic_session.search(
        index=INVENTORY_MO_LINK_INDEX,
        query=search_query,
        size=1,
        track_total_hits=True,
    )

    assert res_before["hits"]["total"]["value"] == 0

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[DEFAULT_PRM_DATA], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    res_after = await async_elastic_session.search(
        index=INVENTORY_MO_LINK_INDEX,
        query=search_query,
        size=1,
        track_total_hits=True,
    )

    assert res_after["hits"]["total"]["value"] == 1
    value = res_after["hits"]["hits"][0]["_source"]["value"]
    assert isinstance(value, int)
    assert value == int(DEFAULT_PRM_DATA["value"])


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_4(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists
    and TPRM.multiple = False:
    - updates mo data in  record in INVENTORY_OBJ_INDEX_PREFIX
    - value is instance of str
    - value equals mo.name in mo_link
    - new value available in search"""
    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[DEFAULT_TMO_DATA],
        list_of_tprm_data_kafka_format=[DEFAULT_TPRM_DATA_KAFKA],
        list_of_mo_data_kafka_format=[DEFAULT_MO_DATA],
    )

    search_query = {"match": {"id": DEFAULT_MO_DATA["id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )
    tprm_id_as_str = str(DEFAULT_PRM_DATA["tprm_id"])
    assert res_before["hits"]["total"]["value"] == 1
    value = res_before["hits"]["hits"][0]["_source"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ].get(tprm_id_as_str)
    assert value is None

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[DEFAULT_PRM_DATA], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    res_after = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )

    assert res_after["hits"]["total"]["value"] == 1
    value = res_after["hits"]["hits"][0]["_source"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ].get(tprm_id_as_str)
    assert value is not None

    search_query = {"match": {"id": DEFAULT_PRM_DATA["value"]}}
    res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )
    mo_name = res["hits"]["hits"][0]["_source"]["name"]
    assert value == mo_name
    # new value is in index
    search_query = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_PRM_DATA['tprm_id']}": mo_name
        }
    }

    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )
    assert search_res["hits"]["total"]["value"] == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_5(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists
    and TPRM.multiple = True:
    - creates record in INVENTORY_PRM_INDEX
    - value is instance of str
    - value equals new prm value"""
    tprm_data = dict()
    tprm_data.update(DEFAULT_TPRM_DATA_KAFKA)
    tprm_data["multiple"] = True

    prm_data = dict()
    prm_data.update(DEFAULT_PRM_DATA)
    prm_data["value"] = str(pickle.dumps([DEFAULT_MO_DATA["id"]]).hex())

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[DEFAULT_TMO_DATA],
        list_of_tprm_data_kafka_format=[tprm_data],
        list_of_mo_data_kafka_format=[DEFAULT_MO_DATA],
    )

    search_query = {"match": {"id": DEFAULT_PRM_DATA["id"]}}

    res_before = await async_elastic_session.search(
        index=INVENTORY_PRM_INDEX,
        query=search_query,
        size=1,
        track_total_hits=True,
    )

    assert res_before["hits"]["total"]["value"] == 0

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    res_after = await async_elastic_session.search(
        index=INVENTORY_PRM_INDEX,
        query=search_query,
        size=1,
        track_total_hits=True,
    )

    assert res_after["hits"]["total"]["value"] == 1
    value = res_after["hits"]["hits"][0]["_source"]["value"]
    assert isinstance(value, str)
    assert value == prm_data["value"]


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_6(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists
    and TPRM.multiple = True:
    - creates record in INVENTORY_MO_LINK_INDEX
    - value is instance of list (list of int)
    - value equals new prm value"""
    tprm_data = dict()
    tprm_data.update(DEFAULT_TPRM_DATA_KAFKA)
    tprm_data["multiple"] = True

    prm_data = dict()
    prm_data.update(DEFAULT_PRM_DATA)
    prm_data_not_converted = [DEFAULT_MO_DATA["id"], DEFAULT_MO_DATA["id"]]
    prm_data["value"] = pickle.dumps(prm_data_not_converted).hex()

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[DEFAULT_TMO_DATA],
        list_of_tprm_data_kafka_format=[tprm_data],
        list_of_mo_data_kafka_format=[DEFAULT_MO_DATA],
    )

    search_query = {"match": {"id": DEFAULT_PRM_DATA["id"]}}

    res_before = await async_elastic_session.search(
        index=INVENTORY_MO_LINK_INDEX,
        query=search_query,
        size=1,
        track_total_hits=True,
    )

    assert res_before["hits"]["total"]["value"] == 0

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    res_after = await async_elastic_session.search(
        index=INVENTORY_MO_LINK_INDEX,
        query=search_query,
        size=1,
        track_total_hits=True,
    )

    assert res_after["hits"]["total"]["value"] == 1
    value = res_after["hits"]["hits"][0]["_source"]["value"]
    assert isinstance(value, list)
    for item in value:
        assert isinstance(item, int)
    assert value == prm_data_not_converted


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_7(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists
    and TPRM.multiple = True:
    - updates mo data in  record in INVENTORY_OBJ_INDEX_PREFIX
    - value is instance of str
    - value equals mo.name in mo_link
    - new value available in search"""
    tprm_data = dict()
    tprm_data.update(DEFAULT_TPRM_DATA_KAFKA)
    tprm_data["multiple"] = True

    prm_data = dict()
    prm_data.update(DEFAULT_PRM_DATA)
    mo_link_ids = [DEFAULT_MO_DATA["id"], DEFAULT_MO_DATA["id"]]
    prm_data["value"] = str(pickle.dumps(mo_link_ids).hex())
    tprm_id_as_str = str(DEFAULT_PRM_DATA["tprm_id"])

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[DEFAULT_TMO_DATA],
        list_of_tprm_data_kafka_format=[tprm_data],
        list_of_mo_data_kafka_format=[DEFAULT_MO_DATA],
    )

    search_query = {"match": {"id": DEFAULT_PRM_DATA["mo_id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )

    assert res_before["hits"]["total"]["value"] == 1
    value = res_before["hits"]["hits"][0]["_source"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ].get(tprm_id_as_str)
    assert value is None

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    res_after = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )

    assert res_after["hits"]["total"]["value"] == 1
    value = res_after["hits"]["hits"][0]["_source"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ].get(tprm_id_as_str)
    assert value is not None
    assert isinstance(value, list)
