import datetime
import pickle

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from elastic.config import (
    INVENTORY_PRM_INDEX,
    ALL_MO_OBJ_INDEXES_PATTERN,
    INVENTORY_PRM_LINK_INDEX,
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

PRM_LINK_TMO_DATA = {
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

CORRESP_TMO_DATA = {
    "id": 2,
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

CORRESP_TPRM_DATA_KAFKA = {
    "id": 3,
    "name": "DEFAULT TPRM",
    "val_type": InventoryFieldValType.INT.value,
    "required": False,
    "returnable": True,
    "tmo_id": CORRESP_TMO_DATA["id"],
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

PRM_LINK_TPRM_DATA_KAFKA = {
    "id": 4,
    "name": "DEFAULT TPRM",
    "val_type": InventoryFieldValType.PRM_LINK.value,
    "required": False,
    "returnable": True,
    "tmo_id": PRM_LINK_TMO_DATA["id"],
    "creation_date": proto_timestamp,
    "modification_date": proto_timestamp,
    "description": "",
    "multiple": False,
    "constraint": f"{CORRESP_TPRM_DATA_KAFKA['id']}",
    "prm_link_filter": "",
    "group": "",
    "created_by": "",
    "modified_by": "",
    "version": 25,
}

CORRESP_MO_DATA = {
    "id": 5,
    "name": "DEFAULT MO DATA",
    "active": True,
    "tmo_id": CORRESP_TMO_DATA["id"],
    "latitude": 0.0,
    "longitude": 0.0,
    "p_id": 0,
    "point_a_id": 0,
    "point_b_id": 0,
    "model": "",
    "version": 0,
    "status": "",
}

PRM_LINK_MO_DATA = {
    "id": 6,
    "name": "DEFAULT MO DATA",
    "active": True,
    "tmo_id": PRM_LINK_TMO_DATA["id"],
    "latitude": 0.0,
    "longitude": 0.0,
    "p_id": 0,
    "point_a_id": 0,
    "point_b_id": 0,
    "model": "",
    "version": 0,
    "status": "",
}

CORRESP_PRM_DATA = {
    "id": 7,
    "value": "25",
    "tprm_id": CORRESP_TPRM_DATA_KAFKA["id"],
    "mo_id": CORRESP_TMO_DATA["id"],
    "version": 0,
}

PRM_LINK_PRM_DATA = {
    "id": 8,
    "value": f"{CORRESP_PRM_DATA['id']}",
    "tprm_id": PRM_LINK_TPRM_DATA_KAFKA["id"],
    "mo_id": PRM_LINK_MO_DATA["id"],
    "version": 0,
}

MSG_CLASS_NAME = "PRM"
MSG_EVENT = "created"
MSG_AS_DICT = {"objects": [PRM_LINK_PRM_DATA]}


@pytest.mark.asyncio(loop_scope="session")
async def test_on_prm_link_prm_create_case_2(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of prm_link prm (prm id) exists:
    - creates record in INVENTORY_PRM_INDEX
    - value is instance of str
    - value equals new prm value"""

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[CORRESP_TMO_DATA, PRM_LINK_TMO_DATA],
        list_of_tprm_data_kafka_format=[
            CORRESP_TPRM_DATA_KAFKA,
            PRM_LINK_TPRM_DATA_KAFKA,
        ],
        list_of_mo_data_kafka_format=[CORRESP_MO_DATA, PRM_LINK_MO_DATA],
        list_of_prm_data_kafka_format=[CORRESP_PRM_DATA],
    )

    search_query = {"match": {"id": PRM_LINK_PRM_DATA["id"]}}

    res_before = await async_elastic_session.search(
        index=INVENTORY_PRM_INDEX,
        query=search_query,
        size=1,
        track_total_hits=True,
    )

    assert res_before["hits"]["total"]["value"] == 0

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[PRM_LINK_PRM_DATA], msg_event=MSG_EVENT
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
    assert value == PRM_LINK_PRM_DATA["value"]


@pytest.mark.asyncio(loop_scope="session")
async def test_on_prm_link_prm_create_case_3(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of prm_link prm (prm id) exists and TPRM.multiple = False:
    - creates record in INVENTORY_PRM_LINK_INDEX
    - value is instance of int
    - value equals new prm value"""
    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[CORRESP_TMO_DATA, PRM_LINK_TMO_DATA],
        list_of_tprm_data_kafka_format=[
            CORRESP_TPRM_DATA_KAFKA,
            PRM_LINK_TPRM_DATA_KAFKA,
        ],
        list_of_mo_data_kafka_format=[CORRESP_MO_DATA, PRM_LINK_MO_DATA],
        list_of_prm_data_kafka_format=[CORRESP_PRM_DATA],
    )

    search_query = {"match": {"id": PRM_LINK_PRM_DATA["id"]}}

    res_before = await async_elastic_session.search(
        index=INVENTORY_PRM_LINK_INDEX,
        query=search_query,
        size=1,
        track_total_hits=True,
    )

    assert res_before["hits"]["total"]["value"] == 0

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[PRM_LINK_PRM_DATA], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    res_after = await async_elastic_session.search(
        index=INVENTORY_PRM_LINK_INDEX,
        query=search_query,
        size=1,
        track_total_hits=True,
    )

    assert res_after["hits"]["total"]["value"] == 1
    value = res_after["hits"]["hits"][0]["_source"]["value"]
    assert isinstance(value, int)
    assert value == int(PRM_LINK_PRM_DATA["value"])


@pytest.mark.asyncio(loop_scope="session")
async def test_on_prm_link_prm_create_case_4(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of prm_link prm (prm id) exists and TPRM.multiple = True:
    - creates record in INVENTORY_PRM_LINK_INDEX
    - value is instance of list (list of int)
    - value equals new prm value"""

    prm_link_tprm_data = dict()
    prm_link_tprm_data.update(PRM_LINK_TPRM_DATA_KAFKA)
    prm_link_tprm_data["multiple"] = True

    prm_link_prm_data = dict()
    prm_link_prm_data.update(PRM_LINK_PRM_DATA)
    prm_link_prm_data["value"] = str(
        pickle.dumps([CORRESP_TPRM_DATA_KAFKA["id"]]).hex()
    )

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[CORRESP_TMO_DATA, PRM_LINK_TMO_DATA],
        list_of_tprm_data_kafka_format=[
            CORRESP_TPRM_DATA_KAFKA,
            prm_link_tprm_data,
        ],
        list_of_mo_data_kafka_format=[CORRESP_MO_DATA, PRM_LINK_MO_DATA],
        list_of_prm_data_kafka_format=[CORRESP_PRM_DATA],
    )

    search_query = {"match": {"id": PRM_LINK_PRM_DATA["id"]}}

    res_before = await async_elastic_session.search(
        index=INVENTORY_PRM_LINK_INDEX,
        query=search_query,
        size=1,
        track_total_hits=True,
    )

    assert res_before["hits"]["total"]["value"] == 0

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_link_prm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    res_after = await async_elastic_session.search(
        index=INVENTORY_PRM_LINK_INDEX,
        query=search_query,
        size=1,
        track_total_hits=True,
    )

    assert res_after["hits"]["total"]["value"] == 1
    value = res_after["hits"]["hits"][0]["_source"]["value"]

    assert isinstance(value, list)
    for v in value:
        assert isinstance(v, int)


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_5(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists
    and prm_link TPRM.multiple = False
    and corresponding tprm val_type is 'InventoryFieldValType.INT.value'
    and corresponding TPRM.multiple = False :
    - updates mo data in  record in INVENTORY_OBJ_INDEX_PREFIX
    - value is instance of InventoryFieldValType.INT.value
    - new value available in search"""
    corres_tprm_data = dict()
    corres_tprm_data.update(CORRESP_TPRM_DATA_KAFKA)
    corres_tprm_data["val_type"] = InventoryFieldValType.INT.value

    corres_prm_data = dict()
    corres_prm_data.update(CORRESP_PRM_DATA)
    corresponding_value = "25"
    corres_prm_data["value"] = corresponding_value

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[CORRESP_TMO_DATA, PRM_LINK_TMO_DATA],
        list_of_tprm_data_kafka_format=[
            corres_tprm_data,
            PRM_LINK_TPRM_DATA_KAFKA,
        ],
        list_of_mo_data_kafka_format=[CORRESP_MO_DATA, PRM_LINK_MO_DATA],
        list_of_prm_data_kafka_format=[corres_prm_data],
    )

    search_query = {"match": {"id": PRM_LINK_PRM_DATA["mo_id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )
    tprm_id_as_str = str(PRM_LINK_PRM_DATA["tprm_id"])
    assert res_before["hits"]["total"]["value"] == 1
    value = res_before["hits"]["hits"][0]["_source"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ].get(tprm_id_as_str)
    assert value is None

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[PRM_LINK_PRM_DATA], msg_event=MSG_EVENT
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
    assert value == int(corresponding_value)


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_6(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists
    and prm_link TPRM.multiple = False
    and corresponding tprm val_type is 'InventoryFieldValType.STR.value'
    and corresponding TPRM.multiple = False :
    - updates mo data in  record in INVENTORY_OBJ_INDEX_PREFIX
    - value is instance of InventoryFieldValType.STR.value
    - new value available in search"""
    corres_tprm_data = dict()
    corres_tprm_data.update(CORRESP_TPRM_DATA_KAFKA)
    corres_tprm_data["val_type"] = InventoryFieldValType.STR.value

    corres_prm_data = dict()
    corres_prm_data.update(CORRESP_PRM_DATA)
    corresponding_value = "25"
    corres_prm_data["value"] = corresponding_value

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[CORRESP_TMO_DATA, PRM_LINK_TMO_DATA],
        list_of_tprm_data_kafka_format=[
            corres_tprm_data,
            PRM_LINK_TPRM_DATA_KAFKA,
        ],
        list_of_mo_data_kafka_format=[CORRESP_MO_DATA, PRM_LINK_MO_DATA],
        list_of_prm_data_kafka_format=[corres_prm_data],
    )

    search_query = {"match": {"id": PRM_LINK_PRM_DATA["mo_id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )
    tprm_id_as_str = str(PRM_LINK_PRM_DATA["tprm_id"])
    assert res_before["hits"]["total"]["value"] == 1
    value = res_before["hits"]["hits"][0]["_source"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ].get(tprm_id_as_str)
    assert value is None

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[PRM_LINK_PRM_DATA], msg_event=MSG_EVENT
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
    assert value == str(corresponding_value)


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_7(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists
    and prm_link TPRM.multiple = False
    and corresponding tprm val_type is 'InventoryFieldValType.FLOAT.value'
    and corresponding TPRM.multiple = False :
    - updates mo data in  record in INVENTORY_OBJ_INDEX_PREFIX
    - value is instance of InventoryFieldValType.FLOAT.value
    - new value available in search"""
    corres_tprm_data = dict()
    corres_tprm_data.update(CORRESP_TPRM_DATA_KAFKA)
    corres_tprm_data["val_type"] = InventoryFieldValType.FLOAT.value

    corres_prm_data = dict()
    corres_prm_data.update(CORRESP_PRM_DATA)
    corresponding_value = "25.25"
    corres_prm_data["value"] = corresponding_value

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[CORRESP_TMO_DATA, PRM_LINK_TMO_DATA],
        list_of_tprm_data_kafka_format=[
            corres_tprm_data,
            PRM_LINK_TPRM_DATA_KAFKA,
        ],
        list_of_mo_data_kafka_format=[CORRESP_MO_DATA, PRM_LINK_MO_DATA],
        list_of_prm_data_kafka_format=[corres_prm_data],
    )

    search_query = {"match": {"id": PRM_LINK_PRM_DATA["mo_id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )
    tprm_id_as_str = str(PRM_LINK_PRM_DATA["tprm_id"])
    assert res_before["hits"]["total"]["value"] == 1
    value = res_before["hits"]["hits"][0]["_source"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ].get(tprm_id_as_str)
    assert value is None

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[PRM_LINK_PRM_DATA], msg_event=MSG_EVENT
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
    assert value == float(corresponding_value)


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_8(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists
    and prm_link TPRM.multiple = False
    and corresponding tprm val_type is 'InventoryFieldValType.BOOL.value'
    and corresponding TPRM.multiple = False :
    - updates mo data in  record in INVENTORY_OBJ_INDEX_PREFIX
    - value is instance of InventoryFieldValType.BOOL.value
    - new value available in search"""
    corres_tprm_data = dict()
    corres_tprm_data.update(CORRESP_TPRM_DATA_KAFKA)
    corres_tprm_data["val_type"] = InventoryFieldValType.BOOL.value

    corres_prm_data = dict()
    corres_prm_data.update(CORRESP_PRM_DATA)
    corresponding_value = "true"
    corres_prm_data["value"] = corresponding_value

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[CORRESP_TMO_DATA, PRM_LINK_TMO_DATA],
        list_of_tprm_data_kafka_format=[
            corres_tprm_data,
            PRM_LINK_TPRM_DATA_KAFKA,
        ],
        list_of_mo_data_kafka_format=[CORRESP_MO_DATA, PRM_LINK_MO_DATA],
        list_of_prm_data_kafka_format=[corres_prm_data],
    )

    search_query = {"match": {"id": PRM_LINK_PRM_DATA["mo_id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )
    tprm_id_as_str = str(PRM_LINK_PRM_DATA["tprm_id"])
    assert res_before["hits"]["total"]["value"] == 1
    value = res_before["hits"]["hits"][0]["_source"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ].get(tprm_id_as_str)
    assert value is None

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[PRM_LINK_PRM_DATA], msg_event=MSG_EVENT
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
    assert value is True


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_9(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists
    and prm_link TPRM.multiple = False
    and corresponding tprm val_type is 'InventoryFieldValType.DATE.value'
    and corresponding TPRM.multiple = False :
    - updates mo data in  record in INVENTORY_OBJ_INDEX_PREFIX
    - value is instance of InventoryFieldValType.DATE.value
    - new value available in search"""
    corres_tprm_data = dict()
    corres_tprm_data.update(CORRESP_TPRM_DATA_KAFKA)
    corres_tprm_data["val_type"] = InventoryFieldValType.DATE.value

    corres_prm_data = dict()
    corres_prm_data.update(CORRESP_PRM_DATA)
    corresponding_value = "2000-01-01"
    corres_prm_data["value"] = corresponding_value

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[CORRESP_TMO_DATA, PRM_LINK_TMO_DATA],
        list_of_tprm_data_kafka_format=[
            corres_tprm_data,
            PRM_LINK_TPRM_DATA_KAFKA,
        ],
        list_of_mo_data_kafka_format=[CORRESP_MO_DATA, PRM_LINK_MO_DATA],
        list_of_prm_data_kafka_format=[corres_prm_data],
    )

    search_query = {"match": {"id": PRM_LINK_PRM_DATA["mo_id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )
    tprm_id_as_str = str(PRM_LINK_PRM_DATA["tprm_id"])
    assert res_before["hits"]["total"]["value"] == 1
    value = res_before["hits"]["hits"][0]["_source"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ].get(tprm_id_as_str)
    assert value is None

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[PRM_LINK_PRM_DATA], msg_event=MSG_EVENT
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
    assert value == corresponding_value


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_10(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists
    and prm_link TPRM.multiple = False
    and corresponding tprm val_type is 'InventoryFieldValType.DATETIME.value'
    and corresponding TPRM.multiple = False :
    - updates mo data in  record in INVENTORY_OBJ_INDEX_PREFIX
    - value is instance of InventoryFieldValType.DATETIME.value
    - new value available in search"""
    corres_tprm_data = dict()
    corres_tprm_data.update(CORRESP_TPRM_DATA_KAFKA)
    corres_tprm_data["val_type"] = InventoryFieldValType.DATETIME.value

    corres_prm_data = dict()
    corres_prm_data.update(CORRESP_PRM_DATA)
    corresponding_value = "2000-01-01"
    corres_prm_data["value"] = corresponding_value

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[CORRESP_TMO_DATA, PRM_LINK_TMO_DATA],
        list_of_tprm_data_kafka_format=[
            corres_tprm_data,
            PRM_LINK_TPRM_DATA_KAFKA,
        ],
        list_of_mo_data_kafka_format=[CORRESP_MO_DATA, PRM_LINK_MO_DATA],
        list_of_prm_data_kafka_format=[corres_prm_data],
    )

    search_query = {"match": {"id": PRM_LINK_PRM_DATA["mo_id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )
    tprm_id_as_str = str(PRM_LINK_PRM_DATA["tprm_id"])
    assert res_before["hits"]["total"]["value"] == 1
    value = res_before["hits"]["hits"][0]["_source"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ].get(tprm_id_as_str)
    assert value is None

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[PRM_LINK_PRM_DATA], msg_event=MSG_EVENT
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
    assert value == f"{corresponding_value}T00:00:00.000000Z"


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_11(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists
    and prm_link TPRM.multiple = False
    and corresponding tprm val_type is 'InventoryFieldValType.STR.value'
    and corresponding TPRM.multiple = True :
    - updates mo data in  record in INVENTORY_OBJ_INDEX_PREFIX
    - value is instance of InventoryFieldValType.STR.value
    - new value available in search"""
    corres_tprm_data = dict()
    corres_tprm_data.update(CORRESP_TPRM_DATA_KAFKA)
    corres_tprm_data["val_type"] = InventoryFieldValType.STR.value
    corres_tprm_data["multiple"] = True

    corres_prm_data = dict()
    corres_prm_data.update(CORRESP_PRM_DATA)
    corresponding_value = ["25"]
    corresponding_value_as_mult = str(pickle.dumps(corresponding_value).hex())
    corres_prm_data["value"] = corresponding_value_as_mult

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[CORRESP_TMO_DATA, PRM_LINK_TMO_DATA],
        list_of_tprm_data_kafka_format=[
            corres_tprm_data,
            PRM_LINK_TPRM_DATA_KAFKA,
        ],
        list_of_mo_data_kafka_format=[CORRESP_MO_DATA, PRM_LINK_MO_DATA],
        list_of_prm_data_kafka_format=[corres_prm_data],
    )

    search_query = {"match": {"id": PRM_LINK_PRM_DATA["mo_id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )
    tprm_id_as_str = str(PRM_LINK_PRM_DATA["tprm_id"])
    assert res_before["hits"]["total"]["value"] == 1
    value = res_before["hits"]["hits"][0]["_source"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ].get(tprm_id_as_str)
    assert value is None

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[PRM_LINK_PRM_DATA], msg_event=MSG_EVENT
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
    assert value == corresponding_value


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_12(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists
    and prm_link TPRM.multiple = False
    and corresponding tprm val_type is 'InventoryFieldValType.INT.value'
    and corresponding TPRM.multiple = True :
    - updates mo data in  record in INVENTORY_OBJ_INDEX_PREFIX
    - value is instance of InventoryFieldValType.INT.value
    - new value available in search"""
    corres_tprm_data = dict()
    corres_tprm_data.update(CORRESP_TPRM_DATA_KAFKA)
    corres_tprm_data["val_type"] = InventoryFieldValType.INT.value
    corres_tprm_data["multiple"] = True

    corres_prm_data = dict()
    corres_prm_data.update(CORRESP_PRM_DATA)

    corresponding_value = [25]
    corresponding_value_as_mult = str(pickle.dumps(corresponding_value).hex())
    corres_prm_data["value"] = corresponding_value_as_mult

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[CORRESP_TMO_DATA, PRM_LINK_TMO_DATA],
        list_of_tprm_data_kafka_format=[
            corres_tprm_data,
            PRM_LINK_TPRM_DATA_KAFKA,
        ],
        list_of_mo_data_kafka_format=[CORRESP_MO_DATA, PRM_LINK_MO_DATA],
        list_of_prm_data_kafka_format=[corres_prm_data],
    )

    search_query = {"match": {"id": PRM_LINK_PRM_DATA["mo_id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )
    tprm_id_as_str = str(PRM_LINK_PRM_DATA["tprm_id"])
    assert res_before["hits"]["total"]["value"] == 1
    value = res_before["hits"]["hits"][0]["_source"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ].get(tprm_id_as_str)
    assert value is None

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[PRM_LINK_PRM_DATA], msg_event=MSG_EVENT
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
    assert value == corresponding_value


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_13(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists
    and prm_link TPRM.multiple = False
    and corresponding tprm val_type is 'InventoryFieldValType.FLOAT.value'
    and corresponding TPRM.multiple = True :
    - updates mo data in  record in INVENTORY_OBJ_INDEX_PREFIX
    - value is instance of InventoryFieldValType.FLOAT.value
    - new value available in search"""
    corres_tprm_data = dict()
    corres_tprm_data.update(CORRESP_TPRM_DATA_KAFKA)
    corres_tprm_data["val_type"] = InventoryFieldValType.FLOAT.value
    corres_tprm_data["multiple"] = True

    corres_prm_data = dict()
    corres_prm_data.update(CORRESP_PRM_DATA)

    corresponding_value = [25.25]
    corresponding_value_as_mult = str(pickle.dumps(corresponding_value).hex())
    corres_prm_data["value"] = corresponding_value_as_mult

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[CORRESP_TMO_DATA, PRM_LINK_TMO_DATA],
        list_of_tprm_data_kafka_format=[
            corres_tprm_data,
            PRM_LINK_TPRM_DATA_KAFKA,
        ],
        list_of_mo_data_kafka_format=[CORRESP_MO_DATA, PRM_LINK_MO_DATA],
        list_of_prm_data_kafka_format=[corres_prm_data],
    )

    search_query = {"match": {"id": PRM_LINK_PRM_DATA["mo_id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )
    tprm_id_as_str = str(PRM_LINK_PRM_DATA["tprm_id"])
    assert res_before["hits"]["total"]["value"] == 1
    value = res_before["hits"]["hits"][0]["_source"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ].get(tprm_id_as_str)
    assert value is None

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[PRM_LINK_PRM_DATA], msg_event=MSG_EVENT
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
    assert value == corresponding_value


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_14(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists
    and prm_link TPRM.multiple = False
    and corresponding tprm val_type is 'InventoryFieldValType.BOOL.value'
    and corresponding TPRM.multiple = True :
    - updates mo data in  record in INVENTORY_OBJ_INDEX_PREFIX
    - value is instance of InventoryFieldValType.BOOL.value
    - new value available in search"""
    corres_tprm_data = dict()
    corres_tprm_data.update(CORRESP_TPRM_DATA_KAFKA)
    corres_tprm_data["val_type"] = InventoryFieldValType.BOOL.value
    corres_tprm_data["multiple"] = True

    corres_prm_data = dict()
    corres_prm_data.update(CORRESP_PRM_DATA)

    corresponding_value = [True]
    corresponding_value_as_mult = str(pickle.dumps(corresponding_value).hex())
    corres_prm_data["value"] = corresponding_value_as_mult

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[CORRESP_TMO_DATA, PRM_LINK_TMO_DATA],
        list_of_tprm_data_kafka_format=[
            corres_tprm_data,
            PRM_LINK_TPRM_DATA_KAFKA,
        ],
        list_of_mo_data_kafka_format=[CORRESP_MO_DATA, PRM_LINK_MO_DATA],
        list_of_prm_data_kafka_format=[corres_prm_data],
    )

    search_query = {"match": {"id": PRM_LINK_PRM_DATA["mo_id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )
    tprm_id_as_str = str(PRM_LINK_PRM_DATA["tprm_id"])
    assert res_before["hits"]["total"]["value"] == 1
    value = res_before["hits"]["hits"][0]["_source"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ].get(tprm_id_as_str)
    assert value is None

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[PRM_LINK_PRM_DATA], msg_event=MSG_EVENT
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
    assert value == corresponding_value


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_15(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists
    and prm_link TPRM.multiple = False
    and corresponding tprm val_type is 'InventoryFieldValType.DATE.value'
    and corresponding TPRM.multiple = True :
    - updates mo data in  record in INVENTORY_OBJ_INDEX_PREFIX
    - value is instance of InventoryFieldValType.DATE.value
    - new value available in search"""
    corres_tprm_data = dict()
    corres_tprm_data.update(CORRESP_TPRM_DATA_KAFKA)
    corres_tprm_data["val_type"] = InventoryFieldValType.DATE.value
    corres_tprm_data["multiple"] = True

    corres_prm_data = dict()
    corres_prm_data.update(CORRESP_PRM_DATA)

    corresponding_value = ["2000-01-01"]
    corresponding_value_as_mult = str(pickle.dumps(corresponding_value).hex())
    corres_prm_data["value"] = corresponding_value_as_mult

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[CORRESP_TMO_DATA, PRM_LINK_TMO_DATA],
        list_of_tprm_data_kafka_format=[
            corres_tprm_data,
            PRM_LINK_TPRM_DATA_KAFKA,
        ],
        list_of_mo_data_kafka_format=[CORRESP_MO_DATA, PRM_LINK_MO_DATA],
        list_of_prm_data_kafka_format=[corres_prm_data],
    )

    search_query = {"match": {"id": PRM_LINK_PRM_DATA["mo_id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )
    tprm_id_as_str = str(PRM_LINK_PRM_DATA["tprm_id"])
    assert res_before["hits"]["total"]["value"] == 1
    value = res_before["hits"]["hits"][0]["_source"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ].get(tprm_id_as_str)
    assert value is None

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[PRM_LINK_PRM_DATA], msg_event=MSG_EVENT
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
    assert value == corresponding_value


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_16(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists
    and prm_link TPRM.multiple = False
    and corresponding tprm val_type is 'InventoryFieldValType.DATETIME.value'
    and corresponding TPRM.multiple = True :
    - updates mo data in  record in INVENTORY_OBJ_INDEX_PREFIX
    - value is instance of InventoryFieldValType.DATETIME.value
    - new value available in search"""
    corres_tprm_data = dict()
    corres_tprm_data.update(CORRESP_TPRM_DATA_KAFKA)
    corres_tprm_data["val_type"] = InventoryFieldValType.DATETIME.value
    corres_tprm_data["multiple"] = True

    corres_prm_data = dict()
    corres_prm_data.update(CORRESP_PRM_DATA)

    corresponding_value = ["2000-01-01"]
    corresponding_value_as_mult = str(pickle.dumps(corresponding_value).hex())
    corres_prm_data["value"] = corresponding_value_as_mult

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[CORRESP_TMO_DATA, PRM_LINK_TMO_DATA],
        list_of_tprm_data_kafka_format=[
            corres_tprm_data,
            PRM_LINK_TPRM_DATA_KAFKA,
        ],
        list_of_mo_data_kafka_format=[CORRESP_MO_DATA, PRM_LINK_MO_DATA],
        list_of_prm_data_kafka_format=[corres_prm_data],
    )

    search_query = {"match": {"id": PRM_LINK_PRM_DATA["mo_id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )
    tprm_id_as_str = str(PRM_LINK_PRM_DATA["tprm_id"])
    assert res_before["hits"]["total"]["value"] == 1
    value = res_before["hits"]["hits"][0]["_source"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ].get(tprm_id_as_str)
    assert value is None

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[PRM_LINK_PRM_DATA], msg_event=MSG_EVENT
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
    assert value == [f"{corresponding_value[0]}T00:00:00.000000Z"]


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_17(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists
    and prm_link TPRM.multiple = True
    and corresponding tprm val_type is 'InventoryFieldValType.INT.value'
    and corresponding TPRM.multiple = False :
    - updates mo data in  record in INVENTORY_OBJ_INDEX_PREFIX
    - value is instance of InventoryFieldValType.INT.value
    - new value available in search"""
    corres_tprm_data = dict()
    corres_tprm_data.update(CORRESP_TPRM_DATA_KAFKA)
    corres_tprm_data["val_type"] = InventoryFieldValType.INT.value

    corres_prm_data = dict()
    corres_prm_data.update(CORRESP_PRM_DATA)
    corresponding_value = "25"
    corres_prm_data["value"] = corresponding_value

    prm_link_tprm_data = dict()
    prm_link_tprm_data.update(PRM_LINK_TPRM_DATA_KAFKA)
    prm_link_tprm_data["multiple"] = True

    prm_link_prm_data = dict()
    prm_link_prm_data.update(PRM_LINK_PRM_DATA)
    prm_link_prm_data["value"] = str(
        pickle.dumps([corres_prm_data["id"], corres_prm_data["id"]]).hex()
    )

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[CORRESP_TMO_DATA, PRM_LINK_TMO_DATA],
        list_of_tprm_data_kafka_format=[corres_tprm_data, prm_link_tprm_data],
        list_of_mo_data_kafka_format=[CORRESP_MO_DATA, PRM_LINK_MO_DATA],
        list_of_prm_data_kafka_format=[corres_prm_data],
    )

    search_query = {"match": {"id": PRM_LINK_PRM_DATA["mo_id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )
    tprm_id_as_str = str(PRM_LINK_PRM_DATA["tprm_id"])
    assert res_before["hits"]["total"]["value"] == 1
    value = res_before["hits"]["hits"][0]["_source"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ].get(tprm_id_as_str)
    assert value is None

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_link_prm_data], msg_event=MSG_EVENT
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
    assert value == [int(corresponding_value), int(corresponding_value)]


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_18(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists
    and prm_link TPRM.multiple = True
    and corresponding tprm val_type is 'InventoryFieldValType.STR.value'
    and corresponding TPRM.multiple = False :
    - updates mo data in  record in INVENTORY_OBJ_INDEX_PREFIX
    - value is instance of InventoryFieldValType.STR.value
    - new value available in search"""
    corres_tprm_data = dict()
    corres_tprm_data.update(CORRESP_TPRM_DATA_KAFKA)
    corres_tprm_data["val_type"] = InventoryFieldValType.STR.value

    corres_prm_data = dict()
    corres_prm_data.update(CORRESP_PRM_DATA)
    corresponding_value = "25"
    corres_prm_data["value"] = corresponding_value

    prm_link_tprm_data = dict()
    prm_link_tprm_data.update(PRM_LINK_TPRM_DATA_KAFKA)
    prm_link_tprm_data["multiple"] = True

    prm_link_prm_data = dict()
    prm_link_prm_data.update(PRM_LINK_PRM_DATA)
    prm_link_prm_data["value"] = str(
        pickle.dumps([corres_prm_data["id"], corres_prm_data["id"]]).hex()
    )

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[CORRESP_TMO_DATA, PRM_LINK_TMO_DATA],
        list_of_tprm_data_kafka_format=[corres_tprm_data, prm_link_tprm_data],
        list_of_mo_data_kafka_format=[CORRESP_MO_DATA, PRM_LINK_MO_DATA],
        list_of_prm_data_kafka_format=[corres_prm_data],
    )

    search_query = {"match": {"id": PRM_LINK_PRM_DATA["mo_id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )
    tprm_id_as_str = str(PRM_LINK_PRM_DATA["tprm_id"])
    assert res_before["hits"]["total"]["value"] == 1
    value = res_before["hits"]["hits"][0]["_source"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ].get(tprm_id_as_str)
    assert value is None

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_link_prm_data], msg_event=MSG_EVENT
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
    assert value == [corresponding_value, corresponding_value]


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_19(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists
    and prm_link TPRM.multiple = True
    and corresponding tprm val_type is 'InventoryFieldValType.FLOAT.value'
    and corresponding TPRM.multiple = False :
    - updates mo data in  record in INVENTORY_OBJ_INDEX_PREFIX
    - value is instance of InventoryFieldValType.FLOAT.value
    - new value available in search"""
    corres_tprm_data = dict()
    corres_tprm_data.update(CORRESP_TPRM_DATA_KAFKA)
    corres_tprm_data["val_type"] = InventoryFieldValType.FLOAT.value

    corres_prm_data = dict()
    corres_prm_data.update(CORRESP_PRM_DATA)
    corresponding_value = "25"
    corres_prm_data["value"] = corresponding_value

    prm_link_tprm_data = dict()
    prm_link_tprm_data.update(PRM_LINK_TPRM_DATA_KAFKA)
    prm_link_tprm_data["multiple"] = True

    prm_link_prm_data = dict()
    prm_link_prm_data.update(PRM_LINK_PRM_DATA)
    prm_link_prm_data["value"] = str(
        pickle.dumps([corres_prm_data["id"], corres_prm_data["id"]]).hex()
    )

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[CORRESP_TMO_DATA, PRM_LINK_TMO_DATA],
        list_of_tprm_data_kafka_format=[corres_tprm_data, prm_link_tprm_data],
        list_of_mo_data_kafka_format=[CORRESP_MO_DATA, PRM_LINK_MO_DATA],
        list_of_prm_data_kafka_format=[corres_prm_data],
    )

    search_query = {"match": {"id": PRM_LINK_PRM_DATA["mo_id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )
    tprm_id_as_str = str(PRM_LINK_PRM_DATA["tprm_id"])
    assert res_before["hits"]["total"]["value"] == 1
    value = res_before["hits"]["hits"][0]["_source"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ].get(tprm_id_as_str)
    assert value is None

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_link_prm_data], msg_event=MSG_EVENT
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
    assert value == [float(corresponding_value), float(corresponding_value)]


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_20(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists
    and prm_link TPRM.multiple = True
    and corresponding tprm val_type is 'InventoryFieldValType.BOOL.value'
    and corresponding TPRM.multiple = False :
    - updates mo data in  record in INVENTORY_OBJ_INDEX_PREFIX
    - value is instance of InventoryFieldValType.BOOL.value
    - new value available in search"""
    corres_tprm_data = dict()
    corres_tprm_data.update(CORRESP_TPRM_DATA_KAFKA)
    corres_tprm_data["val_type"] = InventoryFieldValType.BOOL.value

    corres_prm_data = dict()
    corres_prm_data.update(CORRESP_PRM_DATA)
    corresponding_value = "true"
    corres_prm_data["value"] = corresponding_value

    prm_link_tprm_data = dict()
    prm_link_tprm_data.update(PRM_LINK_TPRM_DATA_KAFKA)
    prm_link_tprm_data["multiple"] = True

    prm_link_prm_data = dict()
    prm_link_prm_data.update(PRM_LINK_PRM_DATA)
    prm_link_prm_data["value"] = str(
        pickle.dumps([corres_prm_data["id"], corres_prm_data["id"]]).hex()
    )

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[CORRESP_TMO_DATA, PRM_LINK_TMO_DATA],
        list_of_tprm_data_kafka_format=[corres_tprm_data, prm_link_tprm_data],
        list_of_mo_data_kafka_format=[CORRESP_MO_DATA, PRM_LINK_MO_DATA],
        list_of_prm_data_kafka_format=[corres_prm_data],
    )

    search_query = {"match": {"id": PRM_LINK_PRM_DATA["mo_id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )
    tprm_id_as_str = str(PRM_LINK_PRM_DATA["tprm_id"])
    assert res_before["hits"]["total"]["value"] == 1
    value = res_before["hits"]["hits"][0]["_source"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ].get(tprm_id_as_str)
    assert value is None

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_link_prm_data], msg_event=MSG_EVENT
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
    assert value == [True, True]


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_21(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists
    and prm_link TPRM.multiple = True
    and corresponding tprm val_type is 'InventoryFieldValType.DATE.value'
    and corresponding TPRM.multiple = False :
    - updates mo data in  record in INVENTORY_OBJ_INDEX_PREFIX
    - value is instance of InventoryFieldValType.DATE.value
    - new value available in search"""
    corres_tprm_data = dict()
    corres_tprm_data.update(CORRESP_TPRM_DATA_KAFKA)
    corres_tprm_data["val_type"] = InventoryFieldValType.DATE.value

    corres_prm_data = dict()
    corres_prm_data.update(CORRESP_PRM_DATA)
    corresponding_value = "2000-01-01"
    corres_prm_data["value"] = corresponding_value

    prm_link_tprm_data = dict()
    prm_link_tprm_data.update(PRM_LINK_TPRM_DATA_KAFKA)
    prm_link_tprm_data["multiple"] = True

    prm_link_prm_data = dict()
    prm_link_prm_data.update(PRM_LINK_PRM_DATA)
    prm_link_prm_data["value"] = str(
        pickle.dumps([corres_prm_data["id"], corres_prm_data["id"]]).hex()
    )

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[CORRESP_TMO_DATA, PRM_LINK_TMO_DATA],
        list_of_tprm_data_kafka_format=[corres_tprm_data, prm_link_tprm_data],
        list_of_mo_data_kafka_format=[CORRESP_MO_DATA, PRM_LINK_MO_DATA],
        list_of_prm_data_kafka_format=[corres_prm_data],
    )

    search_query = {"match": {"id": PRM_LINK_PRM_DATA["mo_id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )
    tprm_id_as_str = str(PRM_LINK_PRM_DATA["tprm_id"])
    assert res_before["hits"]["total"]["value"] == 1
    value = res_before["hits"]["hits"][0]["_source"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ].get(tprm_id_as_str)
    assert value is None

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_link_prm_data], msg_event=MSG_EVENT
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
    assert value == [corresponding_value, corresponding_value]


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_22(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists
    and prm_link TPRM.multiple = True
    and corresponding tprm val_type is 'InventoryFieldValType.DATETIME.value'
    and corresponding TPRM.multiple = False :
    - updates mo data in  record in INVENTORY_OBJ_INDEX_PREFIX
    - value is instance of InventoryFieldValType.DATETIME.value
    - new value available in search"""
    corres_tprm_data = dict()
    corres_tprm_data.update(CORRESP_TPRM_DATA_KAFKA)
    corres_tprm_data["val_type"] = InventoryFieldValType.DATETIME.value

    corres_prm_data = dict()
    corres_prm_data.update(CORRESP_PRM_DATA)
    corresponding_value = "2000-01-01"
    corres_prm_data["value"] = corresponding_value

    prm_link_tprm_data = dict()
    prm_link_tprm_data.update(PRM_LINK_TPRM_DATA_KAFKA)
    prm_link_tprm_data["multiple"] = True

    prm_link_prm_data = dict()
    prm_link_prm_data.update(PRM_LINK_PRM_DATA)
    prm_link_prm_data["value"] = str(
        pickle.dumps([corres_prm_data["id"], corres_prm_data["id"]]).hex()
    )

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[CORRESP_TMO_DATA, PRM_LINK_TMO_DATA],
        list_of_tprm_data_kafka_format=[corres_tprm_data, prm_link_tprm_data],
        list_of_mo_data_kafka_format=[CORRESP_MO_DATA, PRM_LINK_MO_DATA],
        list_of_prm_data_kafka_format=[corres_prm_data],
    )

    search_query = {"match": {"id": PRM_LINK_PRM_DATA["mo_id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )
    tprm_id_as_str = str(PRM_LINK_PRM_DATA["tprm_id"])
    assert res_before["hits"]["total"]["value"] == 1
    value = res_before["hits"]["hits"][0]["_source"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ].get(tprm_id_as_str)
    assert value is None

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_link_prm_data], msg_event=MSG_EVENT
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
    assert value == [
        f"{corresponding_value}T00:00:00.000000Z",
        f"{corresponding_value}T00:00:00.000000Z",
    ]


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_23(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists
    and prm_link TPRM.multiple = True
    and corresponding tprm val_type is 'InventoryFieldValType.INT.value'
    and corresponding TPRM.multiple = True :
    - updates mo data in  record in INVENTORY_OBJ_INDEX_PREFIX
    - value is instance of InventoryFieldValType.INT.value
    - new value available in search"""
    corres_tprm_data = dict()
    corres_tprm_data.update(CORRESP_TPRM_DATA_KAFKA)
    corres_tprm_data["val_type"] = InventoryFieldValType.INT.value
    corres_tprm_data["multiple"] = True

    corres_prm_data = dict()
    corres_prm_data.update(CORRESP_PRM_DATA)
    corresponding_value = [25]
    corresponding_pickled_value = str(pickle.dumps(corresponding_value).hex())
    corres_prm_data["value"] = corresponding_pickled_value

    prm_link_tprm_data = dict()
    prm_link_tprm_data.update(PRM_LINK_TPRM_DATA_KAFKA)
    prm_link_tprm_data["multiple"] = True

    prm_link_prm_data = dict()
    prm_link_prm_data.update(PRM_LINK_PRM_DATA)
    prm_link_prm_data["value"] = str(
        pickle.dumps([corres_prm_data["id"], corres_prm_data["id"]]).hex()
    )

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[CORRESP_TMO_DATA, PRM_LINK_TMO_DATA],
        list_of_tprm_data_kafka_format=[corres_tprm_data, prm_link_tprm_data],
        list_of_mo_data_kafka_format=[CORRESP_MO_DATA, PRM_LINK_MO_DATA],
        list_of_prm_data_kafka_format=[corres_prm_data],
    )

    search_query = {"match": {"id": PRM_LINK_PRM_DATA["mo_id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )
    tprm_id_as_str = str(PRM_LINK_PRM_DATA["tprm_id"])
    assert res_before["hits"]["total"]["value"] == 1
    value = res_before["hits"]["hits"][0]["_source"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ].get(tprm_id_as_str)
    assert value is None

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_link_prm_data], msg_event=MSG_EVENT
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
    assert value == [corresponding_value, corresponding_value]


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_24(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists
    and prm_link TPRM.multiple = True
    and corresponding tprm val_type is 'InventoryFieldValType.STR.value'
    and corresponding TPRM.multiple = True :
    - updates mo data in  record in INVENTORY_OBJ_INDEX_PREFIX
    - value is instance of InventoryFieldValType.STR.value
    - new value available in search"""
    corres_tprm_data = dict()
    corres_tprm_data.update(CORRESP_TPRM_DATA_KAFKA)
    corres_tprm_data["val_type"] = InventoryFieldValType.STR.value
    corres_tprm_data["multiple"] = True

    corres_prm_data = dict()
    corres_prm_data.update(CORRESP_PRM_DATA)
    corresponding_value = ["25"]
    corresponding_pickled_value = str(pickle.dumps(corresponding_value).hex())
    corres_prm_data["value"] = corresponding_pickled_value

    prm_link_tprm_data = dict()
    prm_link_tprm_data.update(PRM_LINK_TPRM_DATA_KAFKA)
    prm_link_tprm_data["multiple"] = True

    prm_link_prm_data = dict()
    prm_link_prm_data.update(PRM_LINK_PRM_DATA)
    prm_link_prm_data["value"] = str(
        pickle.dumps([corres_prm_data["id"], corres_prm_data["id"]]).hex()
    )

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[CORRESP_TMO_DATA, PRM_LINK_TMO_DATA],
        list_of_tprm_data_kafka_format=[corres_tprm_data, prm_link_tprm_data],
        list_of_mo_data_kafka_format=[CORRESP_MO_DATA, PRM_LINK_MO_DATA],
        list_of_prm_data_kafka_format=[corres_prm_data],
    )

    search_query = {"match": {"id": PRM_LINK_PRM_DATA["mo_id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )
    tprm_id_as_str = str(PRM_LINK_PRM_DATA["tprm_id"])
    assert res_before["hits"]["total"]["value"] == 1
    value = res_before["hits"]["hits"][0]["_source"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ].get(tprm_id_as_str)
    assert value is None

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_link_prm_data], msg_event=MSG_EVENT
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
    assert value == [corresponding_value, corresponding_value]


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_25(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists
    and prm_link TPRM.multiple = True
    and corresponding tprm val_type is 'InventoryFieldValType.FLOAT.value'
    and corresponding TPRM.multiple = True :
    - updates mo data in  record in INVENTORY_OBJ_INDEX_PREFIX
    - value is instance of InventoryFieldValType.FLOAT.value
    - new value available in search"""
    corres_tprm_data = dict()
    corres_tprm_data.update(CORRESP_TPRM_DATA_KAFKA)
    corres_tprm_data["val_type"] = InventoryFieldValType.FLOAT.value
    corres_tprm_data["multiple"] = True

    corres_prm_data = dict()
    corres_prm_data.update(CORRESP_PRM_DATA)
    corresponding_value = [25.52]
    corresponding_pickled_value = str(pickle.dumps(corresponding_value).hex())
    corres_prm_data["value"] = corresponding_pickled_value

    prm_link_tprm_data = dict()
    prm_link_tprm_data.update(PRM_LINK_TPRM_DATA_KAFKA)
    prm_link_tprm_data["multiple"] = True

    prm_link_prm_data = dict()
    prm_link_prm_data.update(PRM_LINK_PRM_DATA)
    prm_link_prm_data["value"] = str(
        pickle.dumps([corres_prm_data["id"], corres_prm_data["id"]]).hex()
    )

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[CORRESP_TMO_DATA, PRM_LINK_TMO_DATA],
        list_of_tprm_data_kafka_format=[corres_tprm_data, prm_link_tprm_data],
        list_of_mo_data_kafka_format=[CORRESP_MO_DATA, PRM_LINK_MO_DATA],
        list_of_prm_data_kafka_format=[corres_prm_data],
    )

    search_query = {"match": {"id": PRM_LINK_PRM_DATA["mo_id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )
    tprm_id_as_str = str(PRM_LINK_PRM_DATA["tprm_id"])
    assert res_before["hits"]["total"]["value"] == 1
    value = res_before["hits"]["hits"][0]["_source"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ].get(tprm_id_as_str)
    assert value is None

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_link_prm_data], msg_event=MSG_EVENT
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
    assert value == [corresponding_value, corresponding_value]


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_26(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists
    and prm_link TPRM.multiple = True
    and corresponding tprm val_type is 'InventoryFieldValType.BOOL.value'
    and corresponding TPRM.multiple = True :
    - updates mo data in  record in INVENTORY_OBJ_INDEX_PREFIX
    - value is instance of InventoryFieldValType.BOOL.value
    - new value available in search"""
    corres_tprm_data = dict()
    corres_tprm_data.update(CORRESP_TPRM_DATA_KAFKA)
    corres_tprm_data["val_type"] = InventoryFieldValType.BOOL.value
    corres_tprm_data["multiple"] = True

    corres_prm_data = dict()
    corres_prm_data.update(CORRESP_PRM_DATA)
    corresponding_value = [True]
    corresponding_pickled_value = str(pickle.dumps(corresponding_value).hex())
    corres_prm_data["value"] = corresponding_pickled_value

    prm_link_tprm_data = dict()
    prm_link_tprm_data.update(PRM_LINK_TPRM_DATA_KAFKA)
    prm_link_tprm_data["multiple"] = True

    prm_link_prm_data = dict()
    prm_link_prm_data.update(PRM_LINK_PRM_DATA)
    prm_link_prm_data["value"] = str(
        pickle.dumps([corres_prm_data["id"], corres_prm_data["id"]]).hex()
    )

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[CORRESP_TMO_DATA, PRM_LINK_TMO_DATA],
        list_of_tprm_data_kafka_format=[corres_tprm_data, prm_link_tprm_data],
        list_of_mo_data_kafka_format=[CORRESP_MO_DATA, PRM_LINK_MO_DATA],
        list_of_prm_data_kafka_format=[corres_prm_data],
    )

    search_query = {"match": {"id": PRM_LINK_PRM_DATA["mo_id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )
    tprm_id_as_str = str(PRM_LINK_PRM_DATA["tprm_id"])
    assert res_before["hits"]["total"]["value"] == 1
    value = res_before["hits"]["hits"][0]["_source"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ].get(tprm_id_as_str)
    assert value is None

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_link_prm_data], msg_event=MSG_EVENT
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
    assert value == [[True], [True]]


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_27(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists
    and prm_link TPRM.multiple = True
    and corresponding tprm val_type is 'InventoryFieldValType.DATE.value'
    and corresponding TPRM.multiple = True :
    - updates mo data in  record in INVENTORY_OBJ_INDEX_PREFIX
    - value is instance of InventoryFieldValType.DATE.value
    - new value available in search"""
    corres_tprm_data = dict()
    corres_tprm_data.update(CORRESP_TPRM_DATA_KAFKA)
    corres_tprm_data["val_type"] = InventoryFieldValType.DATE.value
    corres_tprm_data["multiple"] = True

    corres_prm_data = dict()
    corres_prm_data.update(CORRESP_PRM_DATA)
    corresponding_value = ["2000-01-01"]
    corresponding_pickled_value = str(pickle.dumps(corresponding_value).hex())
    corres_prm_data["value"] = corresponding_pickled_value

    prm_link_tprm_data = dict()
    prm_link_tprm_data.update(PRM_LINK_TPRM_DATA_KAFKA)
    prm_link_tprm_data["multiple"] = True

    prm_link_prm_data = dict()
    prm_link_prm_data.update(PRM_LINK_PRM_DATA)
    prm_link_prm_data["value"] = str(
        pickle.dumps([corres_prm_data["id"], corres_prm_data["id"]]).hex()
    )

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[CORRESP_TMO_DATA, PRM_LINK_TMO_DATA],
        list_of_tprm_data_kafka_format=[corres_tprm_data, prm_link_tprm_data],
        list_of_mo_data_kafka_format=[CORRESP_MO_DATA, PRM_LINK_MO_DATA],
        list_of_prm_data_kafka_format=[corres_prm_data],
    )

    search_query = {"match": {"id": PRM_LINK_PRM_DATA["mo_id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )
    tprm_id_as_str = str(PRM_LINK_PRM_DATA["tprm_id"])
    assert res_before["hits"]["total"]["value"] == 1
    value = res_before["hits"]["hits"][0]["_source"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ].get(tprm_id_as_str)
    assert value is None

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_link_prm_data], msg_event=MSG_EVENT
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
    assert value == [corresponding_value, corresponding_value]


@pytest.mark.asyncio(loop_scope="session")
async def test_on_mo_link_prm_create_case_28(async_elastic_session):
    """TEST With receiving PRM:created msg - if value of mo_link prm (mo id) exists
    and prm_link TPRM.multiple = True
    and corresponding tprm val_type is 'InventoryFieldValType.DATETIME.value'
    and corresponding TPRM.multiple = True :
    - updates mo data in  record in INVENTORY_OBJ_INDEX_PREFIX
    - value is instance of InventoryFieldValType.DATETIME.value
    - new value available in search"""
    corres_tprm_data = dict()
    corres_tprm_data.update(CORRESP_TPRM_DATA_KAFKA)
    corres_tprm_data["val_type"] = InventoryFieldValType.DATETIME.value
    corres_tprm_data["multiple"] = True

    corres_prm_data = dict()
    corres_prm_data.update(CORRESP_PRM_DATA)
    corresponding_value = ["2000-01-01"]
    corresponding_pickled_value = str(pickle.dumps(corresponding_value).hex())
    corres_prm_data["value"] = corresponding_pickled_value

    prm_link_tprm_data = dict()
    prm_link_tprm_data.update(PRM_LINK_TPRM_DATA_KAFKA)
    prm_link_tprm_data["multiple"] = True

    prm_link_prm_data = dict()
    prm_link_prm_data.update(PRM_LINK_PRM_DATA)
    prm_link_prm_data["value"] = str(
        pickle.dumps([corres_prm_data["id"], corres_prm_data["id"]]).hex()
    )

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[CORRESP_TMO_DATA, PRM_LINK_TMO_DATA],
        list_of_tprm_data_kafka_format=[corres_tprm_data, prm_link_tprm_data],
        list_of_mo_data_kafka_format=[CORRESP_MO_DATA, PRM_LINK_MO_DATA],
        list_of_prm_data_kafka_format=[corres_prm_data],
    )

    search_query = {"match": {"id": PRM_LINK_PRM_DATA["mo_id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )
    tprm_id_as_str = str(PRM_LINK_PRM_DATA["tprm_id"])
    assert res_before["hits"]["total"]["value"] == 1
    value = res_before["hits"]["hits"][0]["_source"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ].get(tprm_id_as_str)
    assert value is None

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[prm_link_prm_data], msg_event=MSG_EVENT
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
    assert value == [
        [f"{corresponding_value[0]}T00:00:00.000000Z"],
        [f"{corresponding_value[0]}T00:00:00.000000Z"],
    ]
