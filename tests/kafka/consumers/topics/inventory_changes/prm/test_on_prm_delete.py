import datetime

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from elastic.config import (
    INVENTORY_OBJ_INDEX_PREFIX,
    INVENTORY_TPRM_INDEX_V2,
    INVENTORY_PRM_INDEX,
)
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
    "val_type": "str",
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
    "value": "Some val",
    "tprm_id": DEFAULT_TPRM_DATA_KAFKA["id"],
    "mo_id": DEFAULT_MO_DATA["id"],
    "version": 0,
}

MSG_CLASS_NAME = "PRM"
MSG_EVENT = "deleted"
MSG_AS_DICT = {"objects": [DEFAULT_PRM_DATA]}


@pytest.mark.asyncio(loop_scope="session")
async def test_on_delete_prm_case_2(async_elastic_session):
    """TEST With receiving PRM:deleted msg - if tprm does not exist:
    - does not raise error"""

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


@pytest.mark.asyncio(loop_scope="session")
async def test_on_delete_prm_case_3(async_elastic_session):
    """TEST With receiving PRM:deleted msg - if tprm exists but mo does not exist:
    - does not raise error"""

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
    try:
        await handler.process_the_message()
    except Exception:
        assert False


@pytest.mark.asyncio(loop_scope="session")
async def test_on_delete_prm_case_4(async_elastic_session):
    """TEST With receiving PRM:deleted msg - if tprm and mo exist:
    - deletes only special param,
    - does not delete mo"""

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[DEFAULT_TMO_DATA],
        list_of_tprm_data_kafka_format=[DEFAULT_TPRM_DATA_KAFKA],
        list_of_mo_data_kafka_format=[DEFAULT_MO_DATA],
        list_of_prm_data_kafka_format=[DEFAULT_PRM_DATA],
    )

    # before

    all_mo_indexes = f"{INVENTORY_OBJ_INDEX_PREFIX}*"
    search_query = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{DEFAULT_TPRM_DATA_KAFKA['id']}": DEFAULT_PRM_DATA[
                "value"
            ]
        }
    }
    search_res = await async_elastic_session.search(
        index=all_mo_indexes, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1
    mo_id = search_res["hits"]["hits"][0]["_source"]["id"]

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[DEFAULT_PRM_DATA], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)

    await handler.process_the_message()

    # record with matched data does not exist
    search_res = await async_elastic_session.search(
        index=all_mo_indexes, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 0

    # mo was not deleted
    search_query = {"match": {"id": mo_id}}
    search_res = await async_elastic_session.search(
        index=all_mo_indexes, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_on_deleted_prm_case_5(async_elastic_session):
    """TEST With receiving PRM:deleted msg - if msg contains two ot more prm with same mo_id:
    - deletes only special params,
    - does not delete mo"""

    first_tprm = DEFAULT_TPRM_DATA_KAFKA
    second_tprm = dict()
    second_tprm.update(DEFAULT_TPRM_DATA_KAFKA)
    second_tprm["id"] += 1

    first_prm = dict()
    first_prm.update(DEFAULT_PRM_DATA)
    first_prm["tprm_id"] = first_tprm["id"]
    first_prm_value = "first_prm_value"
    first_prm["value"] = first_prm_value

    second_prm = dict()
    second_prm.update(DEFAULT_PRM_DATA)
    second_prm["tprm_id"] = second_tprm["id"]
    second_prm_value = "second_prm_value"
    second_prm["value"] = second_prm_value

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

    all_mo_indexes = f"{INVENTORY_OBJ_INDEX_PREFIX}*"
    search_query_1_prm = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{first_tprm['id']}": first_prm_value
        }
    }
    search_query_2_prm = {
        "match": {
            f"{INVENTORY_PARAMETERS_FIELD_NAME}.{second_tprm['id']}": second_prm_value
        }
    }
    search_query = {"bool": {"must": [search_query_1_prm, search_query_2_prm]}}
    search_res = await async_elastic_session.search(
        index=all_mo_indexes, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1
    mo_id = search_res["hits"]["hits"][0]["_source"]["id"]

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[first_prm, second_prm], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # record with matched data exists
    all_mo_indexes = f"{INVENTORY_OBJ_INDEX_PREFIX}*"

    search_res = await async_elastic_session.search(
        index=all_mo_indexes, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 0

    # mo was not deleted
    search_query = {"match": {"id": mo_id}}
    search_res = await async_elastic_session.search(
        index=all_mo_indexes, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_on_deleted_prm_case_6(async_elastic_session):
    """TEST With receiving PRM:deleted msg:
    - deletes prm in INVENTORY_PRM_INDEX,
    - does not delete mo"""

    # create tmo first tprm and default mo data
    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tmo_data=[DEFAULT_TMO_DATA],
        list_of_tprm_data_kafka_format=[DEFAULT_TPRM_DATA_KAFKA],
        list_of_mo_data_kafka_format=[DEFAULT_MO_DATA],
        list_of_prm_data_kafka_format=[DEFAULT_PRM_DATA],
    )

    search_query = {"match": {"id": DEFAULT_PRM_DATA["id"]}}

    res_before = await async_elastic_session.search(
        index=INVENTORY_PRM_INDEX, query=search_query, track_total_hits=True
    )
    assert res_before["hits"]["total"]["value"] == 1

    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[DEFAULT_PRM_DATA], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    res_after = await async_elastic_session.search(
        index=INVENTORY_PRM_INDEX, query=search_query, track_total_hits=True
    )
    assert res_after["hits"]["total"]["value"] == 0
