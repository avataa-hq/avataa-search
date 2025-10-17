import copy
import datetime

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from elastic.config import ALL_MO_OBJ_INDEXES_PATTERN
from elastic.enum_models import InventoryFieldValType
from indexes_mapping.inventory.mapping import INVENTORY_PARAMETERS_FIELD_NAME

from services.inventory_services.kafka.consumers.inventory_changes.utils import (
    InventoryChangesHandler,
)
from tests.kafka.consumers.topics.inventory_changes.utils import (
    create_all_default_data_inventory_changes_topic,
    create_default_tmo_in_elastic,
    create_default_mo_in_elastic,
    create_cleared_kafka_mo_msg,
)

datetime_value = datetime.datetime.now()
proto_timestamp = Timestamp()
proto_timestamp.FromDatetime(datetime_value)

MO_LINK_TMO_DATA = {
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

MO_LINK_TPRM_DATA_KAFKA = {
    "id": 4,
    "name": "DEFAULT TPRM",
    "val_type": InventoryFieldValType.MO_LINK.value,
    "required": False,
    "returnable": True,
    "tmo_id": MO_LINK_TMO_DATA["id"],
    "creation_date": proto_timestamp,
    "modification_date": proto_timestamp,
    "description": "",
    "multiple": False,
    "constraint": None,
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

MO_LINK_MO_DATA = {
    "id": 6,
    "name": "DEFAULT MO DATA",
    "active": True,
    "tmo_id": MO_LINK_TMO_DATA["id"],
    "latitude": 0.0,
    "longitude": 0.0,
    "p_id": 0,
    "point_a_id": 0,
    "point_b_id": 0,
    "model": "",
    "version": 0,
    "status": "",
    "parameters": dict(),
}

UPDATED_CORRESP_MO_DATA = copy.deepcopy(CORRESP_MO_DATA)
UPDATED_CORRESP_MO_DATA["name"] = "Updated name"

MO_LINK_PRM_DATA = {
    "id": 8,
    "value": f"{CORRESP_MO_DATA['id']}",
    "tprm_id": MO_LINK_TPRM_DATA_KAFKA["id"],
    "mo_id": MO_LINK_MO_DATA["id"],
    "version": 0,
}

MSG_CLASS_NAME = "MO"
MSG_EVENT = "updated"
MSG_AS_DICT = {"objects": [UPDATED_CORRESP_MO_DATA]}


@pytest.mark.asyncio(loop_scope="session")
async def test_with_mo_name_update_case_1(async_elastic_session):
    """TEST With receiving PRM:UPDATED msg - if the value of prm, which is contained in some ptm_link, is updated
    (if prm_link is not multiple and corresponding prm is not multiple):
    - updates value of MO parameter"""
    mo_link_tprm_id_as_str = str(MO_LINK_TPRM_DATA_KAFKA["id"])

    for tmo_data in [CORRESP_TMO_DATA, MO_LINK_TMO_DATA]:
        await create_default_tmo_in_elastic(
            async_elastic_session, default_tmo_data=tmo_data
        )

    for mo in [CORRESP_MO_DATA, MO_LINK_MO_DATA]:
        await create_default_mo_in_elastic(
            async_elastic_session=async_elastic_session, default_mo_data=mo
        )

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tprm_data_kafka_format=[MO_LINK_TPRM_DATA_KAFKA],
        list_of_prm_data_kafka_format=[MO_LINK_PRM_DATA],
    )

    search_query = {"match": {"id": MO_LINK_MO_DATA["id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )
    print(res_before)
    assert res_before["hits"]["total"]["value"] == 1
    res_before = res_before["hits"]["hits"][0]["_source"]
    assert (
        res_before[INVENTORY_PARAMETERS_FIELD_NAME][mo_link_tprm_id_as_str]
        == CORRESP_MO_DATA["name"]
    )
    assert (
        res_before[INVENTORY_PARAMETERS_FIELD_NAME][mo_link_tprm_id_as_str]
        != UPDATED_CORRESP_MO_DATA["name"]
    )

    kafka_msg = create_cleared_kafka_mo_msg(
        list_of_mo_data=[UPDATED_CORRESP_MO_DATA], msg_event=MSG_EVENT
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

    res_after = res_after["hits"]["hits"][0]["_source"]
    assert (
        res_after[INVENTORY_PARAMETERS_FIELD_NAME][mo_link_tprm_id_as_str]
        != CORRESP_MO_DATA["name"]
    )
    assert (
        res_after[INVENTORY_PARAMETERS_FIELD_NAME][mo_link_tprm_id_as_str]
        == UPDATED_CORRESP_MO_DATA["name"]
    )
