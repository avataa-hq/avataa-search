import copy
import datetime
import pickle

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
    create_cleared_kafka_prm_msg,
    create_default_tmo_in_elastic,
    create_default_mo_in_elastic,
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
    "parameters": {"4": "25"},
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
MSG_EVENT = "updated"
MSG_AS_DICT = {"objects": [PRM_LINK_PRM_DATA]}


@pytest.mark.asyncio(loop_scope="session")
async def test_with_prm_link_prm_update_case_1(async_elastic_session):
    """TEST With receiving PRM:UPDATED msg - if the value of prm, which is contained in some ptm_link, is updated
    (if prm_link is not multiple and corresponding prm is not multiple):
    - updates value of MO parameter"""
    corresp_tprm_type = InventoryFieldValType.INT.value
    corresp_tprm_multiple = False
    corresp_prm_value = "25"
    updated_corresp_prm_value = "34"
    expected_value = int(updated_corresp_prm_value)

    prm_link_mo_data = copy.deepcopy(PRM_LINK_MO_DATA)
    prm_link_mo_data[INVENTORY_PARAMETERS_FIELD_NAME][
        str(PRM_LINK_TPRM_DATA_KAFKA["id"])
    ] = int(corresp_prm_value)

    corresp_tprm_data_kafka = copy.deepcopy(CORRESP_TPRM_DATA_KAFKA)
    corresp_tprm_data_kafka["val_type"] = corresp_tprm_type
    corresp_tprm_data_kafka["multiple"] = corresp_tprm_multiple

    corresp_prm_data = copy.deepcopy(CORRESP_PRM_DATA)
    corresp_prm_data["value"] = corresp_prm_value

    for tmo_data in [CORRESP_TMO_DATA, PRM_LINK_TMO_DATA]:
        await create_default_tmo_in_elastic(
            async_elastic_session, default_tmo_data=tmo_data
        )

    for mo in [CORRESP_MO_DATA, prm_link_mo_data]:
        await create_default_mo_in_elastic(
            async_elastic_session=async_elastic_session, default_mo_data=mo
        )

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tprm_data_kafka_format=[
            corresp_tprm_data_kafka,
            PRM_LINK_TPRM_DATA_KAFKA,
        ],
        list_of_prm_data_kafka_format=[corresp_prm_data, PRM_LINK_PRM_DATA],
    )

    search_query = {"match": {"id": prm_link_mo_data["id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )

    assert res_before["hits"]["total"]["value"] == 1

    corresp_prm_data["value"] = updated_corresp_prm_value
    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[corresp_prm_data], msg_event=MSG_EVENT
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
        res_after[INVENTORY_PARAMETERS_FIELD_NAME][
            str(PRM_LINK_TPRM_DATA_KAFKA["id"])
        ]
        == expected_value
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_with_prm_link_prm_update_case_2(async_elastic_session):
    """TEST With receiving PRM:UPDATED msg - if the value of prm, which is contained in some ptm_link, is updated
    (if prm_link is multiple and corresponding prm is not multiple):
    - updates value of MO parameter"""
    # change corresponding tprm data
    corresp_tprm_type = InventoryFieldValType.INT.value
    corresp_tprm_multiple = False
    corresp_tprm_data_kafka = copy.deepcopy(CORRESP_TPRM_DATA_KAFKA)
    corresp_tprm_data_kafka["val_type"] = corresp_tprm_type
    corresp_tprm_data_kafka["multiple"] = corresp_tprm_multiple

    # change corresponding prm data
    corresp_prm_value = "25"
    updated_corresp_prm_value = "34"
    expected_value = int(updated_corresp_prm_value)
    corresp_prm_data = copy.deepcopy(CORRESP_PRM_DATA)
    corresp_prm_data["value"] = corresp_prm_value

    # change current prm_link TPRM data
    prm_link_tprm_data = copy.deepcopy(PRM_LINK_TPRM_DATA_KAFKA)
    prm_link_tprm_data["multiple"] = True

    # one more corresponding prm data
    corresp_prm_data_2 = copy.deepcopy(CORRESP_PRM_DATA)
    corresp_prm_data_2["value"] = "2555"
    corresp_prm_data_2["id"] += 15

    # change current prm_link prm data
    prm_link_prm_data = copy.deepcopy(PRM_LINK_PRM_DATA)
    prm_link_prm_data["value"] = pickle.dumps(
        [corresp_prm_data_2["id"], corresp_prm_data["id"]]
    ).hex()

    # change current MO prm_link parameter data
    prm_link_mo_data = copy.deepcopy(PRM_LINK_MO_DATA)
    prm_link_mo_data[INVENTORY_PARAMETERS_FIELD_NAME][
        str(prm_link_tprm_data["id"])
    ] = [int(corresp_prm_data_2["value"]), int(corresp_prm_value)]

    for tmo_data in [CORRESP_TMO_DATA, PRM_LINK_TMO_DATA]:
        await create_default_tmo_in_elastic(
            async_elastic_session, default_tmo_data=tmo_data
        )

    for mo in [CORRESP_MO_DATA, prm_link_mo_data]:
        await create_default_mo_in_elastic(
            async_elastic_session=async_elastic_session, default_mo_data=mo
        )

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tprm_data_kafka_format=[
            corresp_tprm_data_kafka,
            prm_link_tprm_data,
        ],
        list_of_prm_data_kafka_format=[corresp_prm_data, prm_link_prm_data],
    )

    search_query = {"match": {"id": prm_link_mo_data["id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )

    assert res_before["hits"]["total"]["value"] == 1

    corresp_prm_data["value"] = updated_corresp_prm_value
    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[corresp_prm_data], msg_event=MSG_EVENT
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
    param_value = res_after[INVENTORY_PARAMETERS_FIELD_NAME][
        str(prm_link_tprm_data["id"])
    ]
    assert isinstance(param_value, list)
    assert len(param_value) == 2
    assert param_value[-1] == expected_value


@pytest.mark.asyncio(loop_scope="session")
async def test_with_prm_link_prm_update_case_3(async_elastic_session):
    """TEST With receiving PRM:UPDATED msg - if the value of prm, which is contained in some ptm_link, is updated
    (if prm_link is multiple and corresponding prm is multiple):
    - updates value of MO parameter"""
    # change corresponding tprm data
    corresp_tprm_type = InventoryFieldValType.INT.value
    corresp_tprm_multiple = True
    corresp_tprm_data_kafka = copy.deepcopy(CORRESP_TPRM_DATA_KAFKA)
    corresp_tprm_data_kafka["val_type"] = corresp_tprm_type
    corresp_tprm_data_kafka["multiple"] = corresp_tprm_multiple

    # change corresponding prm data
    corresp_prm_value = ["25", "857"]
    updated_corresp_prm_value = ["34", "2063"]
    updated_corresp_prm_value_as_hex = pickle.dumps(
        updated_corresp_prm_value
    ).hex()
    expected_value = [int(i) for i in updated_corresp_prm_value]
    corresp_prm_data = copy.deepcopy(CORRESP_PRM_DATA)
    corresp_prm_data["value"] = pickle.dumps(corresp_prm_value).hex()

    # change current prm_link TPRM data
    prm_link_tprm_data = copy.deepcopy(PRM_LINK_TPRM_DATA_KAFKA)
    prm_link_tprm_data["multiple"] = True

    # one more corresponding prm data
    corresp_prm_data_2 = copy.deepcopy(CORRESP_PRM_DATA)
    corresp_prm_data_2["value"] = "2555"
    corresp_prm_data_2["id"] += 15

    # change current prm_link prm data
    prm_link_prm_data = copy.deepcopy(PRM_LINK_PRM_DATA)
    prm_link_prm_data["value"] = pickle.dumps(
        [corresp_prm_data_2["id"], corresp_prm_data["id"]]
    ).hex()

    # change current MO prm_link parameter data
    prm_link_mo_data = copy.deepcopy(PRM_LINK_MO_DATA)
    prm_link_mo_data[INVENTORY_PARAMETERS_FIELD_NAME][
        str(prm_link_tprm_data["id"])
    ] = [int(corresp_prm_data_2["value"]), [int(i) for i in corresp_prm_value]]

    for tmo_data in [CORRESP_TMO_DATA, PRM_LINK_TMO_DATA]:
        await create_default_tmo_in_elastic(
            async_elastic_session, default_tmo_data=tmo_data
        )

    for mo in [CORRESP_MO_DATA, prm_link_mo_data]:
        await create_default_mo_in_elastic(
            async_elastic_session=async_elastic_session, default_mo_data=mo
        )

    await create_all_default_data_inventory_changes_topic(
        async_elastic_session=async_elastic_session,
        list_of_tprm_data_kafka_format=[
            corresp_tprm_data_kafka,
            prm_link_tprm_data,
        ],
        list_of_prm_data_kafka_format=[corresp_prm_data, prm_link_prm_data],
    )

    search_query = {"match": {"id": prm_link_mo_data["id"]}}

    res_before = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        size=1,
        track_total_hits=True,
    )

    assert res_before["hits"]["total"]["value"] == 1

    corresp_prm_data["value"] = updated_corresp_prm_value_as_hex
    kafka_msg = create_cleared_kafka_prm_msg(
        list_of_prm_data=[corresp_prm_data], msg_event=MSG_EVENT
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

    param_value = res_after[INVENTORY_PARAMETERS_FIELD_NAME][
        str(prm_link_tprm_data["id"])
    ]
    assert isinstance(param_value, list)
    assert len(param_value) == 2
    assert param_value[-1] == expected_value
