import datetime

import pytest
from google.protobuf import json_format

from google.protobuf.timestamp_pb2 import Timestamp

from elastic.config import INVENTORY_TPRM_INDEX_V2

from services.inventory_services.kafka.consumers.inventory_changes.utils import (
    InventoryChangesHandler,
)
from tests.kafka.consumers.topics.inventory_changes.utils import (
    create_default_tmo_in_elastic,
    create_default_tprm_in_elastic,
    create_cleared_kafka_tprm_msg,
)

datetime_value = datetime.datetime.now()
proto_timestamp = Timestamp()
proto_timestamp.FromDatetime(datetime_value)

DEFAULT_TMO_DATA = {
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

MSG_EVENT = "updated"
MSG_CLASS_NAME = "TPRM"
BEFORE_TPRM_DATA_KAFKA = {
    "id": 126354,
    "name": "delete",
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
    "created_by": "",
    "modified_by": "",
    "version": 25,
}

BEFORE_TPRM_DATA_ELASTIC = dict()
BEFORE_TPRM_DATA_ELASTIC.update(BEFORE_TPRM_DATA_KAFKA)
BEFORE_TPRM_DATA_ELASTIC["creation_date"] = json_format.MessageToDict(
    proto_timestamp
).split("Z")[0]
BEFORE_TPRM_DATA_ELASTIC["modification_date"] = json_format.MessageToDict(
    proto_timestamp
).split("Z")[0]

AFTER_TPRM_DATA_KAFKA = {
    "id": 126354,
    "name": "new name",
    "val_type": "bool",
    "required": False,
    "returnable": False,
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
    "version": 30,
}

AFTER_TPRM_DATA_ELASTIC = dict()
AFTER_TPRM_DATA_ELASTIC.update(AFTER_TPRM_DATA_KAFKA)
AFTER_TPRM_DATA_ELASTIC["creation_date"] = json_format.MessageToDict(
    proto_timestamp
).split("Z")[0]
AFTER_TPRM_DATA_ELASTIC["modification_date"] = json_format.MessageToDict(
    proto_timestamp
).split("Z")[0]

NOT_EXISTING_TPRM_DATA = {
    "id": 10000,
    "name": "delete",
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
    "created_by": "",
    "modified_by": "",
    "version": 25,
}

MSG_AS_DICT = {"objects": [AFTER_TPRM_DATA_KAFKA]}


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_tprm_case_2(async_elastic_session):
    """TEST On receiving TPRM:updated msg - if tprm does not exist - does not raise error"""

    search_query = {"match": {"id": NOT_EXISTING_TPRM_DATA["id"]}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TPRM_INDEX_V2, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 0

    kafka_msg = create_cleared_kafka_tprm_msg(
        list_of_tprm_data=[NOT_EXISTING_TPRM_DATA], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)

    await handler.process_the_message()


@pytest.mark.asyncio(loop_scope="session")
async def test_on_update_tprm_case_3(async_elastic_session):
    """TEST On receiving TPRM:updated msg - if tprm exists:
    - updates all data
    - updated record available in the search results by all attrs"""

    await create_default_tmo_in_elastic(async_elastic_session, DEFAULT_TMO_DATA)
    await create_default_tprm_in_elastic(
        default_tprm_data_kafka_format=BEFORE_TPRM_DATA_KAFKA
    )

    search_query = {"match": {"id": BEFORE_TPRM_DATA_KAFKA["id"]}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TPRM_INDEX_V2, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1

    kafka_msg = create_cleared_kafka_tprm_msg(
        list_of_tprm_data=[AFTER_TPRM_DATA_KAFKA], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)

    await handler.process_the_message()

    assert AFTER_TPRM_DATA_KAFKA["id"] == BEFORE_TPRM_DATA_KAFKA["id"]

    different_data_count = 0
    for k, v in AFTER_TPRM_DATA_ELASTIC.items():
        if v:
            if v != BEFORE_TPRM_DATA_ELASTIC[k]:
                different_data_count += 1
                search_query = {"match": {k: v}}

                search_res = await async_elastic_session.search(
                    index=INVENTORY_TPRM_INDEX_V2,
                    query=search_query,
                    track_total_hits=True,
                )

                search_res["hits"]["hits"][0]["_source"] = (
                    BEFORE_TPRM_DATA_KAFKA["id"]
                )

    # Changed to equal 2
    assert different_data_count == 2
