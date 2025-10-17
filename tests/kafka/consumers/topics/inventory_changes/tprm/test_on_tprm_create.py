import datetime

import pytest

from google.protobuf import json_format
from google.protobuf.timestamp_pb2 import Timestamp

from elastic.config import INVENTORY_TMO_INDEX_V2, INVENTORY_TPRM_INDEX_V2
from elastic.enum_models import InventoryFieldValType
from elastic.query_builder_service.inventory_index.utils.convert_types_utils import (
    get_corresponding_elastic_data_type,
)
from elastic.query_builder_service.inventory_index.utils.index_utils import (
    get_index_name_by_tmo,
)
from indexes_mapping.inventory.mapping import INVENTORY_PARAMETERS_FIELD_NAME
from services.inventory_services.kafka.consumers.inventory_changes.utils import (
    InventoryChangesHandler,
)
from tests.kafka.consumers.topics.inventory_changes.utils import (
    create_cleared_kafka_tprm_msg,
    create_default_tmo_in_elastic,
)

datetime_value = datetime.datetime.now()
proto_timestamp = Timestamp()
proto_timestamp.FromDatetime(datetime_value)

DEFAULT_TMO_DATA_KAFKA = {
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
    "creation_date": datetime_value,
    "modification_date": datetime_value,
    "primary": None,
    "points_constraint_by_tmo": None,
}

DEFAULT_TMO_DATA_ELASTIC = dict()
DEFAULT_TMO_DATA_ELASTIC.update(DEFAULT_TMO_DATA_KAFKA)
DEFAULT_TMO_DATA_ELASTIC["creation_date"] = json_format.MessageToDict(
    proto_timestamp
).split("Z")[0]
DEFAULT_TMO_DATA_ELASTIC["modification_date"] = json_format.MessageToDict(
    proto_timestamp
).split("Z")[0]

MSG_EVENT = "created"
MSG_CLASS_NAME = "TPRM"
NEW_TPRM_DATA_KAFKA = {
    "id": 126354,
    "name": "delete",
    "val_type": "bool",
    "required": True,
    "returnable": True,
    "tmo_id": DEFAULT_TMO_DATA_KAFKA["id"],
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

NEW_TPRM_DATA_ELASTIC = dict()
NEW_TPRM_DATA_ELASTIC.update(NEW_TPRM_DATA_KAFKA)
NEW_TPRM_DATA_ELASTIC["creation_date"] = json_format.MessageToDict(
    proto_timestamp
).split("Z")[0]
NEW_TPRM_DATA_ELASTIC["modification_date"] = json_format.MessageToDict(
    proto_timestamp
).split("Z")[0]


@pytest.mark.asyncio(loop_scope="session")
async def test_on_create_tprm_case_2(async_elastic_session):
    """TEST On receiving TPRM:created msg - if tmo and new tprm do not exist:
    - does not create record
    - does not raise error"""

    kafka_msg = create_cleared_kafka_tprm_msg(
        list_of_tprm_data=[NEW_TPRM_DATA_KAFKA], msg_event=MSG_EVENT
    )

    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    try:
        await handler.process_the_message()
    except Exception:
        assert False

    search_query = {"match_all": {}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TPRM_INDEX_V2, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] != 0


@pytest.mark.asyncio(loop_scope="session")
async def test_on_create_tprm_case_3(async_elastic_session):
    """TEST On receiving TPRM:created msg - if tmo exists and tprm does not exist:
    - creates new record
    - new record become available in the search results by all attrs."""

    await create_default_tmo_in_elastic(
        async_elastic_session=async_elastic_session,
        default_tmo_data=DEFAULT_TMO_DATA_ELASTIC,
    )

    search_query = {"match": {"id": DEFAULT_TMO_DATA_KAFKA["id"]}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TMO_INDEX_V2, query=search_query, track_total_hits=True
    )
    # tmo exists
    assert search_res["hits"]["total"]["value"] == 1

    # tprm does not exist
    search_query = {"match_all": {}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TPRM_INDEX_V2, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 0

    kafka_msg = create_cleared_kafka_tprm_msg(
        list_of_tprm_data=[NEW_TPRM_DATA_KAFKA], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # tprm was created
    search_query = {"match_all": {}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TPRM_INDEX_V2, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1

    assert (
        search_res["hits"]["hits"][0]["_source"]["id"]
        == NEW_TPRM_DATA_KAFKA["id"]
    )

    # tprm attrs available in search

    not_null_attrs = 0
    for k, v in NEW_TPRM_DATA_ELASTIC.items():
        if v:
            not_null_attrs += 1
            search_query = {"match": {k: v}}
            search_res = await async_elastic_session.search(
                index=INVENTORY_TPRM_INDEX_V2,
                query=search_query,
                track_total_hits=True,
            )
            assert search_res["hits"]["total"]["value"] == 1
            assert (
                search_res["hits"]["hits"][0]["_source"]["id"]
                == NEW_TPRM_DATA_KAFKA["id"]
            )
    assert not_null_attrs > 2


@pytest.mark.asyncio(loop_scope="session")
async def test_on_create_tprm_case_4(async_elastic_session):
    """TEST On receiving TPRM:created msg - if tmo and tprm exist:
    - does not raise error
    - record is available in the search results by all attrs."""

    await create_default_tmo_in_elastic(
        async_elastic_session=async_elastic_session,
        default_tmo_data=DEFAULT_TMO_DATA_ELASTIC,
    )

    search_query = {"match": {"id": DEFAULT_TMO_DATA_KAFKA["id"]}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TMO_INDEX_V2, query=search_query, track_total_hits=True
    )
    # tmo exists
    assert search_res["hits"]["total"]["value"] == 1

    kafka_msg = create_cleared_kafka_tprm_msg(
        list_of_tprm_data=[NEW_TPRM_DATA_KAFKA], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # tprm was created
    search_query = {"match_all": {}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TPRM_INDEX_V2, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 1

    assert (
        search_res["hits"]["hits"][0]["_source"]["id"]
        == NEW_TPRM_DATA_KAFKA["id"]
    )

    # tprm same msg
    kafka_msg = create_cleared_kafka_tprm_msg(
        list_of_tprm_data=[NEW_TPRM_DATA_KAFKA], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    try:
        await handler.process_the_message()
    except Exception:
        assert False


@pytest.mark.asyncio(loop_scope="session")
async def test_on_create_tprm_case_5(async_elastic_session):
    """TEST On receiving TPRM:created msg - if tmo exists and tprm does not exist:
    - creates new record
    - updates mapping for InventoryFieldValType.STR.value ."""

    val_type = InventoryFieldValType.STR.value
    await create_default_tmo_in_elastic(
        async_elastic_session=async_elastic_session,
        default_tmo_data=DEFAULT_TMO_DATA_ELASTIC,
    )
    tprm_data = dict()
    tprm_data.update(NEW_TPRM_DATA_KAFKA)
    tprm_data["val_type"] = val_type
    search_query = {"match": {"id": tprm_data["tmo_id"]}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TMO_INDEX_V2, query=search_query, track_total_hits=True
    )
    # tmo exists
    assert search_res["hits"]["total"]["value"] == 1

    # tprm does not exist
    search_query = {"match_all": {}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TPRM_INDEX_V2, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 0

    # create new tprm
    kafka_msg = create_cleared_kafka_tprm_msg(
        list_of_tprm_data=[tprm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # mapping was updated
    index_name = get_index_name_by_tmo(DEFAULT_TMO_DATA_KAFKA["id"])
    index_info = await async_elastic_session.indices.get(index=index_name)
    new_mapping = index_info[index_name]["mappings"]["properties"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ]
    new_mapping = new_mapping["properties"].get(str(tprm_data["id"]))
    assert new_mapping
    assert new_mapping["type"] == get_corresponding_elastic_data_type(val_type)


@pytest.mark.asyncio(loop_scope="session")
async def test_on_create_tprm_case_6(async_elastic_session):
    """TEST On receiving TPRM:created msg - if tmo exists and tprm does not exist:
    - creates new record
    - updates mapping for InventoryFieldValType.DATE.value ."""

    val_type = InventoryFieldValType.DATE.value
    await create_default_tmo_in_elastic(
        async_elastic_session=async_elastic_session,
        default_tmo_data=DEFAULT_TMO_DATA_ELASTIC,
    )
    tprm_data = dict()
    tprm_data.update(NEW_TPRM_DATA_KAFKA)
    tprm_data["val_type"] = val_type
    search_query = {"match": {"id": tprm_data["tmo_id"]}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TMO_INDEX_V2, query=search_query, track_total_hits=True
    )
    # tmo exists
    assert search_res["hits"]["total"]["value"] == 1

    # tprm does not exist
    search_query = {"match_all": {}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TPRM_INDEX_V2, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 0

    # create new tprm
    kafka_msg = create_cleared_kafka_tprm_msg(
        list_of_tprm_data=[tprm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # mapping was updated
    index_name = get_index_name_by_tmo(DEFAULT_TMO_DATA_KAFKA["id"])
    index_info = await async_elastic_session.indices.get(index=index_name)
    new_mapping = index_info[index_name]["mappings"]["properties"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ]
    new_mapping = new_mapping["properties"].get(str(tprm_data["id"]))
    assert new_mapping
    assert new_mapping["type"] == get_corresponding_elastic_data_type(val_type)


@pytest.mark.asyncio(loop_scope="session")
async def test_on_create_tprm_case_7(async_elastic_session):
    """TEST On receiving TPRM:created msg - if tmo exists and tprm does not exist:
    - creates new record
    - updates mapping for InventoryFieldValType.DATETIME.value ."""

    val_type = InventoryFieldValType.DATETIME.value
    await create_default_tmo_in_elastic(
        async_elastic_session=async_elastic_session,
        default_tmo_data=DEFAULT_TMO_DATA_ELASTIC,
    )
    tprm_data = dict()
    tprm_data.update(NEW_TPRM_DATA_KAFKA)
    tprm_data["val_type"] = val_type
    search_query = {"match": {"id": tprm_data["tmo_id"]}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TMO_INDEX_V2, query=search_query, track_total_hits=True
    )
    # tmo exists
    assert search_res["hits"]["total"]["value"] == 1

    # tprm does not exist
    search_query = {"match_all": {}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TPRM_INDEX_V2, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 0

    # create new tprm
    kafka_msg = create_cleared_kafka_tprm_msg(
        list_of_tprm_data=[tprm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # mapping was updated
    index_name = get_index_name_by_tmo(DEFAULT_TMO_DATA_KAFKA["id"])
    index_info = await async_elastic_session.indices.get(index=index_name)
    new_mapping = index_info[index_name]["mappings"]["properties"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ]
    new_mapping = new_mapping["properties"].get(str(tprm_data["id"]))
    assert new_mapping
    assert new_mapping["type"] == get_corresponding_elastic_data_type(val_type)


@pytest.mark.asyncio(loop_scope="session")
async def test_on_create_tprm_case_8(async_elastic_session):
    """TEST On receiving TPRM:created msg - if tmo exists and tprm does not exist:
    - creates new record
    - updates mapping for InventoryFieldValType.FLOAT.value ."""

    val_type = InventoryFieldValType.FLOAT.value
    await create_default_tmo_in_elastic(
        async_elastic_session=async_elastic_session,
        default_tmo_data=DEFAULT_TMO_DATA_ELASTIC,
    )
    tprm_data = dict()
    tprm_data.update(NEW_TPRM_DATA_KAFKA)
    tprm_data["val_type"] = val_type
    search_query = {"match": {"id": tprm_data["tmo_id"]}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TMO_INDEX_V2, query=search_query, track_total_hits=True
    )
    # tmo exists
    assert search_res["hits"]["total"]["value"] == 1

    # tprm does not exist
    search_query = {"match_all": {}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TPRM_INDEX_V2, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 0

    # create new tprm
    kafka_msg = create_cleared_kafka_tprm_msg(
        list_of_tprm_data=[tprm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # mapping was updated
    index_name = get_index_name_by_tmo(DEFAULT_TMO_DATA_KAFKA["id"])
    index_info = await async_elastic_session.indices.get(index=index_name)
    new_mapping = index_info[index_name]["mappings"]["properties"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ]
    new_mapping = new_mapping["properties"].get(str(tprm_data["id"]))
    assert new_mapping
    assert new_mapping["type"] == get_corresponding_elastic_data_type(val_type)


@pytest.mark.asyncio(loop_scope="session")
async def test_on_create_tprm_case_9(async_elastic_session):
    """TEST On receiving TPRM:created msg - if tmo exists and tprm does not exist:
    - creates new record
    - updates mapping for InventoryFieldValType.FLOAT.value ."""

    val_type = InventoryFieldValType.INT.value
    await create_default_tmo_in_elastic(
        async_elastic_session=async_elastic_session,
        default_tmo_data=DEFAULT_TMO_DATA_ELASTIC,
    )
    tprm_data = dict()
    tprm_data.update(NEW_TPRM_DATA_KAFKA)
    tprm_data["val_type"] = val_type
    search_query = {"match": {"id": tprm_data["tmo_id"]}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TMO_INDEX_V2, query=search_query, track_total_hits=True
    )
    # tmo exists
    assert search_res["hits"]["total"]["value"] == 1

    # tprm does not exist
    search_query = {"match_all": {}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TPRM_INDEX_V2, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 0

    # create new tprm
    kafka_msg = create_cleared_kafka_tprm_msg(
        list_of_tprm_data=[tprm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # mapping was updated
    index_name = get_index_name_by_tmo(DEFAULT_TMO_DATA_KAFKA["id"])
    index_info = await async_elastic_session.indices.get(index=index_name)
    new_mapping = index_info[index_name]["mappings"]["properties"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ]
    new_mapping = new_mapping["properties"].get(str(tprm_data["id"]))
    assert new_mapping
    assert new_mapping["type"] == get_corresponding_elastic_data_type(val_type)


@pytest.mark.asyncio(loop_scope="session")
async def test_on_create_tprm_case_10(async_elastic_session):
    """TEST On receiving TPRM:created msg - if tmo exists and tprm does not exist:
    - creates new record
    - updates mapping for InventoryFieldValType.BOOL.value ."""

    val_type = InventoryFieldValType.BOOL.value
    await create_default_tmo_in_elastic(
        async_elastic_session=async_elastic_session,
        default_tmo_data=DEFAULT_TMO_DATA_ELASTIC,
    )
    tprm_data = dict()
    tprm_data.update(NEW_TPRM_DATA_KAFKA)
    tprm_data["val_type"] = val_type
    search_query = {"match": {"id": tprm_data["tmo_id"]}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TMO_INDEX_V2, query=search_query, track_total_hits=True
    )
    # tmo exists
    assert search_res["hits"]["total"]["value"] == 1

    # tprm does not exist
    search_query = {"match_all": {}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TPRM_INDEX_V2, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 0

    # create new tprm
    kafka_msg = create_cleared_kafka_tprm_msg(
        list_of_tprm_data=[tprm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # mapping was updated
    index_name = get_index_name_by_tmo(DEFAULT_TMO_DATA_KAFKA["id"])
    index_info = await async_elastic_session.indices.get(index=index_name)
    new_mapping = index_info[index_name]["mappings"]["properties"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ]
    new_mapping = new_mapping["properties"].get(str(tprm_data["id"]))
    assert new_mapping
    assert new_mapping["type"] == get_corresponding_elastic_data_type(val_type)


@pytest.mark.asyncio(loop_scope="session")
async def test_on_create_tprm_case_11(async_elastic_session):
    """TEST On receiving TPRM:created msg - if tmo exists and tprm does not exist:
    - creates new record
    - updates mapping for InventoryFieldValType.MO_LINK.value ."""

    val_type = InventoryFieldValType.MO_LINK.value
    await create_default_tmo_in_elastic(
        async_elastic_session=async_elastic_session,
        default_tmo_data=DEFAULT_TMO_DATA_ELASTIC,
    )
    tprm_data = dict()
    tprm_data.update(NEW_TPRM_DATA_KAFKA)
    tprm_data["val_type"] = val_type
    search_query = {"match": {"id": tprm_data["tmo_id"]}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TMO_INDEX_V2, query=search_query, track_total_hits=True
    )
    # tmo exists
    assert search_res["hits"]["total"]["value"] == 1

    # tprm does not exist
    search_query = {"match_all": {}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TPRM_INDEX_V2, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 0

    # create new tprm
    kafka_msg = create_cleared_kafka_tprm_msg(
        list_of_tprm_data=[tprm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # mapping was updated
    index_name = get_index_name_by_tmo(DEFAULT_TMO_DATA_KAFKA["id"])
    index_info = await async_elastic_session.indices.get(index=index_name)
    new_mapping = index_info[index_name]["mappings"]["properties"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ]
    new_mapping = new_mapping["properties"].get(str(tprm_data["id"]))
    assert new_mapping
    assert new_mapping["type"] == get_corresponding_elastic_data_type(val_type)


@pytest.mark.asyncio(loop_scope="session")
async def test_on_create_tprm_case_12(async_elastic_session):
    """TEST On receiving TPRM:created msg - if tmo exists and tprm does not exist:
    - creates new record
    - updates mapping for InventoryFieldValType.PRM_LINK.value ."""

    val_type = InventoryFieldValType.PRM_LINK.value
    await create_default_tmo_in_elastic(
        async_elastic_session=async_elastic_session,
        default_tmo_data=DEFAULT_TMO_DATA_ELASTIC,
    )
    tprm_data = dict()
    tprm_data.update(NEW_TPRM_DATA_KAFKA)
    tprm_data["val_type"] = val_type
    search_query = {"match": {"id": tprm_data["tmo_id"]}}

    prm_link_tprm = dict()
    prm_link_tprm.update(NEW_TPRM_DATA_KAFKA)
    prm_link_tprm["val_type"] = InventoryFieldValType.STR.value
    prm_link_tprm["id"] += 10

    # Create prm_link_tprm
    kafka_msg = create_cleared_kafka_tprm_msg(
        list_of_tprm_data=[prm_link_tprm], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    tprm_data["constraint"] = str(prm_link_tprm["id"])

    search_res = await async_elastic_session.search(
        index=INVENTORY_TMO_INDEX_V2, query=search_query, track_total_hits=True
    )
    # tmo exists
    assert search_res["hits"]["total"]["value"] == 1

    # tprm does not exist
    search_query = {"match": {"id": tprm_data["id"]}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TPRM_INDEX_V2, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 0

    # create new tprm

    kafka_msg = create_cleared_kafka_tprm_msg(
        list_of_tprm_data=[tprm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # mapping was updated
    index_name = get_index_name_by_tmo(DEFAULT_TMO_DATA_KAFKA["id"])
    index_info = await async_elastic_session.indices.get(index=index_name)
    new_mapping = index_info[index_name]["mappings"]["properties"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ]
    new_mapping = new_mapping["properties"].get(str(tprm_data["id"]))
    assert new_mapping
    assert new_mapping["type"] == get_corresponding_elastic_data_type(
        prm_link_tprm["val_type"]
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_on_create_tprm_case_13(async_elastic_session):
    """TEST On receiving TPRM:created msg - if tmo exists and tprm does not exist:
    - creates new record
    - updates mapping for InventoryFieldValType.USER_LINK.value ."""

    val_type = InventoryFieldValType.USER_LINK.value
    await create_default_tmo_in_elastic(
        async_elastic_session=async_elastic_session,
        default_tmo_data=DEFAULT_TMO_DATA_ELASTIC,
    )
    tprm_data = dict()
    tprm_data.update(NEW_TPRM_DATA_KAFKA)
    tprm_data["val_type"] = val_type
    search_query = {"match": {"id": tprm_data["tmo_id"]}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TMO_INDEX_V2, query=search_query, track_total_hits=True
    )
    # tmo exists
    assert search_res["hits"]["total"]["value"] == 1

    # tprm does not exist
    search_query = {"match_all": {}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TPRM_INDEX_V2, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 0

    # create new tprm
    kafka_msg = create_cleared_kafka_tprm_msg(
        list_of_tprm_data=[tprm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # mapping was updated
    index_name = get_index_name_by_tmo(DEFAULT_TMO_DATA_KAFKA["id"])
    index_info = await async_elastic_session.indices.get(index=index_name)
    new_mapping = index_info[index_name]["mappings"]["properties"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ]
    new_mapping = new_mapping["properties"].get(str(tprm_data["id"]))
    assert new_mapping
    assert new_mapping["type"] == get_corresponding_elastic_data_type(val_type)


@pytest.mark.asyncio(loop_scope="session")
async def test_on_create_tprm_case_14(async_elastic_session):
    """TEST On receiving TPRM:created msg - if tmo exists and tprm does not exist:
    - creates new record
    - updates mapping for InventoryFieldValType.FORMULA.value ."""

    val_type = InventoryFieldValType.FORMULA.value
    await create_default_tmo_in_elastic(
        async_elastic_session=async_elastic_session,
        default_tmo_data=DEFAULT_TMO_DATA_ELASTIC,
    )
    tprm_data = dict()
    tprm_data.update(NEW_TPRM_DATA_KAFKA)
    tprm_data["val_type"] = val_type
    search_query = {"match": {"id": tprm_data["tmo_id"]}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TMO_INDEX_V2, query=search_query, track_total_hits=True
    )
    # tmo exists
    assert search_res["hits"]["total"]["value"] == 1

    # tprm does not exist
    search_query = {"match_all": {}}
    search_res = await async_elastic_session.search(
        index=INVENTORY_TPRM_INDEX_V2, query=search_query, track_total_hits=True
    )
    assert search_res["hits"]["total"]["value"] == 0

    # create new tprm
    kafka_msg = create_cleared_kafka_tprm_msg(
        list_of_tprm_data=[tprm_data], msg_event=MSG_EVENT
    )
    handler = InventoryChangesHandler(kafka_msg=kafka_msg)
    await handler.process_the_message()

    # mapping was updated
    index_name = get_index_name_by_tmo(DEFAULT_TMO_DATA_KAFKA["id"])
    index_info = await async_elastic_session.indices.get(index=index_name)
    new_mapping = index_info[index_name]["mappings"]["properties"][
        INVENTORY_PARAMETERS_FIELD_NAME
    ]
    new_mapping = new_mapping["properties"].get(str(tprm_data["id"]))
    assert new_mapping
    assert new_mapping["type"] == get_corresponding_elastic_data_type(val_type)
