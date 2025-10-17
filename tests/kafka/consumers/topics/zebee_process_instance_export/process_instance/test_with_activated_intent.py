import pytest

from elastic.config import ALL_MO_OBJ_INDEXES_PATTERN
from indexes_mapping.inventory.zeebe_enums import ZeebeProcessInstanceFields
from services.zeebe_services.kafka.consumers.process_instance_exporter.intents import (
    CamundaOperateStatuses,
)
from services.zeebe_services.kafka.consumers.process_instance_exporter.utils import (
    ProcessInstanceChangesHandler,
)
from tests.kafka.consumers.topics.zebee_process_instance_export.process_instance.utils import (
    create_cleared_kafka_process_instance_msg,
    save_default_mo_data_into_elastic_search,
)

DEFAULT_PROCESS_INSTANCE_ID = 111111111111111

DEFAULT_MO_DATA = {
    "id": 3,
    "name": "DEFAULT MO DATA",
    "active": True,
    "tmo_id": 25,
    "latitude": 0.0,
    "longitude": 0.0,
    "p_id": 0,
    "point_a_id": 0,
    "point_b_id": 0,
    "model": "",
    "version": 0,
    "status": "",
    ZeebeProcessInstanceFields.PROCESS_INSTANCE_ID.value: DEFAULT_PROCESS_INSTANCE_ID,
}

DEFAULT_PROCESS_INSTANCE_DATA = {
    "partitionId": 1,
    "value": {
        "version": 1,
        "bpmnProcessId": "Process_0jbjknh",
        "processInstanceKey": DEFAULT_PROCESS_INSTANCE_ID,
        "elementId": "Process_0jbjknh",
        "processDefinitionKey": 2251799814072891,
        "flowScopeKey": -1,
        "bpmnElementType": "PROCESS",
        "parentProcessInstanceKey": -1,
        "parentElementInstanceKey": -1,
    },
    "key": 2251799815328692,
    "timestamp": 1708605849008,
    "valueType": "PROCESS_INSTANCE",
    "brokerVersion": "8.1.9",
    "recordType": "EVENT",
    "sourceRecordPosition": 3189711,
    "intent": "ELEMENT_ACTIVATED",
    "rejectionType": "NULL_VAL",
    "rejectionReason": "",
    "position": 3189715,
}


@pytest.mark.asyncio(loop_scope="session")
async def test_process_instance_activated_case_1(async_elastic_session):
    """TEST with receiving Kafka process instance msg with intent ELEMENT_ACTIVATED if mo with process_instance_id
    does not exist: - does not raise error"""
    mocked_kafka_msg = create_cleared_kafka_process_instance_msg(
        DEFAULT_PROCESS_INSTANCE_DATA
    )
    handler = ProcessInstanceChangesHandler(kafka_msg=mocked_kafka_msg)
    try:
        await handler.process_the_message()
    except Exception:
        assert False


@pytest.mark.skip(reason="Not corrected implementing")
async def test_process_instance_activated_case_2(async_elastic_session):
    """TEST with receiving Kafka process instance msg with intent ELEMENT_ACTIVATED if mo with process_instance_id
    exists:
    - saves new data StartDate
    - StartDate is  available in search"""
    await save_default_mo_data_into_elastic_search(
        mo_data=DEFAULT_MO_DATA, async_elastic_session=async_elastic_session
    )
    mo_pr_instance_id = DEFAULT_MO_DATA[
        ZeebeProcessInstanceFields.PROCESS_INSTANCE_ID.value
    ]
    search_query = {
        "match": {
            ZeebeProcessInstanceFields.PROCESS_INSTANCE_ID.value: mo_pr_instance_id
        }
    }
    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )

    assert search_res["hits"]["total"]["value"] == 1

    # StartDate - does not exist
    mo_item = search_res["hits"]["hits"][0]["_source"]

    assert mo_item.get(ZeebeProcessInstanceFields.START_DATE.value) is None

    mocked_kafka_msg = create_cleared_kafka_process_instance_msg(
        DEFAULT_PROCESS_INSTANCE_DATA
    )
    handler = ProcessInstanceChangesHandler(kafka_msg=mocked_kafka_msg)
    await handler.process_the_message()

    # After
    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )

    assert search_res["hits"]["total"]["value"] == 1

    mo_item = search_res["hits"]["hits"][0]["_source"]

    new_start_date = mo_item.get(ZeebeProcessInstanceFields.START_DATE.value)

    assert new_start_date is not None

    search_query = {
        "match": {ZeebeProcessInstanceFields.START_DATE.value: new_start_date}
    }

    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )

    assert search_res["hits"]["total"]["value"] == 1
    assert (
        search_res["hits"]["hits"][0]["_source"]["id"] == DEFAULT_MO_DATA["id"]
    )


@pytest.mark.skip(reason="Not corrected implementing")
async def test_process_instance_activated_case_3(async_elastic_session):
    """TEST with receiving Kafka process instance msg with intent ELEMENT_ACTIVATED if mo with process_instance_id
    exists:
    - saves new data processDefinitionKey
    - processDefinitionKey is  available in search"""
    await save_default_mo_data_into_elastic_search(
        mo_data=DEFAULT_MO_DATA, async_elastic_session=async_elastic_session
    )
    mo_pr_instance_id = DEFAULT_MO_DATA[
        ZeebeProcessInstanceFields.PROCESS_INSTANCE_ID.value
    ]
    search_query = {
        "match": {
            ZeebeProcessInstanceFields.PROCESS_INSTANCE_ID.value: mo_pr_instance_id
        }
    }
    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )

    assert search_res["hits"]["total"]["value"] == 1

    # StartDate - does not exist
    mo_item = search_res["hits"]["hits"][0]["_source"]

    assert (
        mo_item.get(ZeebeProcessInstanceFields.PROCESS_DEFINITION_KEY.value)
        is None
    )

    mocked_kafka_msg = create_cleared_kafka_process_instance_msg(
        DEFAULT_PROCESS_INSTANCE_DATA
    )
    handler = ProcessInstanceChangesHandler(kafka_msg=mocked_kafka_msg)
    await handler.process_the_message()

    # After
    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )

    assert search_res["hits"]["total"]["value"] == 1

    mo_item = search_res["hits"]["hits"][0]["_source"]

    new_value = mo_item.get(
        ZeebeProcessInstanceFields.PROCESS_DEFINITION_KEY.value
    )

    assert new_value is not None

    search_query = {
        "match": {
            ZeebeProcessInstanceFields.PROCESS_DEFINITION_KEY.value: new_value
        }
    }

    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )

    assert search_res["hits"]["total"]["value"] == 1
    assert (
        search_res["hits"]["hits"][0]["_source"]["id"] == DEFAULT_MO_DATA["id"]
    )


@pytest.mark.skip(reason="Not corrected implementing")
async def test_process_instance_activated_case_4(async_elastic_session):
    """TEST with receiving Kafka process instance msg with intent ELEMENT_ACTIVATED if mo with process_instance_id
    exists:
    - saves new data processDefinitionId
    - processDefinitionId is  available in search"""
    await save_default_mo_data_into_elastic_search(
        mo_data=DEFAULT_MO_DATA, async_elastic_session=async_elastic_session
    )
    mo_pr_instance_id = DEFAULT_MO_DATA[
        ZeebeProcessInstanceFields.PROCESS_INSTANCE_ID.value
    ]
    search_query = {
        "match": {
            ZeebeProcessInstanceFields.PROCESS_INSTANCE_ID.value: mo_pr_instance_id
        }
    }
    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )

    assert search_res["hits"]["total"]["value"] == 1

    # StartDate - does not exist
    mo_item = search_res["hits"]["hits"][0]["_source"]

    assert (
        mo_item.get(ZeebeProcessInstanceFields.PROCESS_DEFINITION_ID.value)
        is None
    )

    mocked_kafka_msg = create_cleared_kafka_process_instance_msg(
        DEFAULT_PROCESS_INSTANCE_DATA
    )
    handler = ProcessInstanceChangesHandler(kafka_msg=mocked_kafka_msg)
    await handler.process_the_message()

    # After
    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )

    assert search_res["hits"]["total"]["value"] == 1

    mo_item = search_res["hits"]["hits"][0]["_source"]

    new_value = mo_item.get(
        ZeebeProcessInstanceFields.PROCESS_DEFINITION_ID.value
    )

    assert new_value is not None

    search_query = {
        "match": {
            ZeebeProcessInstanceFields.PROCESS_DEFINITION_ID.value: new_value
        }
    }

    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )

    assert search_res["hits"]["total"]["value"] == 1
    assert (
        search_res["hits"]["hits"][0]["_source"]["id"] == DEFAULT_MO_DATA["id"]
    )


@pytest.mark.skip(reason="Not corrected implementing")
async def test_process_instance_activated_case_5(async_elastic_session):
    """TEST with receiving Kafka process instance msg with intent ELEMENT_ACTIVATED if mo with process_instance_id
    exists:
    - creates value of the state
    - state value equals to ACTIVE
    - state value is  available in search"""

    await save_default_mo_data_into_elastic_search(
        mo_data=DEFAULT_MO_DATA, async_elastic_session=async_elastic_session
    )
    mo_pr_instance_id = DEFAULT_MO_DATA[
        ZeebeProcessInstanceFields.PROCESS_INSTANCE_ID.value
    ]
    search_query = {
        "match": {
            ZeebeProcessInstanceFields.PROCESS_INSTANCE_ID.value: mo_pr_instance_id
        }
    }
    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )

    assert search_res["hits"]["total"]["value"] == 1

    # state - does not exist
    mo_item = search_res["hits"]["hits"][0]["_source"]

    assert mo_item.get(ZeebeProcessInstanceFields.STATE.value) is None

    mocked_kafka_msg = create_cleared_kafka_process_instance_msg(
        DEFAULT_PROCESS_INSTANCE_DATA
    )
    handler = ProcessInstanceChangesHandler(kafka_msg=mocked_kafka_msg)
    await handler.process_the_message()

    # After
    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )

    assert search_res["hits"]["total"]["value"] == 1

    mo_item = search_res["hits"]["hits"][0]["_source"]

    new_value = mo_item.get(ZeebeProcessInstanceFields.STATE.value)
    assert new_value == CamundaOperateStatuses.ACTIVE.value

    search_query = {
        "match": {ZeebeProcessInstanceFields.STATE.value: new_value}
    }

    search_res = await async_elastic_session.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        query=search_query,
        track_total_hits=True,
    )

    assert search_res["hits"]["total"]["value"] == 1
    assert (
        search_res["hits"]["hits"][0]["_source"]["id"] == DEFAULT_MO_DATA["id"]
    )
