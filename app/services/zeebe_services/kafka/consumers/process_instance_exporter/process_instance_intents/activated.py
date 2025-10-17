import asyncio
from datetime import datetime

from elasticsearch import AsyncElasticsearch, ConflictError

from elastic.config import ALL_MO_OBJ_INDEXES_PATTERN
from indexes_mapping.inventory.zeebe_enums import ZeebeProcessInstanceFields
from services.zeebe_services.kafka.consumers.process_instance_exporter.intents import (
    CamundaOperateStatuses,
)
from services.zeebe_services.kafka.consumers.process_instance_exporter.models import (
    ClearedProcessInstanceMSGValue,
)


async def with_process_instance_activated(
    msg_data: ClearedProcessInstanceMSGValue, elastic_client: AsyncElasticsearch
):
    """Handler for process instance activated intent"""

    search_query = {
        "term": {
            ZeebeProcessInstanceFields.PROCESS_INSTANCE_ID.value: msg_data.process_instance_id
        }
    }

    retries = 20
    retry_count = 0
    while retry_count < retries:
        existing_mo_data = await elastic_client.search(
            index=ALL_MO_OBJ_INDEXES_PATTERN,
            query=search_query,
            size=1,
            track_total_hits=False,
        )

        existing_mo_data = existing_mo_data["hits"]["hits"]

        if existing_mo_data:
            existing_mo_data = existing_mo_data[0]
            index = existing_mo_data["_index"]
            existing_mo_data = existing_mo_data["_source"]
            object_id = str(existing_mo_data["id"])

            updated_data = dict()

            updated_data[
                ZeebeProcessInstanceFields.PROCESS_DEFINITION_KEY.value
            ] = msg_data.process_definition_key
            updated_data[
                ZeebeProcessInstanceFields.PROCESS_DEFINITION_ID.value
            ] = msg_data.process_definition_id

            dt = datetime.fromtimestamp(msg_data.timestamp / 1000)
            updated_data[ZeebeProcessInstanceFields.START_DATE.value] = dt
            updated_data[ZeebeProcessInstanceFields.STATE.value] = (
                CamundaOperateStatuses.ACTIVE.value
            )

            try:
                await elastic_client.update(
                    index=index, id=object_id, doc=updated_data
                )
            except ConflictError as e:
                print(str(e))
                continue
            break

        retry_count += 1
        if retry_count < retries:
            print(
                f"Retrying [in activated process] in 1 second... Attempt {retry_count}"
            )
            await asyncio.sleep(0.1)
        else:
            print("Max retry attempts reached, exiting from completed process.")

        break
