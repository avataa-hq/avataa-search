from datetime import datetime
import pandas as pd
from elasticsearch import AsyncElasticsearch, ConflictError
import asyncio

from elastic.config import ALL_MO_OBJ_INDEXES_PATTERN
from indexes_mapping.inventory.zeebe_enums import ZeebeProcessInstanceFields
from services.zeebe_services.kafka.consumers.process_instance_exporter.intents import (
    CamundaOperateStatuses,
)
from services.zeebe_services.kafka.consumers.process_instance_exporter.models import (
    ClearedProcessInstanceMSGValue,
)
from dateutil.parser import parse


async def with_process_instance_terminated(
    msg_data: ClearedProcessInstanceMSGValue, elastic_client: AsyncElasticsearch
):
    """Handler for process instance canceled intent"""

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
            mo_id = str(existing_mo_data["id"])

            updated_data = dict()

            start_date = existing_mo_data.get(
                ZeebeProcessInstanceFields.START_DATE.value
            )
            if start_date:
                start = parse(start_date).replace(tzinfo=None)
                end_date = datetime.fromtimestamp(msg_data.timestamp / 1000)
                duration_in_microseconds = int(
                    pd.Timedelta(end_date - start).total_seconds() * 1000
                )
                updated_data[ZeebeProcessInstanceFields.END_DATE.value] = (
                    end_date
                )
                updated_data[ZeebeProcessInstanceFields.DURATION.value] = (
                    duration_in_microseconds
                )

            updated_data[ZeebeProcessInstanceFields.STATE.value] = (
                CamundaOperateStatuses.CANCELED.value
            )

            try:
                await elastic_client.update(
                    index=index,
                    id=mo_id,
                    doc=updated_data,
                )
            except ConflictError as e:
                print(str(e))
                continue
            break

        retry_count += 1
        if retry_count < retries:
            print(
                f"Retrying [in terminated process] in 1 second... Attempt {retry_count}"
            )
            await asyncio.sleep(1)
        else:
            print(
                "Max retry attempts reached, exiting from terminated process."
            )

        break
