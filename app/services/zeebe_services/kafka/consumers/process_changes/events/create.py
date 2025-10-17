import asyncio

from elasticsearch import AsyncElasticsearch, ConflictError

from elastic.config import INVENTORY_OBJ_INDEX_PREFIX
from indexes_mapping.inventory.zeebe_enums import ZeebeProcessInstanceFields


async def with_create_process(
    message_as_dict, elastic_client: AsyncElasticsearch
):
    """Handler for process instance creation"""
    mo_id = message_as_dict["mo_id"]
    tmo_id = message_as_dict["tmo_id"]

    search_query = {"term": {"id": mo_id}}

    retries = 20
    retry_count = 0

    index = f"{INVENTORY_OBJ_INDEX_PREFIX}{tmo_id}_index"
    while retry_count < retries:
        existing_mo_data = await elastic_client.search(
            index=index, query=search_query, size=1
        )

        existing_mo_data = existing_mo_data["hits"]["hits"]

        if existing_mo_data:
            data_to_update = {
                ZeebeProcessInstanceFields.PROCESS_INSTANCE_ID.value: message_as_dict[
                    "process_instance_key"
                ]
            }

            try:
                await elastic_client.update(
                    index=index,
                    id=str(mo_id),
                    doc=data_to_update,
                )

            except ConflictError as e:
                print(str(e))
                continue

            break

        retry_count += 1
        if retry_count < retries:
            print(
                f"Retrying [in process.changes] in 1 second... Attempt {retry_count}"
            )
            await asyncio.sleep(0.5)
