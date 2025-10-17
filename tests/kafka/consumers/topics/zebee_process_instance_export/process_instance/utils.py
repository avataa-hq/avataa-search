import json

from elasticsearch import AsyncElasticsearch

from elastic.config import DEFAULT_SETTING_FOR_MO_INDEXES
from elastic.query_builder_service.inventory_index.utils.index_utils import (
    get_index_name_by_tmo,
)
from indexes_mapping.inventory.mapping import INVENTORY_OBJ_INDEX_MAPPING
from kafka_config.config import KAFKA_ZEEBE_PROCESS_INSTANCE_EXPORTER_TOPIC
from tests.kafka.utils import KafkaMSGMock


async def save_default_mo_data_into_elastic_search(
    mo_data: dict, async_elastic_session: AsyncElasticsearch
):
    new_index_name = get_index_name_by_tmo(tmo_id=mo_data["tmo_id"])

    # create index
    await async_elastic_session.indices.create(
        index=new_index_name,
        mappings=INVENTORY_OBJ_INDEX_MAPPING,
        settings=DEFAULT_SETTING_FOR_MO_INDEXES,
    )
    # save new data
    await async_elastic_session.index(
        index=new_index_name, id=mo_data["id"], document=mo_data, refresh="true"
    )


def create_cleared_kafka_process_instance_msg(process_instance_data: dict):
    process_instance_data = json.dumps(process_instance_data).encode("utf-8")
    mocked_msg = KafkaMSGMock(
        msg_key="",
        msg_topic=KAFKA_ZEEBE_PROCESS_INSTANCE_EXPORTER_TOPIC,
        msg_value=process_instance_data,
    )

    return mocked_msg
