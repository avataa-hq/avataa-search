from datetime import datetime

from elasticsearch import AsyncElasticsearch
from elasticsearch._async.helpers import async_bulk
from elasticsearch.helpers import BulkIndexError

from kafka.consumers.topics.test_hierarchy.init_data.config import (
    HIERARCHY_START_RANGE,
    HIERARCHY_END_RANGE,
    LEVEL_START_RANGE,
    LEVEL_END_RANGE,
)
from services.hierarchy_services.elastic.configs import HIERARCHY_LEVELS_INDEX

initial_data = []


for i in range(HIERARCHY_START_RANGE, HIERARCHY_END_RANGE):
    for j in range(LEVEL_START_RANGE, LEVEL_END_RANGE):
        index = i * 10 ** len(str(j)) + j
        parent_id = str(index - 1) if j != LEVEL_START_RANGE else None
        item = {
            "id": index,
            "parent_id": parent_id,
            "hierarchy_id": i,
            "level": j % 10,
            "name": "Test level " + str(index),
            "object_type_id": index,
            "additional_params_id": 0,
            "latitude_id": 0,
            "longitude_id": 0,
            "author": "Test author",
            "created": datetime(2017, 1, 1, 0, 0).isoformat(),
            "show_without_children": i % 2 == 0,
            "key_attrs": [],
            "description": "Test level description",
            "is_virtual": False,
        }
        initial_data.append(item)


async def init_level_data(async_elastic_session: AsyncElasticsearch):
    actions = []
    for initial_item in initial_data:
        actions.append(
            dict(
                _index=HIERARCHY_LEVELS_INDEX,
                _op_type="index",
                _id=initial_item["id"],
                _source=initial_item,
            )
        )
    try:
        await async_bulk(
            client=async_elastic_session, refresh="true", actions=actions
        )
    except BulkIndexError as e:
        print(*actions, sep="\n")
        print(*e.errors, sep="\n")
        raise e
