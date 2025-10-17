from elasticsearch import AsyncElasticsearch
from elasticsearch._async.helpers import async_bulk
from elasticsearch.helpers import BulkIndexError

from kafka.consumers.topics.test_hierarchy.init_data.config import (
    NODE_DATA_START_RANGE,
    HIERARCHY_START_RANGE,
    HIERARCHY_END_RANGE,
    LEVEL_START_RANGE,
    LEVEL_END_RANGE,
)
from services.hierarchy_services.elastic.configs import HIERARCHY_OBJ_INDEX

initial_data = []


node_id_counter = NODE_DATA_START_RANGE
for i in range(HIERARCHY_START_RANGE, HIERARCHY_END_RANGE):
    for j in range(LEVEL_START_RANGE, LEVEL_END_RANGE):
        index = i * 10 ** len(str(j)) + j
        parent_id = str(index - 1) if j != LEVEL_START_RANGE else 0
        item = {
            "id": f"{index}{node_id_counter}",
            "hierarchy_id": i,
            "parent_id": f"{index}{node_id_counter}",
            "key": f"Test key {index}{node_id_counter}",
            "object_id": node_id_counter,
            "level": j % 10,
            "object_type_id": index,
            "level_id": index,
            "active": True,
            "path": "1/2/3/",
            "latitude": 0,
            "longitude": 0,
            "child_count": 123,
            "key_is_empty": False,
        }
        initial_data.append(item)
        node_id_counter += 1


async def init_obj_data(async_elastic_session: AsyncElasticsearch):
    actions = []
    for initial_item in initial_data:
        actions.append(
            dict(
                _index=HIERARCHY_OBJ_INDEX,
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
