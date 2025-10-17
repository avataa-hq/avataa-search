from elasticsearch import AsyncElasticsearch
from elasticsearch._async.helpers import async_bulk
from elasticsearch.helpers import BulkIndexError

from kafka.consumers.topics.test_hierarchy.init_data.config import (
    HIERARCHY_START_RANGE,
    HIERARCHY_END_RANGE,
    LEVEL_START_RANGE,
    LEVEL_END_RANGE,
    NODE_DATA_START_RANGE,
)
from services.hierarchy_services.elastic.configs import (
    HIERARCHY_NODE_DATA_INDEX,
)

initial_data = []


node_id_counter = NODE_DATA_START_RANGE
for i in range(HIERARCHY_START_RANGE, HIERARCHY_END_RANGE):
    for j in range(LEVEL_START_RANGE, LEVEL_END_RANGE):
        index = i * 10 ** len(str(j)) + j
        parent_id = str(index - 1) if j != LEVEL_START_RANGE else 0
        item = {
            "id": node_id_counter,
            "level_id": index,
            "node_id": f"{index}{node_id_counter}",
            "mo_id": node_id_counter,
            "mo_name": f"Test mo name {node_id_counter}",
            "mo_status": "ACTIVE",
            "mo_tmo_id": index,
            "mo_active": True,
            "unfolded_key": {},
            "mo_latitude": 0,
            "mo_longitude": 0,
            "mo_p_id": 0,
        }
        initial_data.append(item)
        node_id_counter += 1


async def init_node_data(async_elastic_session: AsyncElasticsearch):
    actions = []
    for initial_item in initial_data:
        actions.append(
            dict(
                _index=HIERARCHY_NODE_DATA_INDEX,
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
