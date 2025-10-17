from datetime import datetime

from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import BulkIndexError, async_bulk

from kafka.consumers.topics.test_hierarchy.init_data.config import (
    HIERARCHY_START_RANGE,
    HIERARCHY_END_RANGE,
)
from services.hierarchy_services.elastic.configs import (
    HIERARCHY_HIERARCHIES_INDEX,
)
from services.hierarchy_services.elastic.mapping import (
    HIERARCHY_PERMISSIONS_FIELD_NAME,
)

initial_data = []

for i in range(HIERARCHY_START_RANGE, HIERARCHY_END_RANGE):
    item = {
        "id": i,
        "name": "test " + str(i),
        "description": "test " + str(i) + " description",
        HIERARCHY_PERMISSIONS_FIELD_NAME: [f"test_permissions {i}"],
        "author": "Test Author",
        "change_author": "Test Author change",
        "status": "created",
        "create_empty_nodes": i % 2 == 0,
        "created": datetime(2017, 1, 1, 0, 0),
        "modified": datetime(2017, 1, 1, 0, 0),
    }
    initial_data.append(item)


async def init_hierarchy_data(async_elastic_session: AsyncElasticsearch):
    actions = []
    for initial_item in initial_data:
        actions.append(
            dict(
                _index=HIERARCHY_HIERARCHIES_INDEX,
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
