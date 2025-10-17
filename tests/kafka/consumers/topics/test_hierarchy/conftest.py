from datetime import datetime

from dateutil.parser import isoparse
from google.protobuf import timestamp_pb2
from pytest import fixture

from elasticsearch import AsyncElasticsearch

from kafka.consumers.topics.test_hierarchy.init_data.config import (
    HIERARCHY_START_RANGE,
    LEVEL_START_RANGE,
)
from kafka.consumers.topics.test_hierarchy.init_data.init_hierarchy import (
    init_hierarchy_data,
)
from kafka.consumers.topics.test_hierarchy.init_data.init_level_data import (
    init_level_data,
)
from kafka.consumers.topics.test_hierarchy.init_data.init_node_data import (
    init_node_data,
)
from kafka.consumers.topics.test_hierarchy.init_data.init_obj_data import (
    init_obj_data,
)
from services.hierarchy_services.elastic.configs import (
    HIERARCHY_HIERARCHIES_INDEX,
    HIERARCHY_HIERARCHIES_INDEX_SETTINGS,
    HIERARCHY_LEVELS_INDEX,
    HIERARCHY_LEVELS_INDEX_SETTINGS,
    HIERARCHY_OBJ_INDEX,
    HIERARCHY_OBJ_INDEX_SETTINGS,
    HIERARCHY_NODE_DATA_INDEX,
    HIERARCHY_NODE_DATA_INDEX_SETTINGS,
)
from services.hierarchy_services.elastic.mapping import (
    HIERARCHY_HIERARCHIES_INDEX_MAPPING,
    HIERARCHY_LEVEL_INDEX_MAPPING,
    HIERARCHY_OBJ_INDEX_MAPPING,
    HIERARCHY_NODE_DATA_INDEX_MAPPING,
)
from services.hierarchy_services.kafka.consumers.changes_topic.protobuf.hierarchy_producer_msg_pb2 import (
    HierarchyMessageSchema,
    ListHierarchy,
    LevelMessageSchema,
    ListLevel,
    ListNode,
    NodeMessageSchema,
    NodeDataMessageSchema,
    ListNodeData,
    HierarchyPermissionMessageSchema,
    ListHierarchyPermission,
)


async def init_hierarchy_indexes(async_elastic_session: AsyncElasticsearch):
    main_indexes_and_configs = {
        HIERARCHY_HIERARCHIES_INDEX: {
            "settings": HIERARCHY_HIERARCHIES_INDEX_SETTINGS,
            "mappings": HIERARCHY_HIERARCHIES_INDEX_MAPPING,
        },
        HIERARCHY_LEVELS_INDEX: {
            "settings": HIERARCHY_LEVELS_INDEX_SETTINGS,
            "mappings": HIERARCHY_LEVEL_INDEX_MAPPING,
        },
        HIERARCHY_OBJ_INDEX: {
            "settings": HIERARCHY_OBJ_INDEX_SETTINGS,
            "mappings": HIERARCHY_OBJ_INDEX_MAPPING,
        },
        HIERARCHY_NODE_DATA_INDEX: {
            "settings": HIERARCHY_NODE_DATA_INDEX_SETTINGS,
            "mappings": HIERARCHY_NODE_DATA_INDEX_MAPPING,
        },
    }

    for index_name, conf_data in main_indexes_and_configs.items():
        await async_elastic_session.indices.delete(
            index=index_name, ignore_unavailable=True
        )
        await async_elastic_session.indices.create(
            index=index_name,
            mappings=conf_data["mappings"],
            settings=conf_data["settings"],
        )


async def initial_hierarchy_data(async_elastic_session: AsyncElasticsearch):
    await init_hierarchy_data(async_elastic_session=async_elastic_session)


@fixture(scope="function", autouse=True)
async def init_hierarchy(async_elastic_session: AsyncElasticsearch):
    await init_hierarchy_indexes(async_elastic_session=async_elastic_session)
    await initial_hierarchy_data(async_elastic_session=async_elastic_session)
    await init_level_data(async_elastic_session=async_elastic_session)
    await init_node_data(async_elastic_session=async_elastic_session)
    await init_obj_data(async_elastic_session=async_elastic_session)


@fixture(scope="function")
def new_hierarchy_msg() -> ListHierarchy:
    timestamp = timestamp_pb2.Timestamp()  # noqa
    timestamp.FromDatetime(datetime(2020, 1, 1))

    msg1 = HierarchyMessageSchema(
        id=1,
        name="Test 1",
        description="Test 1 description",
        author="Test 1 author",
        change_author="Test 1 change author",
        status="Created",
        create_empty_nodes=True,
        created=timestamp,
        modified=timestamp,
    )
    msgs = ListHierarchy(objects=[msg1])
    return msgs


@fixture(scope="function")
async def existing_hierarchy_msg(async_elastic_session: AsyncElasticsearch):
    query = {"match_all": {}}
    response = await async_elastic_session.search(
        query=query, index=HIERARCHY_HIERARCHIES_INDEX, size=1
    )
    assert response["hits"]["hits"]

    msgs_list = []
    for msg_dict in response["hits"]["hits"]:
        created = msg_dict["_source"].get("created")
        if created:
            created_timestamp = isoparse(created)
            created_timestamp_proto = timestamp_pb2.Timestamp()  # noqa
            created_timestamp_proto.FromDatetime(created_timestamp)
            msg_dict["_source"]["created"] = created_timestamp_proto

        modified = msg_dict["_source"].get("modified")
        if modified:
            modified_timestamp = isoparse(modified)
            modified_timestamp_proto = timestamp_pb2.Timestamp()  # noqa
            modified_timestamp_proto.FromDatetime(modified_timestamp)
            msg_dict["_source"]["modified"] = modified_timestamp_proto
        msg_dict["_source"].pop("permissions", None)
        msg = HierarchyMessageSchema(**msg_dict["_source"])
        msgs_list.append(msg)

    msgs = ListHierarchy(objects=msgs_list)
    return msgs


@fixture(scope="function")
def new_level_msg() -> ListLevel:
    timestamp = timestamp_pb2.Timestamp()  # noqa
    timestamp.FromDatetime(datetime(2020, 1, 1))

    msg1 = LevelMessageSchema(
        id=100500,
        name="Test 1",
        description="Test 1 description",
        level=100500,
        hierarchy_id=HIERARCHY_START_RANGE,
        parent_id=None,
        object_type_id=100500,
        is_virtual=False,
        param_type_id=100500,
        additional_params_id=0,
        latitude_id=0,
        longitude_id=0,
        author="Test Author",
        change_author="Test Author",
        created=timestamp,
        modified=timestamp,
        show_without_children=True,
        key_attrs=[],
    )
    msgs = ListLevel(objects=[msg1])
    return msgs


@fixture(scope="function")
async def existing_level_msg(async_elastic_session: AsyncElasticsearch):
    query = {"match_all": {}}
    response = await async_elastic_session.search(
        query=query, index=HIERARCHY_LEVELS_INDEX, size=1
    )
    assert response["hits"]["hits"]

    msgs_list = []
    for msg_dict in response["hits"]["hits"]:
        created = msg_dict["_source"].get("created")
        if created:
            created_timestamp = isoparse(created)
            created_timestamp_proto = timestamp_pb2.Timestamp()  # noqa
            created_timestamp_proto.FromDatetime(created_timestamp)
            msg_dict["_source"]["created"] = created_timestamp_proto

        modified = msg_dict["_source"].get("modified")
        if modified:
            modified_timestamp = isoparse(modified)
            modified_timestamp_proto = timestamp_pb2.Timestamp()  # noqa
            modified_timestamp_proto.FromDatetime(modified_timestamp)
            msg_dict["_source"]["modified"] = modified_timestamp_proto

        msg = LevelMessageSchema(**msg_dict["_source"])
        msgs_list.append(msg)

    msgs = ListLevel(objects=msgs_list)
    return msgs


@fixture(scope="function")
def new_obj_msg() -> ListNode:
    msg1 = NodeMessageSchema(
        id="1",
        hierarchy_id=1,
        parent_id=None,
        key="test key",
        object_id=1,
        level=1,
        object_type_id=1,
        level_id=1,
        active=True,
        path="1/2/3/",
        latitude=0,
        longitude=0,
        child_count=123,
        key_is_empty=False,
    )
    msgs = ListNode(objects=[msg1])
    return msgs


@fixture(scope="function")
async def existing_obj_msg(async_elastic_session: AsyncElasticsearch):
    query = {"match_all": {}}
    response = await async_elastic_session.search(
        query=query, index=HIERARCHY_OBJ_INDEX, size=1
    )
    assert response["hits"]["hits"]

    msgs_list = []
    for msg_dict in response["hits"]["hits"]:
        msg = NodeMessageSchema(**msg_dict["_source"])
        msgs_list.append(msg)

    msgs = ListNode(objects=msgs_list)
    return msgs


@fixture(scope="function")
def new_node_data_msg() -> ListNodeData:
    msg1 = NodeDataMessageSchema(
        id=100500,
        level_id=int(f"{HIERARCHY_START_RANGE}{LEVEL_START_RANGE}"),
        node_id="1",
        mo_id=1,
        mo_name="Test mo name",
        mo_status="ACTIVE",
        mo_tmo_id=1,
        mo_active=True,
        unfolded_key={},
        mo_latitude=0,
        mo_longitude=0,
        mo_p_id=0,
    )
    msgs = ListNodeData(objects=[msg1])
    return msgs


@fixture(scope="function")
async def existing_node_data_msg(async_elastic_session: AsyncElasticsearch):
    query = {"match_all": {}}
    response = await async_elastic_session.search(
        query=query, index=HIERARCHY_NODE_DATA_INDEX, size=1
    )
    assert response["hits"]["hits"]

    msgs_list = []
    for msg_dict in response["hits"]["hits"]:
        msg = NodeDataMessageSchema(**msg_dict["_source"])
        msgs_list.append(msg)

    msgs = ListNodeData(objects=msgs_list)
    return msgs


@fixture(scope="function")
async def new_permission_data():
    msg1 = HierarchyPermissionMessageSchema(
        id=1,
        root_permission_id=None,
        permission="test_permissions 100500",
        permission_name="test_permissions 100500",
        create=True,
        read=True,
        update=True,
        delete=True,
        admin=True,
        parent_id=100500,
    )

    msgs = ListHierarchyPermission(objects=[msg1])
    return msgs


@fixture(scope="function")
async def existing_permission_data():
    msg1 = HierarchyPermissionMessageSchema(
        id=1,
        root_permission_id=None,
        permission=f"test_permissions {HIERARCHY_START_RANGE}",
        permission_name=f"test_permissions {HIERARCHY_START_RANGE}",
        create=True,
        read=True,
        update=True,
        delete=True,
        admin=True,
        parent_id=HIERARCHY_START_RANGE,
    )

    msgs = ListHierarchyPermission(objects=[msg1])
    return msgs
