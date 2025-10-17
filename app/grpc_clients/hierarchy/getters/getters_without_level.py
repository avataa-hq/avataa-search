import json
from typing import AsyncGenerator

from google.protobuf import json_format
from grpc import StatusCode, RpcError
from grpc.aio import Channel

from grpc_clients.hierarchy.protobuf import hierarchy_data_pb2
from grpc_clients.hierarchy.protobuf import hierarchy_data_pb2_grpc
from grpc_clients.hierarchy.protobuf.hierarchy_data_pb2 import HierarchySchema


async def get_all_hierarchies_by_grpc(async_channel: Channel) -> AsyncGenerator:
    """Returns GetAllHierarchies instance as AsyncGenerator"""
    msg = hierarchy_data_pb2.EmptyRequest()
    stub = hierarchy_data_pb2_grpc.HierarchyDataStub(async_channel)
    grpc_response = stub.GetAllHierarchies(msg)
    async for grpc_chunk in grpc_response:
        yield grpc_chunk


async def get_all_hierarchies_as_dicts_by_grpc(
    async_channel: Channel,
) -> AsyncGenerator:
    """Returns list of dicts (hierarchies data) as AsyncGenerator
    use always_print_fields_with_no_presence for protobuf version higher than 26 instead including_default_values_fields
    https://github.com/protocolbuffers/protobuf/releases/tag/v26"""
    async for grpc_chunk in get_all_hierarchies_by_grpc(async_channel):
        res = json_format.MessageToDict(
            grpc_chunk,
            # including_default_value_fields=True,
            always_print_fields_with_no_presence=True,
            preserving_proto_field_name=True,
        )
        yield res["items"]


async def get_all_levels_for_spec_hierarchy_by_grpc(
    hierarchy_id: int, async_channel: Channel
) -> AsyncGenerator:
    """Returns GetLevelsByHierarchyIdResponse instance as AsyncGenerator"""
    stub = hierarchy_data_pb2_grpc.HierarchyDataStub(async_channel)
    msg = hierarchy_data_pb2.HierarchyIdRequest(hierarchy_id=hierarchy_id)
    grpc_response = stub.GetLevelsByHierarchyId(msg)
    async for grpc_chunk in grpc_response:
        yield grpc_chunk


async def get_all_levels_for_spec_hierarchy_by_as_dicts_grpc(
    hierarchy_id: int, async_channel: Channel
) -> AsyncGenerator:
    """Returns list of dicts (levels data) as AsyncGenerator
    use always_print_fields_with_no_presence for protobuf version higher than 26 instead including_default_values_fields
    https://github.com/protocolbuffers/protobuf/releases/tag/v26.0
    """
    async for grpc_chunk in get_all_levels_for_spec_hierarchy_by_grpc(
        hierarchy_id, async_channel
    ):
        res = json_format.MessageToDict(
            grpc_chunk,
            # including_default_value_fields=True,
            always_print_fields_with_no_presence=True,
            preserving_proto_field_name=True,
        )
        yield res["items"]


async def get_all_obj_for_spec_level_by_grpc(
    level_id: int, async_channel: Channel
) -> AsyncGenerator:
    """Returns GetObjsByLevelIdResponse instance as AsyncGenerator"""
    stub = hierarchy_data_pb2_grpc.HierarchyDataStub(async_channel)
    msg = hierarchy_data_pb2.LevelIdRequest(level_id=level_id)
    grpc_response = stub.GetObjsByLevelId(msg)
    async for grpc_chunk in grpc_response:
        yield grpc_chunk


async def get_all_obj_for_spec_level_as_dicts_by_grpc(
    level_id: int, async_channel: Channel
) -> AsyncGenerator:
    """Returns list of dicts (levels data) as AsyncGenerator
    use always_print_fields_with_no_presence for protobuf version higher than 26 instead including_default_values_fields
    https://github.com/protocolbuffers/protobuf/releases/tag/v26.0
    """
    async for grpc_chunk in get_all_obj_for_spec_level_by_grpc(
        level_id, async_channel
    ):
        res = json_format.MessageToDict(
            grpc_chunk,
            # including_default_value_fields=True,
            always_print_fields_with_no_presence=True,
            preserving_proto_field_name=True,
        )
        yield res["items"]


async def get_all_node_data_for_spec_level_by_grpc(
    level_id: int, async_channel: Channel
) -> AsyncGenerator:
    """Returns GetObjsByLevelIdResponse instance as AsyncGenerator"""
    stub = hierarchy_data_pb2_grpc.HierarchyDataStub(async_channel)
    msg = hierarchy_data_pb2.LevelIdRequest(level_id=level_id)
    grpc_response = stub.GetNodeDatasByLevelId(msg)
    async for grpc_chunk in grpc_response:
        yield grpc_chunk


async def get_all_node_data_for_spec_level_as_dicts_by_grpc(
    level_id: int, async_channel: Channel
) -> AsyncGenerator:
    """Returns list of dicts (levels data) as AsyncGenerator
    use always_print_fields_with_no_presence for protobuf version higher than 26 instead including_default_values_fields
    https://github.com/protocolbuffers/protobuf/releases/tag/v26"""
    max_long_value = 9_223_372_036_854_775_807
    async for grpc_chunk in get_all_node_data_for_spec_level_by_grpc(
        level_id, async_channel
    ):
        res = json_format.MessageToDict(
            grpc_chunk,
            # including_default_value_fields=True,
            always_print_fields_with_no_presence=True,
            preserving_proto_field_name=True,
        )
        for item in res["items"]:
            unfolded_key = item.get("unfolded_key")
            if unfolded_key:
                item["unfolded_key"] = json.loads(unfolded_key)
                # TODO: A crutch that removes values that exceed the allowable long value
                keys_to_remove = []
                for key, value in item["unfolded_key"].items():
                    if isinstance(value, int) and value > max_long_value:
                        keys_to_remove.append(key)
                for key in keys_to_remove:
                    del item["unfolded_key"][key]
        yield res["items"]


async def get_hierarchy_by_grpc(
    hierarchy_id: int, async_channel: Channel
) -> HierarchySchema:
    """Returns HierarchySchema"""
    stub = hierarchy_data_pb2_grpc.HierarchyDataStub(async_channel)
    msg = hierarchy_data_pb2.HierarchyIdRequest(hierarchy_id=hierarchy_id)
    try:
        grpc_response = await stub.GetHierarchyById(msg)
        return grpc_response
    except RpcError as ex:
        if ex.code() == StatusCode.NOT_FOUND:
            return None
        else:
            print(ex)


async def get_hierarchy_as_dict_by_grpc(
    hierarchy_id: int, async_channel: Channel
) -> dict:
    """Returns HierarchySchema as dict
    use always_print_fields_with_no_presence for protobuf version higher than 26 instead including_default_values_fields
    https://github.com/protocolbuffers/protobuf/releases/tag/v26"""
    h_proto_inst = await get_hierarchy_by_grpc(hierarchy_id, async_channel)
    if h_proto_inst:
        res = json_format.MessageToDict(
            h_proto_inst,
            # including_default_value_fields=True,
            always_print_fields_with_no_presence=True,
            preserving_proto_field_name=True,
        )
        return res
    return {}


async def get_hierarchy_permissions_for_spec_hierarchy_by_grpc(
    hierarchy_id: int, async_channel: Channel
) -> AsyncGenerator:
    """Returns PermissionStreamResponse instance as AsyncGenerator"""
    stub = hierarchy_data_pb2_grpc.HierarchyDataStub(async_channel)
    msg = hierarchy_data_pb2.HierarchyIdRequest(hierarchy_id=hierarchy_id)
    grpc_response = stub.GetHierarchyPermissionByHierarchyId(msg)
    async for grpc_chunk in grpc_response:
        yield grpc_chunk


async def get_hierarchy_permissions_as_dicts_for_spec_hierarchy_by_grpc(
    hierarchy_id: int, async_channel: Channel
) -> AsyncGenerator:
    """Returns list of dicts (hierarchy permissions) as AsyncGenerator
    use always_print_fields_with_no_presence for protobuf version higher than 26 instead including_default_values_fields
    https://github.com/protocolbuffers/protobuf/releases/tag/v26"""
    async for (
        grpc_chunk
    ) in get_hierarchy_permissions_for_spec_hierarchy_by_grpc(
        hierarchy_id, async_channel
    ):
        res = json_format.MessageToDict(
            grpc_chunk,
            # including_default_value_fields=True,
            always_print_fields_with_no_presence=True,
            preserving_proto_field_name=True,
        )
        yield res["items"]
