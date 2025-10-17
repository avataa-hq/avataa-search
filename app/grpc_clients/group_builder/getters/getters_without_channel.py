import pickle
from typing import AsyncGenerator

from grpc.aio import Channel

from grpc_clients.group_builder.protobuf.grpc_group import (
    grpc_group_pb2_grpc,
    grpc_group_pb2,
)
from indexes_mapping.inventory.mapping import INVENTORY_PARAMETERS_FIELD_NAME
from services.group_builder.models import GroupStatisticUniqueFields


async def get_list_of_groups_for_special_tmo(
    tmo_id: int, async_channel: Channel
) -> AsyncGenerator:
    """Returns list of group names for special tmo_id as AsyncGenerator"""
    stub = grpc_group_pb2_grpc.GroupStub(async_channel)
    msg = grpc_group_pb2.RequestListGroupByTMOID(tmo_id=tmo_id)
    grpc_response = stub.ListGroupByTMOID(msg)
    async for grpc_chunk in grpc_response:
        yield grpc_chunk.group_names


async def get_list_of_mo_ids_for_special_group(
    group_name: str, async_channel: Channel
) -> AsyncGenerator:
    """Returns list of mo ids which included in special group as AsyncGenerator"""
    stub = grpc_group_pb2_grpc.GroupStub(async_channel)
    msg = grpc_group_pb2.RequestListMOIdsInSpecialGroup(group_name=group_name)
    grpc_response = stub.ListMOIdsInSpecialGroup(msg)
    async for grpc_chunk in grpc_response:
        yield grpc_chunk.entity_ids


async def get_statistic_of_special_group(
    group_name: str, async_channel: Channel
) -> dict:
    """Returns dict with all statistic for special group"""
    stub = grpc_group_pb2_grpc.GroupStub(async_channel)
    msg = grpc_group_pb2.RequestGetGroupStatistic(group_name=group_name)
    grpc_response = await stub.GetGroupStatistic(msg)
    un_pickled_data = pickle.loads(bytes.fromhex(grpc_response.group_statistic))
    res = dict()
    group_name = un_pickled_data.get("groupName")
    if not group_name:
        return res

    res["state"] = "ACTIVE"

    res[GroupStatisticUniqueFields.GROUP_NAME.value] = group_name
    tmo_data = un_pickled_data.get("TMO")
    if tmo_data:
        tmo_id = tmo_data.get("tmo_id")
        if tmo_id:
            res["tmo_id"] = tmo_id

    tprm_data = un_pickled_data.get("TPRM")
    if tprm_data:
        res[INVENTORY_PARAMETERS_FIELD_NAME] = tprm_data

    group_type = un_pickled_data.get(
        GroupStatisticUniqueFields.GROUP_TYPE.value
    )
    if group_type:
        res[GroupStatisticUniqueFields.GROUP_TYPE.value] = group_type

    return res
