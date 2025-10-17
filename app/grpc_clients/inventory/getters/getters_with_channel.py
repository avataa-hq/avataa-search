import pickle
from typing import List, AsyncGenerator

import grpc

from grpc_clients.inventory.protobuf.mo_info import (
    mo_info_pb2_grpc,
    mo_info_pb2,
)
from settings.config import INVENTORY_HOST, INVENTORY_GRPC_PORT


async def get_all_tmo_data_from_inventory_channel_in():
    """getter for GetTPRMData"""
    async with grpc.aio.insecure_channel(
        f"{INVENTORY_HOST}:{INVENTORY_GRPC_PORT}",
        options=[
            ("grpc.keepalive_time_ms", 20_000),
            ("grpc.keepalive_timeout_ms", 15_000),
            ("grpc.http2.max_pings_without_data", 5),
            ("grpc.keepalive_permit_without_calls", 1),
        ],
    ) as async_channel:
        stub = mo_info_pb2_grpc.InformerStub(async_channel)
        msg = mo_info_pb2.GetAllTMORequest()
        resp = await stub.GetAllTMO(msg)
        return [pickle.loads(bytes.fromhex(item)) for item in resp.tmo_info]


async def get_all_mo_with_params_by_tmo_id_channel_in(tmo_id: int):
    """Returns all MOs with all attrs and all params for particular tmo_id"""
    async with grpc.aio.insecure_channel(
        f"{INVENTORY_HOST}:{INVENTORY_GRPC_PORT}",
        options=[
            ("grpc.keepalive_time_ms", 20_000),
            ("grpc.keepalive_timeout_ms", 15_000),
            ("grpc.http2.max_pings_without_data", 5),
            ("grpc.keepalive_permit_without_calls", 1),
        ],
    ) as async_channel:
        stub = mo_info_pb2_grpc.InformerStub(async_channel)
        msg = mo_info_pb2.GetAllMOWithParamsByTMOIdRequest(tmo_id=tmo_id)
        grpc_response = stub.GetAllMOWithParamsByTMOId(msg)
        res = []
        async for grpc_chunk in grpc_response:
            res.extend(
                [
                    pickle.loads(bytes.fromhex(item))
                    for item in grpc_chunk.mos_with_params
                ]
            )
        return res


async def get_all_tprms_channel_in_as_async_generator(
    tprm_ids: List[int] = None,
) -> AsyncGenerator:
    """Returns all TPRMs data if tprm_ids is empty or None,
    otherwise returns data of TPRMs based on the ids included in tprm_ids"""
    if tprm_ids is None:
        tprm_ids = list()

    async with grpc.aio.insecure_channel(
        f"{INVENTORY_HOST}:{INVENTORY_GRPC_PORT}",
        options=[
            ("grpc.keepalive_time_ms", 20_000),
            ("grpc.keepalive_timeout_ms", 15_000),
            ("grpc.http2.max_pings_without_data", 5),
            ("grpc.keepalive_permit_without_calls", 1),
        ],
    ) as async_channel:
        stub = mo_info_pb2_grpc.InformerStub(async_channel)
        msg = mo_info_pb2.RequestGetTPRMAlldata(tprm_ids=tprm_ids)
        grpc_response = stub.GetTPRMAllData(msg)
        async for grpc_chunk in grpc_response:
            chunk = [
                pickle.loads(bytes.fromhex(item))
                for item in grpc_chunk.tprms_data
            ]
            yield chunk
