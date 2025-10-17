import pickle
from typing import AsyncGenerator, List

from grpc.aio import Channel

from grpc_clients.inventory.protobuf.mo_info import (
    mo_info_pb2_grpc,
    mo_info_pb2,
)


async def get_all_tmo_data_from_inventory(async_channel: Channel):
    """Returns list of dicts with all inventory TMOs"""
    stub = mo_info_pb2_grpc.InformerStub(async_channel)
    msg = mo_info_pb2.GetAllTMORequest()
    resp = await stub.GetAllTMO(msg)
    return [pickle.loads(bytes.fromhex(item)) for item in resp.tmo_info]


async def get_all_tprms_for_special_tmo_id_async_generator(
    tmo_id: int, async_channel: Channel
) -> AsyncGenerator:
    """Returns all TPRMs data for special tmo_id as AsyncGenerator"""
    stub = mo_info_pb2_grpc.InformerStub(async_channel)
    msg = mo_info_pb2.RequestGetAllTPRMSByTMOId(tmo_id=tmo_id)
    grpc_response = stub.GetAllTPRMSByTMOId(msg)
    async for grpc_chunk in grpc_response:
        chunk = [
            pickle.loads(bytes.fromhex(item)) for item in grpc_chunk.tprms_data
        ]
        yield chunk


async def get_raw_prm_data_by_tprm_id(
    tprm_id: int,
    async_channel: Channel,
):
    """Returns all PRMs raw data for special tmo_id as AsyncGenerator"""
    stub = mo_info_pb2_grpc.InformerStub(async_channel)
    msg = mo_info_pb2.RequestGetAllRawPRMDataByTPRMId(tprm_id=tprm_id)
    grpc_response = stub.GetAllRawPRMDataByTPRMId(msg)
    async for grpc_chunk in grpc_response:
        yield grpc_chunk


async def get_tprm_data_by_tprm_ids(
    tprm_ids: List[int], async_channel: Channel
):
    """Returns TPRMs data for tprm_ids"""
    stub = mo_info_pb2_grpc.InformerStub(async_channel)
    msg = mo_info_pb2.RequestGetTPRMAlldata(tprm_ids=tprm_ids)
    grpc_response = stub.GetTPRMAllData(msg)
    async for grpc_chunk in grpc_response:
        yield grpc_chunk


async def get_mo_data_by_mo_ids(mo_ids: List[int], async_channel: Channel):
    chunk_size = 50_000
    stub = mo_info_pb2_grpc.InformerStub(async_channel)
    mo_id_chunks = (
        mo_ids[i : i + chunk_size] for i in range(0, len(mo_ids), chunk_size)
    )
    for mo_id_chunk in mo_id_chunks:
        msg = mo_info_pb2.GetMODataByIdsRequest(mo_ids=mo_id_chunk)
        grpc_response = stub.GetMODataByIds(msg)
        async for grpc_chunk in grpc_response:
            yield grpc_chunk
