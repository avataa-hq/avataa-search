from typing import AsyncGenerator
from grpc.aio import Channel

from grpc_clients.zeebe.protobuf import (
    zeebe_severity_pb2_grpc,
    zeebe_severity_pb2,
)


async def get_all_process_instance_data_from_ms_zeebe_grps(
    async_channel: Channel, tmo_id: int
) -> AsyncGenerator:
    """Returns all Process Instances data for special tmo_id from MS Zeebe service"""
    stub = zeebe_severity_pb2_grpc.SeverityStub(channel=async_channel)
    msg = zeebe_severity_pb2.GetProcessInstanceForSpecialTMORequest(
        tmo_id=tmo_id
    )
    grpc_response = stub.GetProcessInstanceForSpecialTMO(msg)
    async for grpc_chunk in grpc_response:
        yield grpc_chunk
