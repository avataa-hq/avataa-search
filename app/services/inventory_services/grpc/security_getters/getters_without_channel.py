from typing import Iterator, List

from grpc.aio import Channel

from services.inventory_services.protobuf_files.security import (
    transfer_pb2_grpc,
)
from services.inventory_services.protobuf_files.security.custom_deserializer import (
    protobuf_kafka_list_permission_msg_to_list_of_dicts,
)
from services.inventory_services.protobuf_files.security.transfer_pb2 import (
    Empty,
)


async def get_tmo_permissions(async_channel: Channel) -> Iterator[List[dict]]:
    stub = transfer_pb2_grpc.TransferStub(async_channel)
    msg = Empty()
    response = stub.GetTMOPermission(msg)
    async for msg_chunk in response:
        result = protobuf_kafka_list_permission_msg_to_list_of_dicts(
            msg_chunk, including_default_value_fields=True
        )
        yield result


async def get_mo_permissions(async_channel: Channel) -> Iterator[List[dict]]:
    stub = transfer_pb2_grpc.TransferStub(async_channel)
    msg = Empty()
    response = stub.GetMOPermission(msg)
    async for msg_chunk in response:
        result = protobuf_kafka_list_permission_msg_to_list_of_dicts(
            msg_chunk, including_default_value_fields=True
        )
        yield result


async def get_tprm_permissions(async_channel: Channel) -> Iterator[List[dict]]:
    stub = transfer_pb2_grpc.TransferStub(async_channel)
    msg = Empty()
    response = stub.GetTPMPermission(msg)
    async for msg_chunk in response:
        result = protobuf_kafka_list_permission_msg_to_list_of_dicts(
            msg_chunk, including_default_value_fields=True
        )
        yield result
