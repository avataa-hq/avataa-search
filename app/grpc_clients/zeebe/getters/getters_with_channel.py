from typing import AsyncGenerator

import grpc

from grpc_clients.zeebe.getters.getters_without_channel import (
    get_all_process_instance_data_from_ms_zeebe_grps,
)
from settings.config import ZEEBE_CLIENT_HOST, ZEEBE_CLIENT_GRPC_PORT


async def get_all_process_instance_data_from_ms_zeebe_grps_channel_in(
    tmo_id: int,
) -> AsyncGenerator:
    """Returns all Process Instances data for special tmo_id from MS Zeebe service"""

    async with grpc.aio.insecure_channel(
        f"{ZEEBE_CLIENT_HOST}:{ZEEBE_CLIENT_GRPC_PORT}"
    ) as async_channel:
        async for (
            grpc_chunk
        ) in get_all_process_instance_data_from_ms_zeebe_grps(
            async_channel=async_channel, tmo_id=tmo_id
        ):
            yield grpc_chunk
