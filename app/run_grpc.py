import logging
import asyncio

from grpc_server.server import start_grpc_server


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(start_grpc_server())
