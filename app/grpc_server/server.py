import logging

import grpc

from elastic.client import ElasticsearchManager
from grpc_server.group_router.handler import GroupSearchHandler
from grpc_server.group_router.proto.from_group_to_search_pb2_grpc import (
    add_GroupSearchServicer_to_server,
)
from grpc_server.mo_finder.proto.mo_finder_pb2_grpc import (
    add_MOFinderServicer_to_server,
)
from grpc_server.mo_finder.handler import MOFinderHandler
from settings.config import SERVER_GRPC_PORT
from v2.grpc_routers.severity.router import SearchSeverity
from v2.grpc_routers.severity.proto.search_severity_pb2_grpc import (
    add_SearchSeverityServicer_to_server,
)


async def start_grpc_server():
    """Entry point to gRPC server"""
    server = grpc.aio.server()
    add_MOFinderServicer_to_server(MOFinderHandler(), server)
    add_SearchSeverityServicer_to_server(SearchSeverity(), server=server)
    # async for elastic_client in get_async_client():
    #     add_GroupSearchServicer_to_server(
    #         GroupSearchHandler(elastic_client=elastic_client), server=server
    #     )
    add_GroupSearchServicer_to_server(
        GroupSearchHandler(elastic_client=ElasticsearchManager().get_client()),
        server=server,
    )
    listen_addr = f"[::]:{SERVER_GRPC_PORT}"
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    await server.start()
    await server.wait_for_termination()
