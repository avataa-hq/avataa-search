import json
import sys
import traceback

import grpc
from fastapi import HTTPException
from google.protobuf.json_format import MessageToDict
from grpc.aio import ServicerContext

from elastic.client import get_async_client
from security.implementation.disabled import DisabledSecurity
from v2.grpc_routers.severity.models import (
    ApiByRangesInput,
    ApiGetProcessesInput,
)
from v2.grpc_routers.severity.proto.search_severity_pb2 import (
    ListResponseSeverity,
    FilterInput,
    ResponseSeverityItem as GrpcResponseSeverityItem,
    ByRangesInput,
    ProcessesInput,
    ProcessesResponse,
)
from v2.grpc_routers.severity.proto.search_severity_pb2_grpc import (
    SearchSeverityServicer,
)
from v2.routers.severity.models import (
    FilterDataInput,
    ResponseSeverityItem,
    Processes,
)
from v2.routers.severity.router import (
    get_severity_by_filters,
    get_severity_by_ranges,
    get_processes,
)


def convert_severity_response(
    api_response: list[dict] | list[ResponseSeverityItem],
) -> ListResponseSeverity:
    grpc_response_severity_items = []
    for item in api_response:
        # normalize api response to ResponseSeverityItem model
        if isinstance(item, dict):
            item = ResponseSeverityItem(**item)
        # convert to grpc item
        converted_item = GrpcResponseSeverityItem(
            **item.model_dump(mode="json")
        )
        grpc_response_severity_items.append(converted_item)
    return ListResponseSeverity(items=grpc_response_severity_items)


def convert_severity_rows_response(
    api_response: dict | Processes,
) -> ProcessesResponse:
    if isinstance(api_response, dict):
        api_response = Processes.model_validate(api_response)
    return ProcessesResponse(
        **api_response.model_dump(mode="json", exclude={"rows"}),
        rows=[
            json.dumps(i)
            for i in api_response.model_dump(mode="json", include={"rows"})[
                "rows"
            ]
        ],
    )


def set_context_http_exception(
    e: HTTPException, context: ServicerContext
) -> ServicerContext:
    if e.status_code == 409:
        context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
    elif e.status_code == 422:
        context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
    elif e.status_code == 404:
        context.set_code(grpc.StatusCode.NOT_FOUND)
    elif e.status_code == 400:
        context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
    else:
        context.set_code(grpc.StatusCode.UNKNOWN)
    context.set_details(str(e))
    return context


class SearchSeverity(SearchSeverityServicer):
    async def GetSeverityByFilters(
        self,
        request: FilterInput,
        context: ServicerContext,
    ) -> ListResponseSeverity | ServicerContext:
        user_mock = await DisabledSecurity()(request=None)  # noqa
        try:
            async for elastic_client in get_async_client():
                parsed_request = []
                for f in request.filters:
                    parsed_f = json.loads(f)
                    parsed_f = FilterDataInput(**parsed_f)
                    parsed_request.append(parsed_f)
                api_response = await get_severity_by_filters(
                    filters=parsed_request,
                    elastic_client=elastic_client,
                    user_data=user_mock,
                )
                grpc_result: ListResponseSeverity = convert_severity_response(
                    api_response=api_response
                )
                return grpc_result
        except ValueError as e:
            print(traceback.format_exc(), file=sys.stderr)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return context
        except HTTPException as e:
            print(traceback.format_exc(), file=sys.stderr)
            set_context_http_exception(e=e, context=context)
            return context
        except Exception as e:
            print(traceback.format_exc(), file=sys.stderr)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return context

    async def GetSeverityByRanges(
        self,
        request: ByRangesInput,
        context: ServicerContext,
    ) -> ListResponseSeverity | ServicerContext:
        user_mock = await DisabledSecurity()(request=None)  # noqa
        try:
            async for elastic_client in get_async_client():
                dict_request = MessageToDict(
                    message=request, preserving_proto_field_name=True
                )
                parsed_request = ApiByRangesInput.model_validate(dict_request)
                api_response = await get_severity_by_ranges(
                    tmo_id=parsed_request.tmo_id,
                    ranges_object=parsed_request.ranges_object,
                    filters_list=parsed_request.filters_list,
                    find_by_value=parsed_request.find_by_value,
                    elastic_client=elastic_client,
                    user_data=user_mock,
                )
                grpc_result: ListResponseSeverity = convert_severity_response(
                    api_response=api_response
                )
                return grpc_result
        except ValueError as e:
            print(traceback.format_exc(), file=sys.stderr)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return context
        except HTTPException as e:
            print(traceback.format_exc(), file=sys.stderr)
            set_context_http_exception(e=e, context=context)
            return context
        except Exception as e:
            print(traceback.format_exc(), file=sys.stderr)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return context

    async def GetProcesses(
        self,
        request: ProcessesInput,
        context: ServicerContext,
    ) -> ProcessesResponse | ServicerContext:
        user_mock = await DisabledSecurity()(request=None)  # noqa
        try:
            async for elastic_client in get_async_client():
                dict_request = MessageToDict(
                    message=request, preserving_proto_field_name=True
                )
                parsed_request = ApiGetProcessesInput.model_validate(
                    dict_request
                )
                api_response = await get_processes(
                    tmo_id=parsed_request.tmo_id,
                    find_by_value=parsed_request.find_by_value,
                    filters_list=parsed_request.filters_list,
                    with_groups=parsed_request.with_groups,
                    ranges_object=parsed_request.ranges_object,
                    sort=parsed_request.sort,
                    limit=parsed_request.limit,
                    elastic_client=elastic_client,
                    user_data=user_mock,
                )
                grpc_result: ProcessesResponse = convert_severity_rows_response(
                    api_response=api_response
                )
                return grpc_result
        except ValueError as e:
            print(traceback.format_exc(), file=sys.stderr)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return context
        except HTTPException as e:
            print(traceback.format_exc(), file=sys.stderr)
            set_context_http_exception(e=e, context=context)
            return context
        except Exception as e:
            print(traceback.format_exc(), file=sys.stderr)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return context
