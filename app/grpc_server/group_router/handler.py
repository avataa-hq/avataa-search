import json
from typing import Iterator, Annotated, AsyncGenerator, Any

import grpc
from elasticsearch import AsyncElasticsearch
from fastapi import HTTPException
from google.protobuf import json_format
from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel, ValidationError, BeforeValidator

from elastic.pydantic_models import FilterColumn
from grpc_server.group_router.proto.from_group_to_search_pb2 import (
    RequestGetProcesses,
    RequestGetProcessesGroups,
    ResponseGetProcesses,
    ResponseProcessesGroups,
    ProcessesGroupItem,
    RequestGetMOsByFilters,
    ResponseGetMOsByFilters,
)
from grpc_server.group_router.proto.from_group_to_search_pb2_grpc import (
    GroupSearchServicer,
)
from indexes_mapping.inventory.mapping import INVENTORY_PARAMETERS_FIELD_NAME
from security.implementation.disabled import default_user
from security.security_data_models import UserData
from utils_by_services.inventory.common import search_after_generator
from v2.routers.inventory.utils.create_inventory_data_filter import (
    create_inventory_data_filter,
    InventoryDataFilter,
)
from v2.routers.severity.models import SortColumn, Limit, Ranges
from v2.routers.severity.utils import get_process_search_args


def convert_to_str(value):
    if isinstance(value, str):
        return value
    return json.dumps(value, default=str)


class ProcessGroupKeyDto(BaseModel):
    grouped_by: str
    grouping_value: Annotated[str, BeforeValidator(convert_to_str)]


class ProcessesGroupItemDto(BaseModel):
    group: list[ProcessGroupKeyDto]
    quantity: int


class GroupSearchHandler(GroupSearchServicer):
    EXCLUDE_AGGREAGATION_KEYS: set[str] = {"doc_count", "key"}

    def __init__(self, elastic_client: AsyncElasticsearch):
        self._elastic_client = elastic_client

    @staticmethod
    def exception_wrapper(func):
        async def wrapper(*args, **kwargs):
            context: grpc.aio.ServicerContext | None = None
            if "context" in kwargs:
                context = kwargs["context"]
            elif len(args) >= 3:
                context = args[2]
            try:
                async for result in func(*args, **kwargs):
                    yield result

            except HTTPException as exc:
                context.set_code(grpc.StatusCode.ABORTED)
                context.set_details(str(exc))
            except ValidationError as exc:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(str(exc))
            except ValueError as exc:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(str(exc))

        return wrapper

    def _convert_aggregation(
        self,
        aggregations: dict,
        group_keys: list[ProcessGroupKeyDto] | None = None,
    ) -> list[ProcessesGroupItemDto]:
        if not group_keys:
            group_keys = []
        results: list[ProcessesGroupItemDto] = []

        if not aggregations:
            return results

        grouped_by = None
        for key in aggregations.keys():
            if key in self.EXCLUDE_AGGREAGATION_KEYS:
                continue
            grouped_by = key
            break

        if grouped_by is None:
            # deepest level
            result = ProcessesGroupItemDto(
                group=group_keys, quantity=aggregations["doc_count"]
            )
            results.append(result)
        else:
            # mid levels
            for bucket in aggregations[grouped_by]["buckets"]:
                # TODO: doc_count_error_upper_bound, sum_other_doc_count
                group_key = ProcessGroupKeyDto(
                    grouped_by=grouped_by, grouping_value=bucket["key"]
                )
                bucket_group_keys = group_keys.copy()
                bucket_group_keys.append(group_key)
                result = self._convert_aggregation(
                    aggregations=bucket, group_keys=bucket_group_keys
                )
                results.extend(result)
        return results

    def _convert_request(self, request) -> dict:
        dict_request = MessageToDict(
            request,
            # including_default_value_fields=False,
            preserving_proto_field_name=True,
        )
        result = {
            "user_data": default_user,
            "tmo_id": None,
            "filters_list": None,
            "with_groups": False,
            "ranges_object": None,
            "sort": None,
            "limit": Limit(),
            "find_by_value": None,
            "elastic_client": self._elastic_client,
            "group_by": None,
            "min_group_qty": 1,
        }

        if "user_data" in dict_request:
            result["user_data"] = UserData(
                **json.loads(dict_request["user_data"])
            )

        if "tmo_id" in dict_request:
            result["tmo_id"] = int(dict_request["tmo_id"])

        if "filters_list" in dict_request:
            result["filters_list"] = [
                FilterColumn.model_validate(i)
                for i in json.loads(dict_request["filters_list"])
            ]

        if "sort" in dict_request:
            result["sort"] = [
                SortColumn.model_validate(i)
                for i in json.loads(dict_request["sort"])
            ]

        if "find_by_value" in dict_request:
            result["find_by_value"] = dict_request["find_by_value"]

        if "with_groups" in dict_request:
            result["with_groups"] = dict_request["with_groups"]

        if "ranges_object" in dict_request:
            result["ranges_object"] = Ranges.model_validate(
                json.loads(dict_request["ranges_object"])
            )

        if "limit" in dict_request:
            result["limit"] = Limit.model_validate(
                json.loads(dict_request["limit"])
            )

        if "group_by" in dict_request:
            result["group_by"] = json.loads(dict_request["group_by"])

        if "min_group_qty" in dict_request:
            result["min_group_qty"] = dict_request["min_group_qty"]

        return result

    @exception_wrapper
    async def GetProcesses(
        self,
        request: RequestGetProcesses,
        context: grpc.aio.ServicerContext,
    ) -> AsyncGenerator[ResponseGetProcesses, Any]:
        es_size_query = 10_000
        converted_request = self._convert_request(request=request)
        search_args: dict = await get_process_search_args(**converted_request)
        if (
            search_args.get("from_", 0) + search_args.get("size", 0)
            < es_size_query
        ):
            search_res = await self._elastic_client.search(**search_args)
            for item in search_res.get("hits", {}).get("hits", []):
                item = item["_source"]
                params = item.get(INVENTORY_PARAMETERS_FIELD_NAME)
                if params:
                    item.update(params)
                    del item[INVENTORY_PARAMETERS_FIELD_NAME]
                json_item = json.dumps(item)
                yield ResponseGetProcesses(mo=json_item)
        else:
            async for item in search_after_generator(
                self._elastic_client, body=search_args
            ):
                params = item.get(INVENTORY_PARAMETERS_FIELD_NAME)
                if params:
                    item.update(params)
                    del item[INVENTORY_PARAMETERS_FIELD_NAME]
                json_item = json.dumps(item)
                yield ResponseGetProcesses(mo=json_item)

    @exception_wrapper
    async def GetProcessesGroups(
        self,
        request: RequestGetProcessesGroups,
        context: grpc.aio.ServicerContext,
    ) -> Iterator[ResponseProcessesGroups]:
        converted_request = self._convert_request(request=request)
        if not converted_request["group_by"]:
            raise ValueError("group_by is required")
        converted_request["limit"] = Limit(limit=1, offset=0)
        search_args: dict = await get_process_search_args(**converted_request)

        search_res = await self._elastic_client.search(**search_args)
        group_results = self._convert_aggregation(
            aggregations=search_res["aggregations"]
        )

        process_group_item = ProcessesGroupItem()
        for result in group_results:
            parsed_result = json_format.Parse(
                result.model_dump_json(by_alias=True), process_group_item
            )
            yield ResponseProcessesGroups(item=parsed_result)

    @exception_wrapper
    async def GetMOsByFilters(
        self, request: RequestGetMOsByFilters, context: grpc.aio.ServicerContext
    ) -> Iterator[ResponseGetMOsByFilters]:
        converted_request = self._convert_request(request=request)
        inventory_filter: InventoryDataFilter = (
            await create_inventory_data_filter(
                user_data=converted_request["user_data"],
                filter_columns=converted_request["filters_list"],
                elastic_client=converted_request["elastic_client"],
                with_groups=converted_request["with_groups"],
                tmo_id=converted_request["tmo_id"],
                sort_by=[],
                search_by_value=None,
                limit=converted_request["limit"].limit,
                offset=converted_request["limit"].offset,
            )
        )

        search_res = await self._elastic_client.search(
            index=inventory_filter.search_index,
            body=inventory_filter.body,
            ignore_unavailable=True,
        )
        objects = [item["_source"] for item in search_res["hits"]["hits"]]

        for obj in objects:
            obj = json.dumps(obj)
            yield ResponseGetMOsByFilters(mos=[obj])
