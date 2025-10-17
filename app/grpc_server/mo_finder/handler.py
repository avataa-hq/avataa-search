import json

import grpc.aio
from fastapi import HTTPException

from elastic.client import ElasticsearchManager
from elastic.pydantic_models import FilterColumn
from elastic.query_builder_service.inventory_index.mo_object.utils import (
    get_search_query_for_inventory_obj_index,
    get_dict_of_inventory_attr_and_params_types,
)
from elastic.query_builder_service.inventory_index.utils.index_utils import (
    get_index_names_fo_list_of_tmo_ids,
)
from grpc_server.mo_finder.proto import mo_finder_pb2
from grpc_server.mo_finder.proto.mo_finder_pb2_grpc import MOFinderServicer


class MOFinderHandler(MOFinderServicer):
    async def GetMOsByFilters(
        self,
        request: mo_finder_pb2.RequestGetMOsByFilters,
        context: grpc.aio.ServicerContext,
    ) -> mo_finder_pb2.ResponseGetMOsByFilters:
        search_query = {"bool": {"must": []}}
        filters_data = json.loads(request.filters)
        filter_columns = [FilterColumn(**f_data) for f_data in filters_data]
        elastic_client = ElasticsearchManager().get_client()

        column_names = set()
        if filter_columns:
            column_names.update(
                {filter_column.column_name for filter_column in filter_columns}
            )

        dict_of_types = await get_dict_of_inventory_attr_and_params_types(
            array_of_attr_and_params_names=column_names,
            elastic_client=elastic_client,
        )
        try:
            filter_column_query = get_search_query_for_inventory_obj_index(
                filter_columns=filter_columns, dict_of_types=dict_of_types
            )
        except HTTPException as exc:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(exc.detail)
            return

        if filter_column_query:
            search_query = filter_column_query

        if search_query["bool"].get("must", None) is None:
            search_query["bool"]["must"] = list()

        if request.mo_ids:
            mos_query = {
                "bool": {
                    "should": [{"terms": {"id": list(request.mo_ids)}}],
                    "minimum_should_match": 1,
                }
            }
            search_query["bool"]["must"].append(mos_query)

        if request.p_ids:
            mos_query = {
                "bool": {
                    "should": [{"terms": {"p_id": list(request.p_ids)}}],
                    "minimum_should_match": 1,
                }
            }
            search_query["bool"]["must"].append(mos_query)

        search_indexes = get_index_names_fo_list_of_tmo_ids([request.tmo_id])
        search_index = search_indexes

        search_params = {
            "index": search_index,
            "query": search_query,
            "track_total_hits": True,
        }

        search_res = await elastic_client.search(
            **search_params, size=2_000_000, ignore_unavailable=True
        )
        objects = [item["_source"] for item in search_res["hits"]["hits"]]

        for obj in objects:
            if request.tprm_ids:
                obj["parameters"] = {
                    param_id: param_value
                    for param_id, param_value in obj["parameters"].items()
                    if param_id in request.tprm_ids
                }
            if request.only_ids:
                obj = {"id": obj["id"]}
            obj = json.dumps(obj)

            yield mo_finder_pb2.ResponseGetMOsByFilters(mos=[obj])
