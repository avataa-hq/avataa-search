from collections import defaultdict
from typing import List

from elasticsearch import AsyncElasticsearch

from elastic.pydantic_models import AggregationByRanges
from elastic.query_builder_service.inventory_index.mo_object.utils import (
    get_dict_of_inventory_attr_and_params_types,
    get_search_query_for_inventory_obj_index,
)
from elastic.query_builder_service.inventory_index.utils.index_utils import (
    get_index_name_by_tmo,
)
from elastic.query_builder_service.inventory_index.utils.search_utils import (
    get_field_name_for_tprm_id,
)


class AggregationsByRangesHandler:
    def __init__(
        self,
        tmo_id: int | str,
        mo_ids: List[str | int],
        aggregation: AggregationByRanges,
        elastic_client: AsyncElasticsearch,
    ):
        self.mo_ids = mo_ids
        self.aggregation = aggregation
        self.tmo_id = tmo_id
        self.elastic_client = elastic_client

    async def get_results(self):
        main_must_condition = [{"terms": {"id": self.mo_ids}}]

        aggr_filters = dict()

        all_inventory_filter_column_names = {"id"}
        for aggr_item in self.aggregation.aggr_items:
            all_inventory_filter_column_names.update(
                {f_c.column_name for f_c in aggr_item.aggr_filters}
            )

        dict_of_types = await get_dict_of_inventory_attr_and_params_types(
            array_of_attr_and_params_names=list(
                all_inventory_filter_column_names
            ),
            elastic_client=self.elastic_client,
        )

        for aggr_item in self.aggregation.aggr_items:
            filter_search_query = get_search_query_for_inventory_obj_index(
                filter_columns=list(aggr_item.aggr_filters),
                dict_of_types=dict_of_types,
            )
            aggr_filters[aggr_item.aggr_name] = filter_search_query
        additional_aggr_exist = True
        if (
            not self.aggregation.aggr_by_tprm_id
            or not self.aggregation.aggregation_type
        ):
            additional_aggr_exist = False

        if additional_aggr_exist:
            parameter_name = get_field_name_for_tprm_id(
                self.aggregation.aggr_by_tprm_id
            )
            search_body = {
                "size": 0,
                "query": {"bool": {"must": main_must_condition}},
                "aggs": {
                    "f": {
                        "filters": {"filters": aggr_filters},
                        "aggs": {
                            "tprm_aggr": {
                                self.aggregation.aggregation_type: {
                                    "field": parameter_name
                                }
                            }
                        },
                    }
                },
            }
        else:
            search_body = {
                "size": 0,
                "query": {"bool": {"must": main_must_condition}},
                "aggs": {"f": {"filters": {"filters": aggr_filters}}},
            }
        index_name = get_index_name_by_tmo(self.tmo_id)

        agg_res = await self.elastic_client.search(
            index=index_name, body=search_body, ignore_unavailable=True
        )
        agg_res = agg_res["aggregations"]["f"]["buckets"]

        res = defaultdict(dict)
        if additional_aggr_exist:
            for aggr_name, aggr_res in agg_res.items():
                res[aggr_name]["doc_count"] = aggr_res["doc_count"]
                res[aggr_name]["value"] = aggr_res["tprm_aggr"]["value"]
        else:
            for aggr_name, aggr_res in agg_res.items():
                res[aggr_name]["doc_count"] = aggr_res["doc_count"]

        return res
