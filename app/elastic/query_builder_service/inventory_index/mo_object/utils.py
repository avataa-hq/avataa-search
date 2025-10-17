from typing import Iterable
from fastapi import HTTPException
from elasticsearch import AsyncElasticsearch

from elastic.config import INVENTORY_TPRM_INDEX_V2
from elastic.enum_models import LogicalOperator
from elastic.pydantic_models import (
    SortColumn,
    FilterColumn,
    SortModel,
    SearchModel,
)
from elastic.query_builder_service.inventory_index.search_query_builder import (
    InventoryIndexQueryBuilder,
)
from elastic.query_builder_service.inventory_index.sort_query_builder import (
    InventorySortQueryBuilder,
)
from indexes_mapping.inventory.mapping import INVENTORY_OBJ_INDEX_MAPPING
from services.inventory_services.converters.val_type_converter import (
    get_corresponding_python_val_type_for_elastic_val_type,
)


async def get_dict_of_inventory_attr_and_params_types(
    array_of_attr_and_params_names: Iterable, elastic_client: AsyncElasticsearch
) -> dict:
    """Returns dict of MO attrs and params ids and their val_type and multiple configs"""
    ids_of_parameters_in_filters = [
        column_name
        for column_name in array_of_attr_and_params_names
        if column_name.isdigit()
    ]

    exclude_types = {"object", "flattened"}
    # dict mo attrs and parameters and their configs (val_type, multiple)
    field_value_dict = dict()
    for attr_name, data in INVENTORY_OBJ_INDEX_MAPPING["properties"].items():
        if data["type"] not in exclude_types:
            corresp_type = (
                get_corresponding_python_val_type_for_elastic_val_type(
                    data["type"]
                )
            )
            field_value_dict[attr_name] = {
                "val_type": corresp_type,
                "multiple": False,
            }

    if ids_of_parameters_in_filters:
        query = {
            "bool": {"must": [{"terms": {"id": ids_of_parameters_in_filters}}]}
        }
        size = len(ids_of_parameters_in_filters)

        res = await elastic_client.search(
            index=INVENTORY_TPRM_INDEX_V2,
            query=query,
            size=size,
            source_includes=["id", "multiple", "val_type"],
        )

        for item in res["hits"]["hits"]:
            item = item["_source"]
            field_value_dict[str(item["id"])] = {
                "val_type": item["val_type"],
                "multiple": item["multiple"],
            }

    return field_value_dict


def get_sort_query_for_inventory_obj_index(
    sort_columns: list[SortColumn], dict_of_types: dict
) -> list[dict]:
    """Returns sort query for elasticsearch client as dict."""
    sort_models = []

    for sort_column in sort_columns:
        sort_column_data = dict_of_types.get(sort_column.column_name)
        if sort_column_data is None:
            raise HTTPException(
                status_code=400,
                detail=f"Can`t create sort query for column named "
                f"{sort_column.column_name}",
            )
        sort_model = SortModel(
            column_name=sort_column.column_name,
            ascending=sort_column.ascending,
            column_type=sort_column_data["val_type"],
        )

        sort_models.append(sort_model)

    sort_query = InventorySortQueryBuilder(sort_order_list=sort_models)
    return sort_query.create_sort_list_as_list_of_dicts()


def get_search_query_for_inventory_obj_index(
    filter_columns: list[FilterColumn], dict_of_types: dict
) -> dict:
    """Returns search query for elasticsearch client as dict."""

    join_queries = []
    search_models = []

    for filter_column in filter_columns:
        filter_column_data = dict_of_types.get(filter_column.column_name)
        if filter_column_data is None:
            raise HTTPException(
                status_code=400,
                detail=f"Can`t create search query for column named "
                f"{filter_column.column_name}",
            )

        if len(filter_column.filters) == 1:
            for filter_item in filter_column.filters:
                search_model = SearchModel(
                    column_name=filter_column.column_name,
                    operator=filter_item.operator,
                    value=filter_item.value,
                    column_type=filter_column_data["val_type"],
                    multiple=filter_column_data["multiple"],
                )
                search_models.append(search_model)

        elif len(filter_column.filters) > 1:
            inner_search_models = list()
            for filter_item in filter_column.filters:
                search_model = SearchModel(
                    column_name=filter_column.column_name,
                    operator=filter_item.operator,
                    value=filter_item.value,
                    column_type=filter_column_data["val_type"],
                    multiple=filter_column_data["multiple"],
                )
                inner_search_models.append(search_model)

            inner_query = InventoryIndexQueryBuilder(
                logical_operator=filter_column.rule,
                search_list_order=inner_search_models,
            )

            join_queries.append(inner_query.create_query_as_dict())

    main_query = dict()
    if search_models:
        main_query = InventoryIndexQueryBuilder(
            logical_operator=LogicalOperator.AND.value,
            search_list_order=search_models,
        )
        main_query = main_query.create_query_as_dict()

    if join_queries:
        inner_dict = main_query.get("bool")
        if inner_dict is None:
            main_query["bool"] = dict()
            inner_dict = main_query.get("bool")

        for query_as_dict in join_queries:
            for k, v in query_as_dict["bool"].items():
                values_for_elastic_q_operator = inner_dict.get(k)
                if values_for_elastic_q_operator is None:
                    inner_dict[k] = v
                else:
                    values_for_elastic_q_operator.extend(v)

    return main_query
