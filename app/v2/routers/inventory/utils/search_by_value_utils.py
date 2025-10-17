import math
from typing import List

from elasticsearch import AsyncElasticsearch

from elastic.config import INVENTORY_TPRM_INDEX_V2
from elastic.enum_models import LogicalOperator
from elastic.pydantic_models import SearchModel
from elastic.query_builder_service.inventory_index.search_query_builder import (
    InventoryIndexQueryBuilder,
)
from v2.routers.inventory.utils.helpers import (
    get_tprm_val_types_need_to_check_in_global_search_by_inputted_data,
    get_query_to_find_tprms_by_val_types,
    get_operator_for_global_search_by_inventory_val_type,
)
from v2.routers.inventory.utils.mo_attr_search_helper import (
    get_mo_type_and_attr_names_need_to_check_in_global_search_by_inputted_data,
)


async def get_list_of_search_models_for_search_by_value_in_tmo_scope(
    elastic_client: AsyncElasticsearch,
    tmo_ids: List[int],
    search_value: str,
    client_permissions: List[str] = None,
):
    """Returns list of Search models to obtain search result by search_value (in all mo attrs and params)
    otherwise return empty list"""
    tprm_per_step = 10000

    tprm_val_types = list(
        get_tprm_val_types_need_to_check_in_global_search_by_inputted_data(
            search_value
        )
    )
    tprms_query = get_query_to_find_tprms_by_val_types(
        val_types=tprm_val_types,
        tmo_ids=tmo_ids,
        only_returnable=False,
        client_permissions=client_permissions,
    )

    tprms_from_search = await elastic_client.search(
        index=INVENTORY_TPRM_INDEX_V2,
        query=tprms_query,
        size=tprm_per_step,
        track_total_hits=True,
    )

    tprms = tprms_from_search["hits"]["hits"]

    total_tprms_hits = tprms_from_search["hits"]["total"]["value"]
    if total_tprms_hits > tprm_per_step:
        steps = math.ceil(total_tprms_hits / tprm_per_step)

        for step in range(1, steps):
            offset = step * tprm_per_step
            step_res = await elastic_client.search(
                index=INVENTORY_TPRM_INDEX_V2,
                query=tprms_query,
                ignore_unavailable=True,
                size=tprm_per_step,
                from_=offset,
                track_total_hits=True,
            )

            step_res = step_res["hits"]["hits"]
            tprms.extend(step_res)

    inventory_search_models = []

    for tprm in tprms:
        column_type = tprm["_source"]["val_type"]
        search_operator = get_operator_for_global_search_by_inventory_val_type(
            column_type
        )
        search_model = SearchModel(
            column_name=str(tprm["_source"]["id"]),
            operator=search_operator,
            column_type=column_type,
            multiple=False,
            value=search_value,
        )

        inventory_search_models.append(search_model)

    mo_attrs = get_mo_type_and_attr_names_need_to_check_in_global_search_by_inputted_data(
        search_value
    )

    for mo_attr_name, mo_attr_val_type in mo_attrs.items():
        search_operator = get_operator_for_global_search_by_inventory_val_type(
            mo_attr_val_type
        )
        search_model = SearchModel(
            column_name=mo_attr_name,
            operator=search_operator,
            column_type=mo_attr_val_type,
            multiple=False,
            value=search_value,
        )
        inventory_search_models.append(search_model)

    return inventory_search_models


async def get_query_for_search_by_value_in_tmo_scope(
    elastic_client: AsyncElasticsearch,
    tmo_ids: List[int],
    search_value: str,
    client_permissions: List[str] = None,
):
    """Returns elastic query as dict to obtain search result by search_value (in all mo attrs and params)
    otherwise return empty dict"""
    inventory_search_models = (
        await get_list_of_search_models_for_search_by_value_in_tmo_scope(
            elastic_client=elastic_client,
            tmo_ids=tmo_ids,
            search_value=search_value,
            client_permissions=client_permissions,
        )
    )
    if inventory_search_models:
        search_query = InventoryIndexQueryBuilder(
            logical_operator=LogicalOperator.OR.value,
            search_list_order=inventory_search_models,
        )
        return search_query.create_query_as_dict()

    return dict()
