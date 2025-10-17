from typing import List, Any

from elasticsearch import AsyncElasticsearch

from elastic.config import INVENTORY_TPRM_INDEX_V2
from elastic.enum_models import InventoryFieldValType
from elastic.query_builder_service.inventory_index.utils.search_utils import (
    get_field_name_for_tprm_id,
)
from indexes_mapping.inventory.mapping import INVENTORY_PERMISSIONS_FIELD_NAME
from security.security_data_models import UserPermission
from utils_by_services.inventory.common import search_after
from v2.routers.inventory.utils.helpers import (
    get_query_to_find_tprms_by_val_types,
)


async def get_list_of_ids_of_unavailable_tprms(
    elastic_client: AsyncElasticsearch,
    user_permissions: UserPermission,
    tmo_id: int,
) -> List[str]:
    """Returns list of unavailable tprms by user permissions"""

    if user_permissions.is_admin:
        return list()
    size_per_step = 100_000
    search_body = {
        "query": {
            "bool": {
                "must": [{"match": {"tmo_id": tmo_id}}],
                "must_not": [
                    {
                        "terms": {
                            INVENTORY_PERMISSIONS_FIELD_NAME: user_permissions.user_permissions
                        }
                    }
                ],
            }
        },
        "sort": {"level": {"order": "desc"}},
        "track_total_hits": True,
        "size": size_per_step,
        "_source": {"includes": "id"},
    }

    res_list = list()

    while True:
        search_res = await elastic_client.search(
            index=INVENTORY_TPRM_INDEX_V2,
            body=search_body,
            ignore_unavailable=True,
        )

        total_hits = search_res["hits"]["total"]["value"]

        search_res = search_res["hits"]["hits"]

        if not res_list:
            res_list = [item["_source"]["id"] for item in search_res]
        else:
            res_list.extend([item["_source"]["id"] for item in search_res])

        if total_hits < size_per_step:
            break

        if len(search_res) == size_per_step:
            search_after = search_res[-1]["sort"]
            search_body["search_after"] = search_after
        else:
            break
    return res_list


async def get_list_of_unavailable_parameters(
    elastic_client: AsyncElasticsearch,
    user_permissions: UserPermission,
    tmo_id: int,
) -> List[str]:
    """Returns list of unavailable parameters by user permissions"""

    if user_permissions.is_admin:
        return list()
    size_per_step = 100_000
    search_body = {
        "query": {
            "bool": {
                "must": [{"match": {"tmo_id": tmo_id}}],
                "must_not": [
                    {
                        "terms": {
                            INVENTORY_PERMISSIONS_FIELD_NAME: user_permissions.user_permissions
                        }
                    }
                ],
            }
        },
        "sort": {"id": {"order": "desc"}},
        "track_total_hits": True,
        "size": size_per_step,
        "_source": {"includes": "id"},
    }

    res_list = list()

    while True:
        search_res = await elastic_client.search(
            index=INVENTORY_TPRM_INDEX_V2,
            body=search_body,
            ignore_unavailable=True,
        )

        total_hits = search_res["hits"]["total"]["value"]

        search_res = search_res["hits"]["hits"]

        if not res_list:
            res_list = [
                get_field_name_for_tprm_id(item["_source"]["id"])
                for item in search_res
            ]
        else:
            res_list.extend(
                [
                    get_field_name_for_tprm_id(item["_source"]["id"])
                    for item in search_res
                ]
            )

        if total_hits < size_per_step:
            break

        if len(search_res) == size_per_step:
            search_after = search_res[-1]["sort"]
            search_body["search_after"] = search_after
        else:
            break
    return res_list


async def get_list_of_mo_link_tprm(
    elastic_client: AsyncElasticsearch,
    is_admin: bool,
    user_permissions: list[str],
    tmo_ids: list[int] = None,
    returned_columns: list[str] = None,
    sorting: dict = None,
) -> list[Any]:
    """Return list of tprm for mo_link"""
    if is_admin:
        tprms_query = get_query_to_find_tprms_by_val_types(
            val_types=[
                InventoryFieldValType.MO_LINK.value,
                InventoryFieldValType.TWO_WAY_MO_LINK.value,
            ],
            only_returnable=False,
            tmo_ids=tmo_ids,
        )
    else:
        tprms_query = get_query_to_find_tprms_by_val_types(
            val_types=[
                InventoryFieldValType.MO_LINK.value,
                InventoryFieldValType.TWO_WAY_MO_LINK.value,
            ],
            only_returnable=False,
            client_permissions=user_permissions,
            tmo_ids=tmo_ids,
        )
    if user_permissions:
        # TODO : remove comment to enable tprm permissions
        # tprms_query.append({"terms": {INVENTORY_PERMISSIONS_FIELD_NAME: user_permissions}})
        pass
    body = {
        "query": tprms_query,
        "track_total_hits": True,
    }
    if returned_columns:
        body["_source"] = {"includes": returned_columns}
    if sorting:
        body["sort"] = sorting
    output = await search_after(
        elastic_client=elastic_client, index=INVENTORY_TPRM_INDEX_V2, body=body
    )
    return output
