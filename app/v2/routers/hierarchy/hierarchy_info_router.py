from collections import defaultdict
from typing import List

from elasticsearch import AsyncElasticsearch
from fastapi import APIRouter, Depends, Query

from common_utils.dto_models.models import AllResAsListQueryModel
from common_utils.features.utils import (
    get_count_and_all_items_as_list_from_special_index,
)
from elastic.client import get_async_client
from elastic.config import (
    INVENTORY_TMO_INDEX_V2,
    INVENTORY_TPRM_INDEX_V2,
    ALL_MO_OBJ_INDEXES_PATTERN,
)
from elastic.enum_models import InventoryFieldValType
from indexes_mapping.inventory.mapping import INVENTORY_PARAMETERS_FIELD_NAME

from security.security_data_models import UserData
from services.hierarchy_services.elastic.configs import (
    HIERARCHY_HIERARCHIES_INDEX,
    HIERARCHY_LEVELS_INDEX,
    HIERARCHY_OBJ_INDEX,
)
from services.hierarchy_services.elastic.mapping import (
    HIERARCHY_PERMISSIONS_FIELD_NAME,
)

from services.inventory_services.utils.security.filter_by_realm import (
    check_permission_is_admin,
    get_permissions_from_client_role,
    raise_forbidden_ex_if_user_has_no_permission,
)

from v2.routers.hierarchy.configs import HIERARCHY_INFO_ROUTER_PREFIX
from security.security_factory import security

router = APIRouter(
    prefix=HIERARCHY_INFO_ROUTER_PREFIX,
    tags=["Hierarchy indexes: Hierarchy info"],
)


@router.get("/children_mo_ids_of_particular_hierarchy", status_code=200)
async def get_children_mo_ids_of_particular_hierarchy(
    hierarchy_ids: List[int] = Query(min_length=1),
    elastic_client: AsyncElasticsearch = Depends(get_async_client),
    user_data: UserData = Depends(security),
):
    is_admin = check_permission_is_admin(client_role=user_data.realm_access)
    user_permissions = get_permissions_from_client_role(
        client_role=user_data.realm_access
    )
    if not is_admin:
        raise_forbidden_ex_if_user_has_no_permission(
            client_permissions=user_permissions
        )

    search_conditions = [{"terms": {"id": hierarchy_ids}}]

    if not is_admin:
        # TODO : remove comment to enable permissions
        # search_conditions.append({"terms": {HIERARCHY_PERMISSIONS_FIELD_NAME: user_permissions}})
        pass

    sort_cond = {"id": {"order": "asc"}}

    search_query = {"bool": {"must": search_conditions}}

    query_model = AllResAsListQueryModel(
        query_condition={"query": search_query},
        sort_cond={"sort": sort_cond},
        source_conditions={
            "_source": {"excludes": [HIERARCHY_PERMISSIONS_FIELD_NAME]}
        },
    )

    res = await get_count_and_all_items_as_list_from_special_index(
        index=HIERARCHY_HIERARCHIES_INDEX,
        query_model=query_model,
        async_client=elastic_client,
    )

    all_hierarchies = res.items
    hierarchy_id_hierarchy_data = {h["id"]: h for h in all_hierarchies}

    # get all levels grouped by hierarchy_id ordered by id
    size_per_step = 100_000
    search_body = {
        "size": size_per_step,
        "query": {"terms": {"hierarchy_id": list(hierarchy_id_hierarchy_data)}},
        "sort": [{"hierarchy_id": "asc"}, {"level": "asc"}],
        "track_total_hits": True,
    }
    result_dict = defaultdict(list)

    while True:
        search_res = await elastic_client.search(
            index=HIERARCHY_LEVELS_INDEX,
            body=search_body,
            ignore_unavailable=True,
        )

        total_hits = search_res["hits"]["total"]["value"]

        search_res = search_res["hits"]["hits"]
        for item in search_res:
            item = item["_source"]
            result_dict[item["hierarchy_id"]].append(item)

        if total_hits < size_per_step:
            break

        if len(search_res) == size_per_step:
            search_after = search_res[-1]["sort"]
            search_body["search_after"] = search_after
        else:
            break

    # get all obj_ids grouped by hierarchy id
    main_res = defaultdict(set)

    for hierarchy_id, levels in result_dict.items():
        hierarchy = hierarchy_id_hierarchy_data.get(hierarchy_id)
        if not hierarchy:
            continue

        must_search_conditions_common = [
            {"match": {"hierarchy_id": hierarchy["id"]}},
            {"exists": {"field": "object_id"}},
            {"exists": {"field": "active"}},
            {"match": {"active": True}},
        ]

        if hierarchy["create_empty_nodes"] is False:
            must_search_conditions_common.append(
                {"match": {"key_is_empty": False}}
            )
        should_conditions = list()
        first = True
        obj_type_ids_was_processed = set()
        for level in levels:
            if level.get("is_virtual"):
                continue

            level_object_type = level.get("object_type_id")
            if (
                not level_object_type
                or level_object_type in obj_type_ids_was_processed
            ):
                continue

            must_condition = list()
            show_without_children = level.get("show_without_children")
            if show_without_children is False:
                must_condition = [
                    {"exists": {"field": "child_count"}},
                    {"range": {"child_count": {"gt": 0}}},
                ]
            if first:
                if must_condition:
                    must_condition.append({"match": {"level_id": level["id"]}})
                    should_conditions.append({"bool": {"must": must_condition}})
                first = False
                continue

            must_condition.extend(
                [
                    {"exists": {"field": "parent_id"}},
                    {"match": {"level_id": level["id"]}},
                ]
            )
            should_conditions.append({"bool": {"must": must_condition}})

            obj_type_ids_was_processed.add(level_object_type)

        size_per_step = 300_000

        search_query = {"bool": {"must": must_search_conditions_common}}

        if should_conditions:
            bool_cond = search_query["bool"]
            bool_cond["should"] = should_conditions
            bool_cond["minimum_should_match"] = 1

        search_body = {
            "query": search_query,
            "_source": {"includes": "object_id"},
            "track_total_hits": True,
            "sort": {"id": "asc"},
            "size": size_per_step,
        }

        while True:
            search_res = await elastic_client.search(
                index=HIERARCHY_OBJ_INDEX,
                body=search_body,
                ignore_unavailable=True,
            )

            total_hits = search_res["hits"]["total"]["value"]

            search_res = search_res["hits"]["hits"]

            main_res[hierarchy_id].update(
                [int(item["_source"]["object_id"]) for item in search_res]
            )

            if total_hits < size_per_step:
                break

            if len(search_res) == size_per_step:
                search_after = search_res[-1]["sort"]
                search_body["search_after"] = search_after
            else:
                break

    return main_res


@router.get("/count_children_with_lifecycle_and_max_severity", status_code=200)
async def get_count_children_with_lifecycle_and_max_severity_by_hierarchy_ids(
    hierarchy_ids: List[int] = Query(min_length=1),
    elastic_client: AsyncElasticsearch = Depends(get_async_client),
    user_data: UserData = Depends(security),
):
    empty_resp = {}

    is_admin = check_permission_is_admin(client_role=user_data.realm_access)
    user_permissions = get_permissions_from_client_role(
        client_role=user_data.realm_access
    )
    if not is_admin:
        raise_forbidden_ex_if_user_has_no_permission(
            client_permissions=user_permissions
        )

    search_conditions = [{"terms": {"id": hierarchy_ids}}]

    if not is_admin:
        # TODO : remove comment to enable permissions
        # search_conditions.append({"terms": {HIERARCHY_PERMISSIONS_FIELD_NAME: user_permissions}})
        pass

    sort_cond = {"id": {"order": "asc"}}

    search_query = {"bool": {"must": search_conditions}}

    query_model = AllResAsListQueryModel(
        query_condition={"query": search_query},
        sort_cond={"sort": sort_cond},
        source_conditions={
            "_source": {"excludes": [HIERARCHY_PERMISSIONS_FIELD_NAME]}
        },
    )

    res = await get_count_and_all_items_as_list_from_special_index(
        index=HIERARCHY_HIERARCHIES_INDEX,
        query_model=query_model,
        async_client=elastic_client,
    )

    all_hierarchies = res.items
    if all_hierarchies.count == 0:
        return empty_resp
    hierarchy_ids = [h["id"] for h in all_hierarchies]

    # get all tmo with lifecycle
    sort_cond = {"id": {"order": "asc"}}

    search_query = {
        "bool": {
            "must": [{"exists": {"field": "lifecycle_process_definition"}}]
        }
    }
    query_model = AllResAsListQueryModel(
        query_condition={"query": search_query},
        sort_cond={"sort": sort_cond},
        source_conditions={"_source": {"includes": "id"}},
    )
    all_tmo_with_lifecycle = (
        await get_count_and_all_items_as_list_from_special_index(
            index=INVENTORY_TMO_INDEX_V2,
            query_model=query_model,
            async_client=elastic_client,
        )
    )

    if all_tmo_with_lifecycle.count == 0:
        return empty_resp

    # get levels with this tmo grouped by hierarchy_id
    tmo_ids = [tmo["id"] for tmo in all_tmo_with_lifecycle.items]

    sort_cond = {"id": {"order": "asc"}}

    search_query = {
        "bool": {
            "must": [
                {"terms": {"object_type_id": tmo_ids}},
                {"terms": {"hierarchy_id": hierarchy_ids}},
                {"match": {"is_virtual": False}},
            ]
        }
    }
    size_per_step = 100_000
    search_body = {
        "size": size_per_step,
        "query": search_query,
        "sort": sort_cond,
        "_source": {"includes": ["hierarchy_id", "object_type_id"]},
        "track_total_hits": True,
    }

    hierarchy_object_types = defaultdict(set)
    object_type_ids = set()

    while True:
        search_res = await elastic_client.search(
            index=HIERARCHY_LEVELS_INDEX,
            body=search_body,
            ignore_unavailable=True,
        )

        total_hits = search_res["hits"]["total"]["value"]

        search_res = search_res["hits"]["hits"]

        for item in search_res:
            item = item["_source"]
            object_type_id = int(item["object_type_id"])
            hierarchy_object_types[item["hierarchy_id"]].add(object_type_id)
            object_type_ids.add(object_type_id)

        if total_hits < size_per_step:
            break

        if len(search_res) == size_per_step:
            search_after = search_res[-1]["sort"]
            search_body["search_after"] = search_after
        else:
            break

    if not object_type_ids:
        return empty_resp

    # get all severity tprms for object_types
    sort_cond = {"id": {"order": "asc"}}

    search_query = {
        "bool": {
            "must": [
                {"terms": {"tmo_id": list(object_type_ids)}},
                {
                    "wildcard": {
                        "name": {
                            "value": "*severity*",
                            "case_insensitive": True,
                        }
                    }
                },
                {
                    "terms": {
                        "val_type": [
                            InventoryFieldValType.INT.value,
                            InventoryFieldValType.FLOAT.value,
                        ]
                    }
                },
            ]
        }
    }
    size_per_step = 100_000
    search_body = {
        "size": size_per_step,
        "query": search_query,
        "sort": sort_cond,
        "_source": {"includes": ["id", "tmo_id"]},
        "track_total_hits": True,
    }

    object_type_severity_id = dict()

    while True:
        search_res = await elastic_client.search(
            index=INVENTORY_TPRM_INDEX_V2,
            body=search_body,
            ignore_unavailable=True,
        )

        total_hits = search_res["hits"]["total"]["value"]

        search_res = search_res["hits"]["hits"]

        for item in search_res:
            item = item["_source"]
            object_type_severity_id[item["tmo_id"]] = item["id"]

        if total_hits < size_per_step:
            break

        if len(search_res) == size_per_step:
            search_after = search_res[-1]["sort"]
            search_body["search_after"] = search_after
        else:
            break

    if not object_type_severity_id:
        return empty_resp
    # get max_value for each tmo_id

    aggregations = dict()
    for object_type_id, tprm_id in object_type_severity_id.items():
        aggregations[f"{tprm_id}"] = {
            "max": {"field": f"{INVENTORY_PARAMETERS_FIELD_NAME}.{tprm_id}"}
        }

    search_conditions = {"terms": {"tmo_id": list(object_type_severity_id)}}
    aggregations["count_by_tmo_id"] = {"terms": {"field": "tmo_id"}}

    request_body = {"query": search_conditions, "size": 0, "aggs": aggregations}

    aggr_res = await elastic_client.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN, body=request_body
    )

    aggr_res = aggr_res["aggregations"]

    count_per_tmo_id = {
        item["key"]: item["doc_count"]
        for item in aggr_res["count_by_tmo_id"]["buckets"]
    }

    result = dict()

    for h_id, set_of_tmo_ids in hierarchy_object_types.items():
        data_dict = {"count": 0, "severity": None}
        for tmo_id in set_of_tmo_ids:
            severity_tprm_id = object_type_severity_id.get(tmo_id)
            if not severity_tprm_id:
                continue

            max_severity = aggr_res.get(str(severity_tprm_id))
            if not max_severity:
                continue

            severity = data_dict["severity"]
            if severity is None or severity < max_severity:
                data_dict["severity"] = max_severity

            count = count_per_tmo_id.get(tmo_id)
            if count:
                data_dict["count"] += count
        if data_dict["count"] > 0:
            result[int(h_id)] = data_dict

    return result
