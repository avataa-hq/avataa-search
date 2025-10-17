from elasticsearch import AsyncElasticsearch

from elasticsearch.helpers import BulkIndexError
from elasticsearch._async.helpers import async_bulk

from elastic.config import INVENTORY_OBJ_INDEX_PREFIX, INVENTORY_TMO_INDEX_V2
from elastic.query_builder_service.inventory_index.utils.index_utils import (
    get_index_name_by_tmo,
)
from indexes_mapping.inventory.mapping import (
    INVENTORY_PARAMETERS_FIELD_NAME,
    INVENTORY_FUZZY_FIELD_NAME,
)
from services.inventory_services.kafka.consumers.inventory_changes.helpers.mo_utils import (
    change_value_of_mo_linked_values,
    normalize_geometry,
)
from services.inventory_services.models import InventoryFuzzySearchFields


async def on_create_mo(msg, async_client: AsyncElasticsearch):
    tmo_ids = set()
    mo_p_ids = set()
    mo_point_a_ids = set()
    mo_point_b_ids = set()
    for mo_data in msg["objects"]:
        tmo_ids.add(mo_data["tmo_id"])
        mo_p_id = mo_data.get("p_id")
        if mo_p_id:
            mo_p_ids.add(mo_p_id)

        point_a_id = mo_data.get("point_a_id")
        point_b_id = mo_data.get("point_b_id")
        if point_a_id:
            mo_point_a_ids.add(point_a_id)
        if point_b_id:
            mo_point_b_ids.add(point_b_id)

    # get existing tmos
    search_query = {"terms": {"id": list(tmo_ids)}}

    existing_indexes_result = await async_client.search(
        index=INVENTORY_TMO_INDEX_V2, query=search_query, size=len(tmo_ids)
    )

    existing_tmo_id_names = {
        item["_source"]["id"]: get_index_name_by_tmo(item["_source"]["id"])
        for item in existing_indexes_result["hits"]["hits"]
    }

    # get existing parent_names
    existing_mo_names = dict()
    if mo_p_ids or mo_point_a_ids or mo_point_b_ids:
        list_of_ids = list(
            set().union(mo_p_ids, mo_point_a_ids, mo_point_b_ids)
        )

        search_query = {"terms": {"id": list_of_ids}}
        all_mo_indexes = f"{INVENTORY_OBJ_INDEX_PREFIX}*"
        existing_mo_p_id = await async_client.search(
            index=all_mo_indexes, query=search_query, size=len(list_of_ids)
        )
        existing_mo_names = {
            item["_source"]["id"]: item["_source"].get("name")
            for item in existing_mo_p_id["hits"]["hits"]
        }

    actions = list()
    for mo_data in msg["objects"]:
        tmo_id_of_mo = mo_data["tmo_id"]
        mo_data["parent_name"] = None
        # add parameters field
        mo_data[INVENTORY_PARAMETERS_FIELD_NAME] = {}
        if tmo_id_of_mo in existing_tmo_id_names:
            index_name = existing_tmo_id_names[tmo_id_of_mo]
            parent_id = mo_data.get("p_id")
            if parent_id:
                parent_name = existing_mo_names.get(parent_id)
                if parent_name:
                    mo_data["parent_name"] = parent_name

            point_a_id = mo_data.get("point_a_id")
            if point_a_id:
                mo_data["point_a_name"] = existing_mo_names.get(point_a_id)

            point_b_id = mo_data.get("point_b_id")
            if point_b_id:
                mo_data["point_b_name"] = existing_mo_names.get(point_b_id)

            geometry = mo_data.get("geometry")
            normalized = normalize_geometry(geometry)
            if normalized:
                mo_data["geometry"] = normalized
            else:
                mo_data.pop("geometry", None)

            # add fields for fuzzy search
            fuzzy_search_data = dict()
            for enum_item in InventoryFuzzySearchFields:
                field_name = enum_item.value
                field_value = mo_data.get(field_name)
                if field_value:
                    fuzzy_search_data[field_name] = field_value

            if fuzzy_search_data:
                mo_data[INVENTORY_FUZZY_FIELD_NAME] = fuzzy_search_data

            action_item = dict(
                _index=index_name,
                _op_type="index",
                _id=mo_data["id"],
                _source=mo_data,
            )
            actions.append(action_item)
    if actions:
        try:
            print(actions)
            await async_bulk(
                client=async_client, refresh="true", actions=actions
            )
        except BulkIndexError as e:
            print(e.errors)
            raise e


async def on_update_mo(msg, async_client: AsyncElasticsearch):
    mo_ids_mo_data = {item["id"]: item for item in msg["objects"]}

    # get existing mos
    search_query = {"terms": {"id": list(mo_ids_mo_data.keys())}}
    all_mo_indexes = f"{INVENTORY_OBJ_INDEX_PREFIX}*"
    existing_indexes_result = await async_client.search(
        index=all_mo_indexes, query=search_query, size=len(mo_ids_mo_data)
    )
    cache_mo_data_from_search = {
        item["_source"]["id"]: item["_source"]
        for item in existing_indexes_result["hits"]["hits"]
    }

    items_with_updated_names = dict()
    items_with_updated_p_ids = dict()
    items_with_updated_point_a = dict()
    items_with_updated_point_b = dict()
    tmo_ids_of_existing_mo = set()

    for mo_id, mo_data in cache_mo_data_from_search.items():
        tmo_ids_of_existing_mo.add(mo_data["tmo_id"])

        mo_from_kafka = mo_ids_mo_data[mo_id]

        if mo_from_kafka["name"] != mo_data.get("name"):
            items_with_updated_names[mo_id] = mo_from_kafka

        p_id_of_mo_from_kafka = mo_from_kafka.get("p_id")
        p_id_of_mo_data = mo_data.get("p_id")
        if p_id_of_mo_from_kafka != p_id_of_mo_data:
            items_with_updated_p_ids[mo_id] = mo_from_kafka

        if "point_a_id" in mo_from_kafka:
            point_a_from_kafka = mo_from_kafka["point_a_id"]
            point_a_from_elastic = mo_data.get("point_a_id")
            if point_a_from_kafka != point_a_from_elastic:
                items_with_updated_point_a[mo_id] = mo_from_kafka

        if "point_b_id" in mo_from_kafka:
            point_b_from_kafka = mo_from_kafka["point_b_id"]
            point_b_from_elastic = mo_data.get("point_b_id")
            if point_b_from_kafka != point_b_from_elastic:
                items_with_updated_point_b[mo_id] = mo_from_kafka

    tmo_id_index_name = {
        tmo_id: get_index_name_by_tmo(tmo_id)
        for tmo_id in tmo_ids_of_existing_mo
    }

    if items_with_updated_names:
        list_of_ids_with_updated_names = list(items_with_updated_names)

        # update point_a_name and point_b_name if mo with changed names exist in point_a_id point_b_id
        search_query = {
            "bool": {
                "should": [
                    {"terms": {"point_a_id": list_of_ids_with_updated_names}},
                    {"terms": {"point_b_id": list_of_ids_with_updated_names}},
                ],
                "minimum_should_match": 1,
            }
        }

        exist_in_points = await async_client.search(
            index=all_mo_indexes,
            track_total_hits=True,
            query=search_query,
            size=0,
        )

        if exist_in_points["hits"]["total"]["value"]:
            for mo_id, mo_data in items_with_updated_names.items():
                new_name = mo_data.get("name")
                # update point_a_name
                search_query = {"match": {"point_a_id": mo_id}}
                update_script = {
                    "source": "ctx._source.point_a_name = params['new_point_a_name'];",
                    "lang": "painless",
                    "params": {"new_point_a_name": new_name},
                }

                await async_client.update_by_query(
                    index=all_mo_indexes,
                    query=search_query,
                    script=update_script,
                    refresh=True,
                )

                # update point_b_name
                search_query = {"match": {"point_b_id": mo_id}}
                update_script = {
                    "source": "ctx._source.point_b_name = params['new_point_b_name'];",
                    "lang": "painless",
                    "params": {"new_point_b_name": new_name},
                }

                await async_client.update_by_query(
                    index=all_mo_indexes,
                    query=search_query,
                    script=update_script,
                    refresh=True,
                )

        # change parent name for all children

        # objects with children
        find_children_query = {
            "bool": {
                "must": [
                    {"exists": {"field": "p_id"}},
                    {"terms": {"p_id": list(items_with_updated_names)}},
                ]
            }
        }

        agg_query = {"children_by_mo_id": {"terms": {"field": "p_id"}}}

        children_res = await async_client.search(
            size=0,
            index=all_mo_indexes,
            query=find_children_query,
            aggs=agg_query,
        )

        children_res_by_p_id = children_res["aggregations"][
            "children_by_mo_id"
        ]["buckets"]

        for mo_id_with_children in children_res_by_p_id:
            p_id = mo_id_with_children["key"]
            p_id_query = {
                "bool": {
                    "must": [
                        {"exists": {"field": "p_id"}},
                        {"match": {"p_id": p_id}},
                    ]
                }
            }
            new_parent_name = items_with_updated_names[p_id]["name"]

            update_script = {
                "source": "ctx._source.parent_name = params['new_parent_name']",
                "lang": "painless",
                "params": {"new_parent_name": new_parent_name},
            }
            await async_client.update_by_query(
                index=all_mo_indexes,
                query=p_id_query,
                script=update_script,
                refresh=True,
            )
        # update values of mo links
        await change_value_of_mo_linked_values(
            mo_with_changed_names=items_with_updated_names,
            async_client=async_client,
        )

    cache_of_new_parent_names = {}
    if (
        items_with_updated_p_ids
        or items_with_updated_point_a
        or items_with_updated_point_b
    ):
        set_of_p_ids = {
            v.get("p_id")
            for v in items_with_updated_p_ids.values()
            if v.get("p_id")
        }
        set_of_point_a_ids = {
            p_a_id
            for v in items_with_updated_point_a.values()
            if (p_a_id := v.get("point_a_id"))
        }
        set_of_point_b_ids = {
            p_b_id
            for v in items_with_updated_point_b.values()
            if (p_b_id := v.get("point_b_id"))
        }

        list_of_ids = list(
            set().union(set_of_p_ids, set_of_point_a_ids, set_of_point_b_ids)
        )

        if list_of_ids:
            search_query = {"terms": {"id": list_of_ids}}

            search_res = await async_client.search(
                index=all_mo_indexes, query=search_query, size=len(list_of_ids)
            )

            cache_of_new_parent_names = {
                item["_source"]["id"]: item["_source"].get("name")
                for item in search_res["hits"]["hits"]
            }

    actions = list()
    for mo_id in cache_mo_data_from_search.keys():
        mo_data = mo_ids_mo_data[mo_id]

        # if changed p_id change parent_name

        if mo_id in items_with_updated_p_ids:
            mo_p_id = mo_data.get("p_id")
            mo_data["parent_name"] = cache_of_new_parent_names.get(mo_p_id)

        # if changed point_a_id change point_a_parent_name
        if mo_id in items_with_updated_point_a:
            point_a_id = mo_data.get("point_a_id")
            mo_data["point_a_name"] = cache_of_new_parent_names.get(point_a_id)

        # if changed point_b_id change point_b_parent_name
        if mo_id in items_with_updated_point_b:
            point_b_id = mo_data.get("point_b_id")
            mo_data["point_b_name"] = cache_of_new_parent_names.get(point_b_id)
        try:
            index_name = tmo_id_index_name[mo_data["tmo_id"]]
        except KeyError:
            print(tmo_id_index_name)
            print(mo_data)
            raise

        geometry = mo_data.get("geometry")
        normalized = normalize_geometry(geometry)
        if normalized:
            mo_data["geometry"] = normalized
        else:
            mo_data.pop("geometry", None)

        # add fields for fuzzy search
        fuzzy_search_data = dict()
        for enum_item in InventoryFuzzySearchFields:
            field_name = enum_item.value
            field_value = mo_data.get(field_name)
            fuzzy_search_data[field_name] = field_value

        if fuzzy_search_data:
            mo_data[INVENTORY_FUZZY_FIELD_NAME] = fuzzy_search_data
        else:
            mo_data[INVENTORY_FUZZY_FIELD_NAME] = None

        action_item = dict(
            _index=index_name, _op_type="update", _id=mo_id, doc=mo_data
        )
        actions.append(action_item)

    if actions:
        try:
            await async_bulk(
                client=async_client, refresh="true", actions=actions
            )
        except BulkIndexError as e:
            print(e.errors)
            raise e


async def on_delete_mo(msg, async_client: AsyncElasticsearch):
    mo_ids = [item["id"] for item in msg["objects"]]

    delete_query = {"terms": {"id": mo_ids}}
    all_mo_indexes = f"{INVENTORY_OBJ_INDEX_PREFIX}*"
    try:
        await async_client.delete_by_query(
            index=all_mo_indexes,
            query=delete_query,
            ignore_unavailable=True,
            scroll="1s",
            scroll_size=5000,
            search_timeout="1s",
            slices=1,
            refresh=True,
        )
    except Exception as ex:
        print(f"On delete mo: {type(ex)}: {ex}, {len(mo_ids)=}")

    search_query = {
        "bool": {
            "must": [{"exists": {"field": "p_id"}}, {"terms": {"p_id": mo_ids}}]
        }
    }

    update_script = {
        "source": "ctx._source.parent_name = params['new_parent_name'];"
        "ctx._source.p_id = params['new_parent_name']",
        "lang": "painless",
        "params": {"new_parent_name": None},
    }

    await async_client.update_by_query(
        index=all_mo_indexes,
        query=search_query,
        script=update_script,
        # scroll="1s",
        # scroll_size=1,
        # search_timeout="1s",
        slices=1,
        refresh=True,
    )

    # update point_a_name and point_b_name

    search_query = {
        "bool": {
            "should": [
                {"terms": {"point_a_id": mo_ids}},
                {"terms": {"point_b_id": mo_ids}},
            ],
            "minimum_should_match": 1,
        }
    }

    exist_in_points = await async_client.search(
        index=all_mo_indexes, track_total_hits=True, query=search_query, size=0
    )

    if exist_in_points["hits"]["total"]["value"]:
        # update point_a_name
        search_query = {"terms": {"point_a_id": mo_ids}}
        update_script = {
            "source": "ctx._source.point_a_name = params['new_point_a_name'];",
            "lang": "painless",
            "params": {"new_point_a_name": None},
        }

        await async_client.update_by_query(
            index=all_mo_indexes,
            query=search_query,
            script=update_script,
            # scroll="1s",
            # scroll_size=1,
            # search_timeout="1s",
            slices=1,
            refresh=True,
        )

        # update point_b_name
        search_query = {"terms": {"point_b_id": mo_ids}}
        update_script = {
            "source": "ctx._source.point_b_name = params['new_point_b_name'];",
            "lang": "painless",
            "params": {"new_point_b_name": None},
        }

        await async_client.update_by_query(
            index=all_mo_indexes,
            query=search_query,
            script=update_script,
            # scroll="1s",
            # scroll_size=1,
            # search_timeout="1s",
            slices=1,
            refresh=True,
        )
