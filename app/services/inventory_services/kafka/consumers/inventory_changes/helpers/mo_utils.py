from ast import literal_eval
from collections import defaultdict
from typing import Any

from elasticsearch import AsyncElasticsearch
from elasticsearch._async.helpers import async_bulk
from elasticsearch.helpers import BulkIndexError

from elastic.config import (
    INVENTORY_MO_LINK_INDEX,
    ALL_MO_OBJ_INDEXES_PATTERN,
    INVENTORY_PRM_LINK_INDEX,
)
from elastic.query_builder_service.inventory_index.utils.index_utils import (
    get_index_name_by_tmo,
)
from indexes_mapping.inventory.mapping import INVENTORY_PARAMETERS_FIELD_NAME
from services.inventory_services.kafka.consumers.inventory_changes.helpers.common_utils import (
    get_all_data_from_special_index,
)
from services.inventory_services.kafka.consumers.inventory_changes.helpers.models import (
    PRMLinkChangeData,
    MOLinkChangeData,
)
from services.inventory_services.utils.common.validation import (
    geometry_validation,
)


async def change_value_of_mo_linked_values(
    mo_with_changed_names: dict[int, dict], async_client: AsyncElasticsearch
) -> None:
    if not mo_with_changed_names:
        return

    search_body = {"query": {"terms": {"value": list(mo_with_changed_names)}}}

    mo_links_results = await get_all_data_from_special_index(
        index=INVENTORY_MO_LINK_INDEX,
        body=search_body,
        results_as_dict=False,
        async_client=async_client,
    )

    if not mo_links_results:
        return

    all_mo_links_values = set()
    for mo_link in mo_links_results:
        value = mo_link.get("value")

        if isinstance(value, list):
            all_mo_links_values.update(value)
        elif isinstance(value, int):
            all_mo_links_values.add(value)

    for mo_id, mo_data in mo_with_changed_names.items():
        new_name = mo_data.get("name")

        if mo_id not in all_mo_links_values:
            continue

        search_body = {"query": {"terms": {"value": [mo_id]}}}
        spec_mo_links = await get_all_data_from_special_index(
            index=INVENTORY_MO_LINK_INDEX,
            body=search_body,
            results_as_dict=True,
            async_client=async_client,
        )

        data_grouped_by_mo_id = defaultdict(list)

        for mo_link_prm_id, mo_link_prm in spec_mo_links.items():
            mo_link_prm_mo_id = mo_link_prm.get("mo_id")
            mo_link_prm_tprm_id = mo_link_prm.get("tprm_id")
            mo_link_prm_value = mo_link_prm.get("value")

            data_for_instance = {
                "prm_id": mo_link_prm_id,
                "tprm_id": mo_link_prm_tprm_id,
                "multiple": False,
            }

            if isinstance(mo_link_prm_value, list):
                data_for_instance["multiple"] = True

                data_for_instance["index_of_prm_in_array"] = (
                    mo_link_prm_value.index(mo_id)
                )

            data_grouped_by_mo_id[mo_link_prm_mo_id].append(
                MOLinkChangeData(**data_for_instance)
            )

        # check if mo links in prm links
        mo_link_ids_in_prm_links = (
            await get_prm_ids_that_contained_in_prm_links(
                prm_ids=list(spec_mo_links), async_client=async_client
            )
        )

        prm_id_new_value = dict()

        search_body = {"query": {"terms": {"id": list(data_grouped_by_mo_id)}}}

        all_mo = await async_client.search(
            index=ALL_MO_OBJ_INDEXES_PATTERN,
            body=search_body,
            ignore_unavailable=True,
            size=len(data_grouped_by_mo_id),
        )

        all_mo = all_mo["hits"]["hits"]

        actions = []

        for mo in all_mo:
            mo = mo["_source"]
            mo_id = mo.get("id")
            mo_parameters = mo.get(INVENTORY_PARAMETERS_FIELD_NAME)
            data_to_update = data_grouped_by_mo_id.get(mo_id)

            if not mo_parameters:
                mo[INVENTORY_PARAMETERS_FIELD_NAME] = dict()
                mo_parameters = mo.get(INVENTORY_PARAMETERS_FIELD_NAME)

            if not data_to_update:
                continue

            for update_tprm_data in data_to_update:
                prm_new_value = None
                if update_tprm_data.multiple is False:
                    mo_parameters[str(update_tprm_data.tprm_id)] = new_name
                    prm_new_value = new_name

                else:
                    parameter_from_mo = mo_parameters.get(
                        str(update_tprm_data.tprm_id)
                    )
                    if not isinstance(parameter_from_mo, list):
                        mo_parameters[str(update_tprm_data.tprm_id)] = new_name

                        prm_new_value = new_name

                    else:
                        if (
                            len(parameter_from_mo)
                            >= update_tprm_data.index_of_prm_in_array
                        ):
                            parameter_from_mo[
                                update_tprm_data.index_of_prm_in_array
                            ] = new_name

                        else:
                            parameter_from_mo.append(new_name)

                        prm_new_value = parameter_from_mo

                # if in prm_links
                if update_tprm_data.prm_id in mo_link_ids_in_prm_links:
                    prm_id_new_value[update_tprm_data.prm_id] = prm_new_value

            index_name = get_index_name_by_tmo(mo["tmo_id"])

            action_item = dict(
                _index=index_name, _op_type="index", _id=mo_id, _source=mo
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

        # if there are mo_links in prm_links and they have new values
        if prm_id_new_value:
            for prm_id, prm_value in prm_id_new_value.items():
                await update_mo_data_if_id_of_changed_prm_in_prm_links(
                    prm_id=prm_id,
                    prm_value=prm_value,
                    async_client=async_client,
                )


async def get_prm_ids_that_contained_in_prm_links(
    prm_ids: list[int], async_client: AsyncElasticsearch
) -> set:
    """Returns prm_data if prm_ids contained in prm_links prms"""

    if not prm_ids:
        return set()

    search_body = {
        "query": {"terms": {"value": prm_ids}},
        "_source": {"includes": ["value"]},
    }

    prm_link_prms = await get_all_data_from_special_index(
        index=INVENTORY_PRM_LINK_INDEX,
        body=search_body,
        results_as_dict=False,
        async_client=async_client,
    )

    set_of_prm_ids = set(prm_ids)

    set_of_values = set()

    for prm_v in prm_link_prms:
        v = prm_v.get("value")
        if v:
            if isinstance(v, list):
                set_of_values.update(v)
            else:
                set_of_values.add(v)

    return set_of_values.intersection(set_of_prm_ids)


async def update_mo_data_if_id_of_changed_prm_in_prm_links(
    prm_id: int, prm_value: Any, async_client: AsyncElasticsearch
):
    search_body = {"query": {"terms": {"value": [prm_id]}}}

    prm_link_prms = await get_all_data_from_special_index(
        index=INVENTORY_PRM_LINK_INDEX,
        body=search_body,
        results_as_dict=False,
        async_client=async_client,
    )

    if not prm_link_prms:
        return

    data_grouped_by_mo_id = defaultdict(list)

    for prm_link_prm in prm_link_prms:
        prm_link_prm_mo_id = prm_link_prm.get("mo_id")
        prm_link_prm_tprm_id = prm_link_prm.get("tprm_id")
        prm_link_prm_value = prm_link_prm.get("value")

        data_for_instance = {"tprm_id": prm_link_prm_tprm_id, "multiple": False}

        if isinstance(prm_link_prm_value, list):
            data_for_instance["multiple"] = True

            data_for_instance["index_of_prm_in_array"] = (
                prm_link_prm_value.index(prm_id)
            )

        data_grouped_by_mo_id[prm_link_prm_mo_id].append(
            PRMLinkChangeData(**data_for_instance)
        )

    search_body = {"query": {"terms": {"id": list(data_grouped_by_mo_id)}}}

    all_mo = await async_client.search(
        index=ALL_MO_OBJ_INDEXES_PATTERN,
        body=search_body,
        ignore_unavailable=True,
        size=len(data_grouped_by_mo_id),
    )

    all_mo = all_mo["hits"]["hits"]

    actions = []

    for mo in all_mo:
        mo = mo["_source"]
        mo_id = mo.get("id")
        mo_parameters = mo.get(INVENTORY_PARAMETERS_FIELD_NAME)
        data_to_update = data_grouped_by_mo_id.get(mo_id)

        if not mo_parameters:
            mo[INVENTORY_PARAMETERS_FIELD_NAME] = dict()
            mo_parameters = mo.get(INVENTORY_PARAMETERS_FIELD_NAME)

        if not data_to_update:
            continue

        for update_tprm_data in data_to_update:
            if update_tprm_data.multiple is False:
                mo_parameters[str(update_tprm_data.tprm_id)] = prm_value

            else:
                parameter_from_mo = mo_parameters.get(
                    str(update_tprm_data.tprm_id)
                )
                if not isinstance(parameter_from_mo, list):
                    mo_parameters[str(update_tprm_data.tprm_id)] = prm_value
                else:
                    if (
                        len(parameter_from_mo)
                        >= update_tprm_data.index_of_prm_in_array
                    ):
                        parameter_from_mo[
                            update_tprm_data.index_of_prm_in_array
                        ] = prm_value
                    else:
                        parameter_from_mo.append(prm_value)

        index_name = get_index_name_by_tmo(mo["tmo_id"])

        action_item = dict(
            _index=index_name, _op_type="index", _id=mo_id, _source=mo
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


def normalize_geometry(geometry: dict) -> dict | None:
    if not isinstance(geometry, dict):
        return None

    path = geometry.get("path")
    geojson_type = geometry.get("type", None)
    path_data = {}

    if path:
        path_data = check_geometry_path(path)
    elif geojson_type is not None and isinstance(geojson_type, str):
        path_data = check_geometry_geojson(geojson_type, geometry)
    if path_data and geometry_validation(path_data):
        return {"path": path_data}
    return None


def check_geometry_path(path) -> dict:
    match path:
        case str():
            return {
                "type": "MultiPoint",
                "coordinates": literal_eval(path),
            }
        case list():
            return {
                "type": "MultiPoint",
                "coordinates": path,
            }
        case dict():
            return path
        case _:
            raise ValueError("Incorrect Path type in geometry.")


def check_geometry_geojson(geojson_type: str, geometry: dict) -> dict | None:
    match geojson_type:
        case "FeatureCollection":
            features = geometry.get("features", [])
            if isinstance(features, list):
                geometries = [
                    f.get("geometry")
                    for f in features
                    if f.get("type") == "Feature"
                    and isinstance(f.get("geometry"), dict)
                ]
                if geometries:
                    return geometries[0]
                return None
            return None
        case "Feature":
            geom = geometry.get("geometry")
            if geom and isinstance(geom, dict):
                return geom
            return None
        case (
            "Point"
            | "LineString"
            | "Polygon"
            | "MultiPoint"
            | "MultiLineString"
            | "MultiPolygon"
        ):
            if "coordinates" in geometry:
                return {
                    "type": geometry["type"],
                    "coordinates": geometry["coordinates"],
                }
            return None
        case "GeometryCollection":
            geoms = geometry.get("geometries", [])
            if isinstance(geoms, list) and geoms:
                return geoms[0]
            return None
        case _:
            return None
