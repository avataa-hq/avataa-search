from elastic.config import (
    INVENTORY_INDEX,
    PARAMS_INDEX,
    TMO_INDEX,
    HIERARCHY_INDEX,
    PERMISSION_INDEX,
)


def correct_date(objects: list[dict]):
    for obj in objects:
        if "Inspection Date" in obj.keys():
            if "AM" in obj["Inspection Date"]:
                obj["Inspection Date"] = obj["Inspection Date"].replace(
                    " AM", ""
                )
            if "PM" in obj["Inspection Date"]:
                obj["Inspection Date"] = obj["Inspection Date"].replace(
                    " PM", ""
                )

    return objects


def permission_type_mapper(type_):
    mapper = {
        "mo": "objects",
        "tmo": "object_type",
        "tprm": "param_type",
        "object_type": "tmo",
        "objects": "mo",
        "param_type": "tprm",
    }

    return mapper[type_]


def generate_docs(data: list[dict], index: str):
    for idx, item in enumerate(data):
        doc = {"_index": index, "_source": cleaners[index](item)}
        yield doc


def inventory_index_cleaner(item: dict):
    return {
        "tmo_id": item["tmo_id"],
        "active": item["active"],
        "p_id": item["p_id"],
        "latitude": item["latitude"],
        "longitude": item["longitude"],
        "description": item["description"],
        "point_a_id": item["point_a_id"],
        "point_b_id": item["point_b_id"],
        "status": item["status"],
        "model": item["model"],
        "id": item["id"],
        "geometry": item["geometry"],
        "version": item["version"],
        "pov": item["pov"],
        "name": item["name"],
        "params": item["params"],
    }


cleaners = {
    INVENTORY_INDEX: inventory_index_cleaner,
    PARAMS_INDEX: lambda x: x,
    TMO_INDEX: lambda x: x,
    HIERARCHY_INDEX: lambda x: x,
    PERMISSION_INDEX: lambda x: x,
}
