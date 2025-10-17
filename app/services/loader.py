from elasticsearch import Elasticsearch
from services.hierarchy import load_hierarchies
from services.inventory import (
    load_inventory_objects,
    load_params,
    load_tmo,
    load_permissions,
)


def load_objects(client: Elasticsearch, type_: str):
    match type_:
        case "inventory":
            load_inventory_objects(client)
        case "params":
            load_params(client)
        case "tmo":
            load_tmo(client)
        case "hierarchy":
            load_hierarchies(client)
        case "permission":
            load_permissions(client)
