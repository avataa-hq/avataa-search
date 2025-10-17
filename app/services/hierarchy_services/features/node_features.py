from typing import List

from elasticsearch import AsyncElasticsearch

from services.hierarchy_services.elastic.configs import (
    HIERARCHY_OBJ_INDEX,
    HIERARCHY_NODE_DATA_INDEX,
)
from services.hierarchy_services.models.dto import NodeDTO


def create_path_for_children_node_by_parent_node(parent_node: NodeDTO = None):
    """Returns path for children of parent_node"""
    path = None
    if parent_node:
        parent_path = parent_node.path if parent_node.path else ""
        path = f"{parent_path}{parent_node.id}/"

    return path


def get_list_of_node_parent_ids_from_path(path: str):
    """Returns a list of parent ids from a path"""
    return [p_id for p_id in path.split("/") if p_id]


async def get_chain_of_parent_nodes(
    elastic_client: AsyncElasticsearch, node_path: str | None
) -> List[NodeDTO]:
    """Returns chain of parent nodes form top to bottom"""
    result = list()
    if not node_path:
        return result

    p_ids = get_list_of_node_parent_ids_from_path(node_path)

    size_per_step = 100_000

    for start in range(0, len(p_ids), size_per_step):
        end = start + size_per_step
        step_pids = p_ids[start:end]

        search_body = {
            "query": {"bool": {"must": [{"terms": {"id": step_pids}}]}},
            "sort": {"level": {"order": "asc"}},
            "track_total_hits": True,
            "size": size_per_step,
        }

        search_res = await elastic_client.search(
            index=HIERARCHY_OBJ_INDEX, body=search_body, ignore_unavailable=True
        )
        search_res = search_res["hits"]["hits"]

        step_res = [
            NodeDTO.model_validate(item["_source"]) for item in search_res
        ]

        if result:
            result.extend(step_res)
        else:
            result = step_res

    return result


async def get_all_mo_ids_of_special_node_id(
    elastic_client: AsyncElasticsearch, node_id: str
):
    """Returns list of all mo_ids of special node_id"""
    res = list()

    search_query = {"match": {"node_id": node_id}}
    size_per_step = 100_000

    search_body = {
        "query": search_query,
        "sort": {"id": {"order": "asc"}},
        "_source": {"includes": ["mo_id"]},
        "track_total_hits": True,
        "size": size_per_step,
    }

    while True:
        search_res = await elastic_client.search(
            index=HIERARCHY_NODE_DATA_INDEX,
            body=search_body,
            ignore_unavailable=True,
        )

        total_hits = search_res["hits"]["total"]["value"]

        search_res = search_res["hits"]["hits"]

        if res:
            res.extend([item["_source"]["mo_id"] for item in search_res])
        else:
            res = [item["_source"]["mo_id"] for item in search_res]

        if total_hits < size_per_step:
            break

        if len(search_res) == size_per_step:
            search_after = search_res[-1]["sort"]
            search_body["search_after"] = search_after
        else:
            break

    return res
