from uuid import UUID

from elasticsearch import AsyncElasticsearch

from services.hierarchy_services.elastic.configs import HIERARCHY_OBJ_INDEX
from v2.tasks.utils.exceptions import SearchNotFoundException


async def get_node_or_raise_error(
    node_id: UUID, elastic_client: AsyncElasticsearch
):
    """Returns node by node_id, otherwise raise error"""

    search_body = {"query": {"match": {"id": node_id}}}
    search_res = await elastic_client.search(
        index=HIERARCHY_OBJ_INDEX, body=search_body, ignore_unavailable=True
    )
    search_res = search_res["hits"]["hits"]
    if not search_res:
        raise SearchNotFoundException(f"Node with id = {node_id} not found")

    return search_res[0]["_source"]
