from collections import defaultdict
from typing import AsyncIterator

from elasticsearch import AsyncElasticsearch

from elastic.config import ALL_MO_OBJ_INDEXES_PATTERN
from security.security_data_models import UserPermission
from services.hierarchy_services.elastic.configs import (
    HIERARCHY_NODE_DATA_INDEX,
    HIERARCHY_OBJ_INDEX,
    HIERARCHY_HIERARCHIES_INDEX,
)
from services.hierarchy_services.elastic.mapping import (
    HIERARCHY_PERMISSIONS_FIELD_NAME,
)
from services.inventory_services.utils.security.filter_by_realm import (
    get_only_available_to_read_tmo_ids_for_special_client,
)


class FindMoIdsInHierarchies:
    STEP_SIZE = 100_000

    def __init__(
        self,
        elastic_client: AsyncElasticsearch,
        mo_ids: list[int],
        permission: UserPermission,
    ):
        self._elastic_client = elastic_client
        self._mo_ids = mo_ids
        self._permission = permission

    async def search_iterator(
        self,
        body: dict,
        index: str,
        ignore_unavailable: bool = True,
        size: int | None = None,
    ) -> AsyncIterator:
        step_size = size or self.STEP_SIZE
        last_response_size = step_size
        body["size"] = step_size
        while last_response_size >= step_size:
            search_res = await self._elastic_client.search(
                index=index, body=body, ignore_unavailable=ignore_unavailable
            )
            result = search_res["hits"]["hits"]

            for res in result:
                yield res

            last_response_size = len(result)
            if len(result):
                search_after = result[-1]["sort"]
                body["search_after"] = search_after

    async def _filter_mo_ids_by_mo_permission_and_group_by_tmo_id(
        self,
        mo_ids: list[int],
        permission: UserPermission,
    ) -> dict[int, list[int]]:
        filtered_mo_ids = defaultdict(list)
        if not mo_ids:
            return filtered_mo_ids

        must = [{"terms": {"id": mo_ids}}]
        if not permission.is_admin:
            must.append(
                {"terms": {"permission.keyword": permission.user_permissions}}
            )
        body = {
            "query": {"bool": {"must": must}},
            "_source": {"includes": ["id", "tmo_id"]},
            "sort": {"id": {"order": "asc"}},
        }
        index = ALL_MO_OBJ_INDEXES_PATTERN
        print(body)
        print(index)
        async for row in self.search_iterator(
            body=body, index=index, ignore_unavailable=True
        ):
            filtered_mo_ids[row["_source"]["tmo_id"]].append(
                row["_source"]["id"]
            )
        return filtered_mo_ids

    async def _filter_grouped_mo_ids_by_tmo_permission(
        self,
        mo_ids_by_tmo_ids: dict[int, list[int]],
        permission: UserPermission,
    ):
        if not mo_ids_by_tmo_ids:
            return mo_ids_by_tmo_ids
        if permission.is_admin:
            return mo_ids_by_tmo_ids

        filtered_mo_ids = {}
        tmo_ids = await get_only_available_to_read_tmo_ids_for_special_client(
            client_permissions=permission.user_permissions,
            elastic_client=self._elastic_client,
            tmo_ids=list(mo_ids_by_tmo_ids),
        )
        for tmo_id in tmo_ids:
            filtered_mo_ids[tmo_id] = mo_ids_by_tmo_ids[tmo_id]
        return filtered_mo_ids

    async def _find_node_ids_by_mo_ids(self, mo_ids: list[int]) -> list[str]:
        results = []
        if not mo_ids:
            return results
        must = [{"terms": {"mo_id": mo_ids}}]
        query = {"bool": {"must": must}}
        body = {
            "query": query,
            "_source": {"includes": "node_id"},
            "sort": {"node_id": {"order": "asc"}},
        }
        index = HIERARCHY_NODE_DATA_INDEX
        async for row in self.search_iterator(
            body=body, index=index, ignore_unavailable=True
        ):
            results.append(row["_source"]["node_id"])
        return results

    async def _get_nodes_data_by_node_ids_and_group_by_hierarchy_ids(
        self, node_ids: list[str]
    ) -> dict[int, list[dict]]:
        results = defaultdict(list)
        if not node_ids:
            return results
        must = [{"terms": {"id": node_ids}}]
        query = {"bool": {"must": must}}
        body = {
            "query": query,
            "sort": {"id": {"order": "asc"}},
        }
        index = HIERARCHY_OBJ_INDEX

        async for row in self.search_iterator(
            body=body, index=index, ignore_unavailable=True
        ):
            source = row["_source"]
            results[source["hierarchy_id"]].append(source)

        return results

    async def _filter_grouped_node_data_by_hierarchy_permission(
        self,
        nodes_data_by_hierarchy_ids: dict[int, list[dict]],
        permission: UserPermission,
    ):
        if permission.is_admin:
            return nodes_data_by_hierarchy_ids
        ids = list(nodes_data_by_hierarchy_ids.keys())
        must = [
            {"terms": {"id": ids}},
            {
                "terms": {
                    HIERARCHY_PERMISSIONS_FIELD_NAME: permission.user_permissions
                }
            },
        ]
        query = {"bool": {"must": must}}
        body = {
            "query": query,
            "_source": {"includes": "id"},
            "sort": {"id": {"order": "asc"}},
        }
        index = HIERARCHY_HIERARCHIES_INDEX

        results = {}
        async for row in self.search_iterator(
            body=body, index=index, ignore_unavailable=True
        ):
            tmo_id = row["_source"]["id"]
            results[tmo_id].append(nodes_data_by_hierarchy_ids[tmo_id])
        return results

    async def execute(self):
        mo_ids_by_tmo_ids = (
            await self._filter_mo_ids_by_mo_permission_and_group_by_tmo_id(
                mo_ids=self._mo_ids, permission=self._permission
            )
        )
        mo_ids_by_tmo_ids = await self._filter_grouped_mo_ids_by_tmo_permission(
            mo_ids_by_tmo_ids=mo_ids_by_tmo_ids, permission=self._permission
        )
        mo_ids = [
            value for values in mo_ids_by_tmo_ids.values() for value in values
        ]
        node_ids = await self._find_node_ids_by_mo_ids(mo_ids=mo_ids)
        nodes_data_by_hierarchy_ids = (
            await self._get_nodes_data_by_node_ids_and_group_by_hierarchy_ids(
                node_ids=node_ids
            )
        )
        nodes_data_by_hierarchy_ids = (
            await self._filter_grouped_node_data_by_hierarchy_permission(
                nodes_data_by_hierarchy_ids=nodes_data_by_hierarchy_ids,
                permission=self._permission,
            )
        )
        results = [
            value
            for values in nodes_data_by_hierarchy_ids.values()
            for value in values
        ]
        return results
