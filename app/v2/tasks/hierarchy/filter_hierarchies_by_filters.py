import uuid

from elasticsearch import AsyncElasticsearch

from elastic.config import INVENTORY_TPRM_INDEX_V2
from elastic.pydantic_models import HierarchyFilter
from indexes_mapping.inventory.mapping import INVENTORY_PERMISSIONS_FIELD_NAME
from security.security_data_models import UserPermission
from services.hierarchy_services.features.dependet_level_factory import (
    DependentLevelsChainFactory,
    LevelDataForChain,
    DependentLevelsChain,
)
from services.hierarchy_services.models.dto import HierarchyDTO, NodeDTO
from services.inventory_services.utils.security.filter_by_realm import (
    get_only_available_to_read_tmo_ids_for_special_client,
)
from utils_by_services.hierarchy.level_cond_query_handler import (
    LevelConditionsOrderHandler,
    LevelConditionsOrderResults,
)
from utils_by_services.hierarchy.level_condition_grouper import (
    InventoryLevelConditionGrouper,
)
from utils_by_services.hierarchy.node_checker import NodeCheckerWithFilters
from v2.tasks.hierarchy.utils.get_hierarchy_with_permission_check_or_raise_error import (
    get_hierarchy_with_permission_check_or_raise_error,
)
from v2.tasks.hierarchy.utils.get_node_or_raise_error import (
    get_node_or_raise_error,
)
from v2.tasks.utils.exceptions import (
    SearchPermissionException,
    SearchValueError,
)


class FilterHierarchy:
    def __init__(
        self,
        elastic_client: AsyncElasticsearch,
        user_permissions: UserPermission,
        hierarchy_id: int,
        filters: list[HierarchyFilter] = None,
        parent_node_id: str = None,
    ):
        self._elastic_client = elastic_client
        self._user_permissions = user_permissions
        self._hierarchy_id = hierarchy_id
        self._filters = filters
        self._parent_node_id = parent_node_id

        self.__parent_node_id = ...
        self.__parent_node = ...
        self.__hierarchy = ...

    async def execute(self) -> None | LevelConditionsOrderResults:
        await self.check_permissions(user_permission=self._user_permissions)
        parent_node = await self.get_parent_node()
        hierarchy = await self.get_hierarchy()
        chain_of_p_levels = await self.get_chain_of_p_levels(
            parent_node=parent_node, hierarchy=hierarchy
        )
        if not chain_of_p_levels:
            return None
        level_conditions = await self.get_level_conditions(
            chain_of_p_levels=chain_of_p_levels,
            parent_node=parent_node,
            hierarchy=hierarchy,
        )
        return level_conditions

    async def check_permissions(self, user_permission: UserPermission) -> None:
        if user_permission.is_admin:
            return
        all_tprms = set()
        all_tmos = set()
        if self._filters:
            for h_filter in self._filters:
                if not h_filter.filter_columns:
                    continue
                f_tprms = {
                    int(f_c.column_name)
                    for f_c in h_filter.filter_columns
                    if f_c.column_name.isdigit()
                }
                all_tprms.update(f_tprms)
                all_tmos.add(h_filter.tmo_id)
        if all_tprms:
            search_body = {
                "query": {
                    "bool": {
                        "must": [
                            {"terms": {"id": list(all_tprms)}},
                            {
                                "terms": {
                                    INVENTORY_PERMISSIONS_FIELD_NAME: user_permission.user_permissions
                                }
                            },
                        ]
                    }
                },
                "size": len(all_tprms),
                "_source": {"includes": "id"},
            }

            search_res = await self._elastic_client.search(
                index=INVENTORY_TPRM_INDEX_V2,
                body=search_body,
                ignore_unavailable=True,
            )

            search_res = search_res["hits"]["hits"]
            available_tprms = {item["_source"]["id"] for item in search_res}

            difference = all_tprms.difference(available_tprms)
            if difference:
                raise SearchPermissionException(
                    f"User has no permissions for tprms: {difference}"
                )

        if all_tmos:
            available_tmos = (
                await get_only_available_to_read_tmo_ids_for_special_client(
                    client_permissions=user_permission.user_permissions,
                    elastic_client=self._elastic_client,
                    tmo_ids=list(all_tmos),
                )
            )
            difference = set(all_tmos).difference(available_tmos)
            if difference:
                raise SearchPermissionException(
                    f"User has no permissions for tmos: {difference}"
                )

    @property
    def parent_node_id(self) -> uuid.UUID | None:
        if self.__parent_node_id is ...:
            if not self._parent_node_id:
                parent_node_id = None
            else:
                if self._parent_node_id.upper() in ("ROOT", "NONE", "NULL"):
                    parent_node_id = None
                else:
                    try:
                        parent_node_id = uuid.UUID(self._parent_node_id)
                    except ValueError:
                        raise SearchValueError(
                            "parent_node_id must be instance of UUID or be one of values: root, none, null"
                        )
            self.__parent_node_id = parent_node_id
        return self.__parent_node_id

    async def get_parent_node(self) -> NodeDTO | None:
        if self.__parent_node is ...:
            parent_node_id = self.parent_node_id
            parent_node = None
            if isinstance(parent_node_id, uuid.UUID):
                parent_node = await get_node_or_raise_error(
                    node_id=parent_node_id, elastic_client=self._elastic_client
                )
                node_hierarchy_id_as_int = int(parent_node["hierarchy_id"])
                if node_hierarchy_id_as_int != self._hierarchy_id:
                    raise SearchValueError(
                        "Parent node hierarchy id does not match hierarchy_id "
                        f"({node_hierarchy_id_as_int} != {self._hierarchy_id})"
                    )
                is_active = parent_node.get("active")
                if not is_active:
                    raise SearchValueError("Parent node is not active")
            self.__parent_node = (
                NodeDTO.model_validate(parent_node) if parent_node else None
            )
        return self.__parent_node

    async def get_hierarchy(self):
        if self.__hierarchy is ...:
            hierarchy = (
                await get_hierarchy_with_permission_check_or_raise_error(
                    hierarchy_id=self._hierarchy_id,
                    user_permission=self._user_permissions,
                    elastic_client=self._elastic_client,
                )
            )
            self.__hierarchy = HierarchyDTO.model_validate(hierarchy)
        return self.__hierarchy

    async def get_chain_of_p_levels(
        self,
        parent_node: NodeDTO | None,
        hierarchy: HierarchyDTO,
    ) -> DependentLevelsChain | None:
        parent_level_data = (
            LevelDataForChain(
                level_id=parent_node.level_id,
                level_level=int(parent_node.level),
            )
            if parent_node
            else None
        )
        factory_dep_level = DependentLevelsChainFactory(
            self._elastic_client,
            self._hierarchy_id,
            parent_level_data=parent_level_data,
        )

        chain_of_p_levels = (
            await factory_dep_level.create_dependent_levels_chain_by()
        )

        if parent_node:
            checker = NodeCheckerWithFilters(
                node=parent_node,
                filters=self._filters,
                hierarchy=hierarchy,
                elastic_client=self._elastic_client,
                dependent_levels_chain=chain_of_p_levels,
                user_permissions=self._user_permissions,
            )
            check_res = await checker.check()
            if (
                check_res.full_match_filter_condition is False
                and not check_res.mo_ids_matched_conditions
            ):
                return None

        return chain_of_p_levels

    async def get_level_conditions(
        self,
        chain_of_p_levels: DependentLevelsChain,
        hierarchy: HierarchyDTO,
        parent_node: NodeDTO | None,
    ) -> None | LevelConditionsOrderResults:
        order_builder = InventoryLevelConditionGrouper(
            depends_level_chain=chain_of_p_levels,
            hierarchy=hierarchy,
            hierarchy_filters=self._filters,
            aggregation=None,
        )
        order_to_process = order_builder.get_order_to_process()
        if not order_to_process:
            return None

        handler = LevelConditionsOrderHandler(
            level_cond_order=order_to_process,
            elastic_client=self._elastic_client,
            parent_node_dto=parent_node,
            user_permissions=self._user_permissions,
        )

        handler_res = await handler.process()
        return handler_res
