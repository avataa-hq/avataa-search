from copy import deepcopy
from typing import AsyncIterator, TYPE_CHECKING

from elasticsearch import AsyncElasticsearch

from v3.models.input.operators.field import field
from v3.models.input.operators.field_operators.comparison import In
from v3.models.input.operators.logical_operators.logical import And
from v3.db.implementation.es.es_secured_db import EsSecuredTable


if TYPE_CHECKING:
    from v3.models.input.operators.input_union import base_operators_union
    from v3.db.base_db import BaseTable


"""
Realization of search by hierarchy tables taking into account user access rights
"""


class HierarchiesEsSecuredTable(EsSecuredTable):
    TABLE_PREFIX = "hierarchy_hierarchies_index"

    def __init__(
        self,
        connection: AsyncElasticsearch,
        is_admin: bool = True,
        permissions: list[str] | None = None,
    ):
        super().__init__(connection, is_admin=is_admin, permissions=permissions)

    @property
    def table_name(self) -> str:
        return self.TABLE_PREFIX

    async def find_by_query(
        self, query: "base_operators_union", includes: list[str] | None = None
    ) -> AsyncIterator[dict]:
        """
        Supplement the query, if necessary, by checking user rights and execute the basic query of the parent class
        """
        query = deepcopy(query)
        if not self.is_admin:
            if isinstance(query, field):
                query = And(
                    value=[query, field(permissions=In(value=self.permissions))]
                )
            else:
                # if LogicalBase
                query = And(
                    value=[
                        query,
                        And(
                            value=[
                                field(permissions=self.permissions),
                            ]
                        ),
                    ]
                )
        async for elem in super().find_by_query(query=query, includes=includes):
            yield elem


class LevelsEsSecuredTable(EsSecuredTable):
    TABLE_PREFIX = "hierarchy_levels_index"

    def __init__(
        self,
        connection: AsyncElasticsearch,
        tmo_table: "BaseTable",  # Necessary to verify user rights
        is_admin: bool = True,
        permissions: list[str] | None = None,
    ):
        super().__init__(connection, is_admin=is_admin, permissions=permissions)
        self.tmo_table = tmo_table

    @property
    def table_name(self) -> str:
        return self.TABLE_PREFIX

    async def filter_by_tmo_permission(
        self, buffer: list[dict], tmo_ids_buffer: set[int]
    ) -> AsyncIterator[dict]:
        if self.is_admin:
            for elem in buffer:
                yield elem
            return
        query = field(id=In(value=list(tmo_ids_buffer)))
        permitted_tmo_ids = set(
            [
                i
                async for i in self.tmo_table.find_by_query(
                    query=query, includes=["id"]
                )
            ]
        )
        for elem in buffer:
            if elem["id"] in permitted_tmo_ids:
                yield elem
        buffer.clear()
        tmo_ids_buffer.clear()

    async def find_by_query(
        self, query: "base_operators_union", includes: list[str] | None = None
    ) -> AsyncIterator[dict]:
        """
        Do a post-check of user rights by TMO. Check permissions in chunks for more processing speed
        """
        query = deepcopy(query)
        if self.is_admin:
            async for elem in super().find_by_query(
                query=query, includes=includes
            ):
                yield elem
        else:
            buffer = []
            tmo_ids_buffer = set()
            async for elem in super().find_by_query(
                query=query, includes=includes
            ):
                buffer.append(elem)
                tmo_ids_buffer.add(elem["id"])
                if len(buffer) >= self.CHUNK_SIZE:
                    async for elem_perm in self.filter_by_tmo_permission(
                        buffer=buffer, tmo_ids_buffer=tmo_ids_buffer
                    ):
                        yield elem_perm
            if buffer:
                async for elem_perm in self.filter_by_tmo_permission(
                    buffer=buffer, tmo_ids_buffer=tmo_ids_buffer
                ):
                    yield elem_perm


class NodesEsSecuredTable(EsSecuredTable):
    TABLE_PREFIX = "hierarchy_node_data_index"

    def __init__(
        self,
        connection: AsyncElasticsearch,
        mo_table: "BaseTable",  # Necessary to verify user rights
        is_admin: bool = True,
        permissions: list[str] | None = None,
    ):
        super().__init__(connection, is_admin=is_admin, permissions=permissions)
        self.mo_table = mo_table

    @property
    def table_name(self) -> str:
        return self.TABLE_PREFIX

    async def filter_by_mo_permission(
        self, buffer: list[dict], mo_ids_buffer: set[int]
    ) -> AsyncIterator[dict]:
        if self.is_admin:
            for elem in buffer:
                yield elem
            return
        query = field(id=In(value=list(mo_ids_buffer)))
        permitted_mo_ids = set(
            [
                i
                async for i in self.mo_table.find_by_query(
                    query=query, includes=["id"]
                )
            ]
        )
        for elem in buffer:
            if elem["id"] in permitted_mo_ids:
                yield elem
        buffer.clear()
        mo_ids_buffer.clear()

    async def find_by_query(
        self, query: "base_operators_union", includes: list[str] | None = None
    ) -> AsyncIterator[dict]:
        """
        Do a post-check of user rights by MO. Check permissions in chunks for more processing speed
        """
        query = deepcopy(query)
        if self.is_admin:
            async for elem in super().find_by_query(
                query=query, includes=includes
            ):
                yield elem
        else:
            buffer = []
            mo_ids_buffer = set()
            async for elem in super().find_by_query(
                query=query, includes=includes
            ):
                buffer.append(elem)
                mo_ids_buffer.add(elem["id"])
                if len(buffer) >= self.CHUNK_SIZE:
                    async for elem_perm in self.filter_by_mo_permission(
                        buffer=buffer, mo_ids_buffer=mo_ids_buffer
                    ):
                        yield elem_perm
            if buffer:
                async for elem_perm in self.filter_by_mo_permission(
                    buffer=buffer, mo_ids_buffer=mo_ids_buffer
                ):
                    yield elem_perm


class ObjectsEsSecuredTable(EsSecuredTable):
    TABLE_PREFIX = "hierarchy_obj_index"

    def __init__(
        self,
        connection: AsyncElasticsearch,
        nodes_table: "BaseTable",  # Necessary to verify user rights
        is_admin: bool = True,
        permissions: list[str] | None = None,
    ):
        super().__init__(connection, is_admin=is_admin, permissions=permissions)
        self.nodes_table = nodes_table

    @property
    def table_name(self) -> str:
        return self.TABLE_PREFIX

    async def filter_by_obj_permission(
        self, buffer: list[dict], obj_ids_buffer: set[int]
    ) -> AsyncIterator[dict]:
        if self.is_admin:
            for elem in buffer:
                yield elem
            return
        query = field(parent_id=In(value=list(obj_ids_buffer)))
        permitted_mo_ids = set(
            await self.nodes_table.find_by_query(
                query=query, includes=["parent_id"]
            )
        )
        for elem in buffer:
            if elem["id"] in permitted_mo_ids:
                yield elem
        buffer.clear()
        obj_ids_buffer.clear()

    async def find_by_query(
        self, query: "base_operators_union", includes: list[str] | None = None
    ) -> AsyncIterator[dict]:
        """
        Do a post-check of user rights by Node. Check permissions in chunks for more processing speed
        """
        query = deepcopy(query)
        if self.is_admin:
            async for elem in super().find_by_query(
                query=query, includes=includes
            ):
                yield elem
        else:
            buffer = []
            obj_ids_buffer = set()
            async for elem in super().find_by_query(
                query=query, includes=includes
            ):
                buffer.append(elem)
                obj_ids_buffer.add(elem["id"])
                if len(buffer) >= self.CHUNK_SIZE:
                    async for elem_perm in self.filter_by_obj_permission(
                        buffer=buffer, obj_ids_buffer=obj_ids_buffer
                    ):
                        yield elem_perm
            if buffer:
                async for elem_perm in self.filter_by_obj_permission(
                    buffer=buffer, obj_ids_buffer=obj_ids_buffer
                ):
                    yield elem_perm
