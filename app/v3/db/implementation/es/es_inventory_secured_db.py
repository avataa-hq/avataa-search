from copy import deepcopy
from typing import AsyncIterator

from elasticsearch import AsyncElasticsearch

from v3.models.input.operators.field import field
from v3.models.input.operators.field_operators.comparison import In
from v3.models.input.operators.input_union import base_operators_union
from v3.models.input.operators.logical_operators.logical import And
from v3.db.implementation.es.es_secured_db import EsSecuredTable


"""
Realization of search by inventory tables taking into account user access rights
"""


class TmoEsSecuredTable(EsSecuredTable):
    TABLE_PREFIX = "inventory_tmo_index"

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
        self, query: base_operators_union, includes: list[str] | None = None
    ) -> AsyncIterator[dict]:
        """
        Supplement the query, if necessary, by checking user rights and execute the basic query of the parent class
        """
        query = deepcopy(query)
        if not self.is_admin:
            query = And(
                value=[query, field(permissions=In(value=self.permissions))]
            )
        async for elem in super().find_by_query(query=query, includes=includes):
            yield elem


class MoEsSecuredTable(EsSecuredTable):
    TABLE_PREFIX = "inventory_obj_tmo_"

    def __init__(
        self,
        connection: AsyncElasticsearch,
        table_id,
        is_admin: bool = True,
        permissions: list[str] | None = None,
    ):
        super().__init__(connection, is_admin=is_admin, permissions=permissions)
        self.table_id = table_id

    @property
    def table_name(self) -> str:
        return f"{self.TABLE_PREFIX}{self.table_id}_index"

    async def find_by_query(
        self, query: base_operators_union, includes: list[str] | None = None
    ) -> AsyncIterator[dict]:
        """
        Supplement the query, if necessary, by checking user rights and execute the basic query of the parent class
        """
        query = deepcopy(query)
        if not self.is_admin:
            query = And(
                value=[query, field(permissions=In(value=self.permissions))]
            )
        async for elem in super().find_by_query(query=query, includes=includes):
            yield elem


class TprmEsSecuredTable(EsSecuredTable):
    TABLE_PREFIX = "inventory_tprm_index"

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
        self, query: base_operators_union, includes: list[str] | None = None
    ) -> AsyncIterator[dict]:
        """
        Supplement the query, if necessary, by checking user rights and execute the basic query of the parent class
        """
        query = deepcopy(query)
        if not self.is_admin:
            query = And(value=[query, field(permissions=self.permissions)])
        async for elem in super().find_by_query(query=query, includes=includes):
            yield elem
