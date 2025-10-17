from abc import ABC
from typing import AsyncIterator

from elasticsearch import AsyncElasticsearch

from v3.input_parser.base_parser import Parser
from v3.models.input.operators.field_operators.base_operators import BaseLogical
from v3.models.input.operators.input_union import base_operators_union
from v3.models.input.operators.logical_operators.logical import And
from v3.query.implementations.elasticsearch_query.parser import (
    ElasticsearchQueryParser,
)
from v3.db.base_db import BaseTable, RowMapping


class EsSecuredTable(BaseTable, ABC):
    """
    Base class for Elasticsearch tables.
    """

    CHUNK_SIZE = 10_000
    DEFAULT_ORDER_BY = "id"

    parser: Parser = ElasticsearchQueryParser()

    def __init__(
        self,
        connection: AsyncElasticsearch,
        is_admin: bool = True,
        permissions: list[str] | None = None,
    ):
        self.connection = connection
        self.is_admin = is_admin
        self.permissions = permissions or []

    async def get_mapping(self) -> list[RowMapping]:
        mapping = await self.connection.indices.get_mapping(self.table_name)
        return mapping

    async def find_by_query(
        self,
        query: base_operators_union,
        includes: list[str] | None = None,
    ) -> AsyncIterator[dict]:
        """
        Basic implementation of table lookup.
        Takes filtered data as chunks and gives it back as an iterator.
        In this way, optimal memory utilization is achieved.
        If on the other side you need to get fewer items, you can simply stop getting items from the iterator.
        """
        if not isinstance(query, BaseLogical):
            query = And(value=[query])
        parsed_query = self.parser.parse(in_query=query)
        stmt = dict(
            index=self.table_name,
            query={"bool": parsed_query.create_query()},
            sort={self.DEFAULT_ORDER_BY: "ASC"},
            source_includes=includes,
            size=self.CHUNK_SIZE,
        )
        search_after = None

        last_response_size = self.CHUNK_SIZE
        while last_response_size >= self.CHUNK_SIZE:
            stmt["search_after"] = search_after
            chunk = await self.connection.search(**stmt)
            last_response_size = len(chunk["hits"]["hits"])
            for elem in chunk["hits"]["hits"]:
                yield elem["_source"]
