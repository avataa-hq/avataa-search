import dataclasses
from abc import ABC, abstractmethod
from typing import AsyncIterator

from v3.input_parser.base_parser import Parser
from v3.models.input.operators.input_union import base_operators_union
from v3.value_types.value_types import ValueType


@dataclasses.dataclass
class RowMapping:
    column: str
    val_type: ValueType


class BaseTable(ABC):
    """
    Abstract base class that defines common methods for all tables.
    """

    def __init__(self, *args, **kwargs):
        raise NotImplementedError

    @property
    @abstractmethod
    def parser(self) -> Parser:
        """
        Abstract property that defines the parser of the query.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def table_name(self) -> str:
        """
        Used for the base class in the function @find_by_query
        """
        raise NotImplementedError

    @abstractmethod
    async def get_mapping(self) -> list[RowMapping]:
        """
        Can be used for type conversion or checking the existence of a field
        """
        raise NotImplementedError

    @abstractmethod
    async def find_by_query(
        self, query: base_operators_union, includes: list[str] | None = None
    ) -> AsyncIterator[dict]:
        """
        Abstract method that finds rows matching given query (BaseOperator | field).
        """
        raise NotImplementedError
