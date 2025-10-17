from abc import ABC

from v3.query.base_query.operators.base_operator import BaseOperator


class BaseLogical(BaseOperator, ABC):
    def __init__(self, values: list[BaseOperator]):
        self.values = values
