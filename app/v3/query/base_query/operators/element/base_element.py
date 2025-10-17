from abc import ABC

from v3.query.base_query.operators.base_operator import BaseOperator


class BaseElement(BaseOperator, ABC):
    def __init__(self, key: str, value: bool):
        self.key = key
        self.value = value
