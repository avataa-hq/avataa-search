from abc import ABC
from typing import Any

from v3.query.base_query.operators.base_operator import BaseOperator


class BaseEvaluation(BaseOperator, ABC):
    def __init__(self, key: str, value: Any):
        self.key = key
        self.value = value
