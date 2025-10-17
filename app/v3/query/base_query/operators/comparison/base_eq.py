from abc import ABC

from v3.query.base_query.operators.comparison.base_comparison import (
    BaseComparison,
)


class BaseEq(BaseComparison, ABC):
    pass
