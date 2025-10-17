from typing import Union

from v3.models.input.operators.field import field
from v3.models.input.operators.logical_subclasses import (
    logical_operators_subclasses,
)

# Required for typing in Pydantic and FastApi
operators = [*logical_operators_subclasses, field]
base_operators_union = Union[*operators]
