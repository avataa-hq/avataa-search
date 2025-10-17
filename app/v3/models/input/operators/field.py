from typing import TypeAlias, Union

from v3.models.input.operators.input_subclasses import (
    comparison_operators_subclasses,
    element_operators_subclasses,
    evaluation_operators_subclasses,
)

operators = frozenset().union(
    [
        *comparison_operators_subclasses,
        *element_operators_subclasses,
        *evaluation_operators_subclasses,
    ]
)

# Required for typing in Pydantic and FastApi
base_operators_union = Union[*operators]

field: TypeAlias = dict[str, base_operators_union]
