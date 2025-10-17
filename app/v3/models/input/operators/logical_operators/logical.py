from __future__ import annotations

from collections import defaultdict
from typing import Annotated, get_origin

from pydantic import BaseModel, Field, AfterValidator

from v3.models.input.operators.field_operators.base_operators import (
    in_operator_prefix,
    BaseLogical,
)
from v3.models.input.operators.field import field


def check_value(value: list[field | BaseLogical]) -> list[field | BaseLogical]:
    """
    Verification is difficult due to the limitations of elasticSearch
    """
    base_type = get_origin(field)
    is_values = isinstance(value[0], base_type)
    types_count = defaultdict(lambda: 0)
    for v in value:
        v_is_values = isinstance(v, base_type)
        if v_is_values != is_values:
            raise ValueError(
                f"All items in {value} must be fields or operators"
            )
        elif v_is_values and len(v) != 1:
            raise ValueError(
                f"Each filter must be passed separately, not as one object. Unprocessable value {v}"
            )
        if not v_is_values:
            types_count[type(v)] += 1
    for val_type, count in types_count.items():
        if count <= 1:
            continue
        raise ValueError(
            f"All items in {value} must be fields or different operators. Unprocessable type {val_type}"
            f"with count = {count} (must be 1)"
        )
    return value


class And(BaseLogical, BaseModel):
    value: Annotated[list[field | BaseLogical], AfterValidator(check_value)] = (
        Field(
            ...,
            validation_alias=f"{in_operator_prefix}and",
            serialization_alias="and",
            min_length=1,
        )
    )


class Or(BaseLogical, BaseModel):
    value: Annotated[list[field | BaseLogical], AfterValidator(check_value)] = (
        Field(
            ...,
            validation_alias=f"{in_operator_prefix}or",
            serialization_alias="or",
            min_length=1,
        )
    )


class Nor(BaseLogical, BaseModel):
    value: Annotated[list[field | BaseLogical], AfterValidator(check_value)] = (
        Field(
            ...,
            validation_alias=f"{in_operator_prefix}nor",
            serialization_alias="nor",
            min_length=1,
        )
    )
