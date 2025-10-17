from typing import Any

from pydantic import BaseModel, Field

from v3.models.input.operators.field_operators.base_operators import (
    in_operator_prefix,
    BaseComparison,
)


class Eq(BaseComparison, BaseModel):
    value: Any = Field(
        ...,
        validation_alias=f"{in_operator_prefix}eq",
        serialization_alias="eq",
    )


class Gt(BaseComparison, BaseModel):
    value: Any = Field(
        ...,
        validation_alias=f"{in_operator_prefix}gt",
        serialization_alias="gt",
    )


class Gte(BaseComparison, BaseModel):
    value: Any = Field(
        ...,
        validation_alias=f"{in_operator_prefix}gte",
        serialization_alias="gt",
    )


class Lt(BaseComparison, BaseModel):
    value: Any = Field(
        ...,
        validation_alias=f"{in_operator_prefix}lt",
        serialization_alias="lt",
    )


class Lte(BaseComparison, BaseModel):
    value: Any = Field(
        ...,
        validation_alias=f"{in_operator_prefix}lte",
        serialization_alias="lt",
    )


class In(BaseComparison, BaseModel):
    value: list = Field(
        ...,
        validation_alias=f"{in_operator_prefix}in",
        serialization_alias="in",
    )


class Ne(BaseComparison, BaseModel):
    value: Any = Field(
        ...,
        validation_alias=f"{in_operator_prefix}ne",
        serialization_alias="ne",
    )


class Nin(BaseComparison, BaseModel):
    value: Any = Field(
        ...,
        validation_alias=f"{in_operator_prefix}nin",
        serialization_alias="nin",
    )
