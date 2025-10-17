from pydantic import BaseModel, Field

from v3.models.input.operators.field_operators.base_operators import (
    in_operator_prefix,
    BaseElement,
)


class Exists(BaseElement, BaseModel):
    value: bool = Field(
        ...,
        validation_alias=f"{in_operator_prefix}exists",
        serialization_alias="exists",
    )
