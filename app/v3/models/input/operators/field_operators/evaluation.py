from pydantic import BaseModel, Field

from v3.models.input.operators.field_operators.base_operators import (
    in_operator_prefix,
    BaseEvaluation,
)


class Regex(BaseEvaluation, BaseModel):
    value: str = Field(
        ...,
        validation_alias=f"{in_operator_prefix}regex",
        serialization_alias="regex",
    )
