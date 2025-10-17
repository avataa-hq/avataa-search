from pydantic import BaseModel, Field

from v3.models.input.operators.field import (
    base_operators_union as field_base_operators_union,
)
from v3.models.input.operators.input_union import base_operators_union


# class Limit(BaseModel):
#     offset: int = Field(0, ge=0)
#     limit: int = Field(1000, gt=0, le=1000)


class LevelFilter(BaseModel):
    level_id: int
    path: field_base_operators_union | None = Field(default=None)
    id: field_base_operators_union | None = Field(default=None)
    parent_id: field_base_operators_union | None = Field(default=None)
    key: field_base_operators_union | None = Field(default=None)
    mo_filters: base_operators_union | None = Field(default=None)


class HierarchyFilter(BaseModel):
    hierarchy_id: int
    active: bool | None = Field(default=None)
    level_filters: list[LevelFilter] = Field(default_factory=list)


class HierarchyFilterModel(BaseModel):
    filters: HierarchyFilter
    show_data_from_level_id: int
    # limit: Limit = Field(default_factory=Limit)
