from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class LevelDto(BaseModel):
    id: int
    hierarchy_id: int
    level: int
    name: str
    object_type_id: int
    param_type_id: int
    additional_params_id: int | None = Field(default=None)
    latitude_id: int | None = Field(default=None)
    longitude_id: int | None = Field(default=None)
    author: str
    created: datetime
    show_without_children: bool
    key_attrs: list[int] = Field(default_factory=list)
    parent_id: int | None
    description: str | None
    is_virtual: bool
    change_author: str
    attr_as_parent: bool = Field(default=False)


class LevelWay(BaseModel):
    level_data: LevelDto
    children: list[LevelWay] = Field(default_factory=list)
    is_target: bool = Field(default=False)
