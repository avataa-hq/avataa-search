from __future__ import annotations


from pydantic import BaseModel, Field

from services.hierarchy_services.models.dto import LevelDTO


class TprmDto(BaseModel):
    # required: bool
    id: int
    # version: int
    returnable: bool
    # created_by: str
    # modified_by: str
    name: str
    tmo_id: int
    # constraint: str | None = None
    # creation_date: str
    # modification_date: str
    # description: str | None = None
    # prm_link_filter: str | None = None
    val_type: str
    # group: str | None = None
    multiple: bool
    # field_value: str | None = None


class LevelHierarchyDto(BaseModel):
    current_level: LevelDTO

    children: list[LevelHierarchyDto] = Field(default_factory=list)


class LevelHierarchyWithTmoTprmsDto(BaseModel):
    current_level: LevelDTO
    current_level_tprms_by_tprm_id: dict[int, TprmDto]
    parent: LevelHierarchyWithTmoTprmsDto | None = None
    children: list[LevelHierarchyWithTmoTprmsDto] = Field(default_factory=list)


class FilteredLevelsDto(BaseModel):
    level: LevelHierarchyWithTmoTprmsDto
    left_tprm_ids: set[int]
