from datetime import datetime
from typing import Literal

from pydantic import BaseModel


class HierarchyDTO(BaseModel):
    id: int
    name: str
    description: str | None = None
    author: str | None = None
    change_author: str | None = None
    created: datetime
    modified: datetime | None = None
    create_empty_nodes: bool = False
    status: str | None = None


LITERAL_HIERARCHY_SORT_PARAMETERS = Literal[tuple(HierarchyDTO.model_fields)]


class NodeDTO(BaseModel):
    id: str
    hierarchy_id: int
    parent_id: str | None = None
    key: str | None = None
    object_id: int | None = None
    level: int
    latitude: float | None = None
    longitude: float | None = None
    child_count: int | None = None
    object_type_id: int
    level_id: int
    active: bool | None = False
    path: str | None = None
    key_is_empty: bool | None = False


class LevelDTO(BaseModel):
    id: int
    parent_id: int | None = None
    hierarchy_id: int
    level: int
    name: str
    description: str | None = None
    object_type_id: int
    is_virtual: bool = False
    latitude_id: int | None = None
    longitude_id: int | None = None
    author: str | None = None
    created: datetime
    change_author: str | None = None
    modified: datetime | None = None
    show_without_children: bool = False
    key_attrs: list
