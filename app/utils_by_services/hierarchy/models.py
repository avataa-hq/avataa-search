from pydantic import BaseModel, Field


class HierarchyModel(BaseModel):
    model_id: str = Field(validation_alias="id", serialization_alias="id")
    hierarchy_id: str
    key: str
    object_id: str | None = None
    level: str
    child_count: int | None = None
    object_type_id: str
    level_id: str
    active: bool
    parent_id: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    path: str | None = None
    key_is_empty: bool | None = None


class InventoryModel(BaseModel):
    id: int
    longitude: float | None
    modification_date: str
    version: int
    status: str | None
    name: str
    document_count: int
    label: str | None
    tmo_id: int
    pov: dict | None
    p_id: int | None
    point_a_id: int | None
    model: str | None
    active: bool
    point_b_id: int | None
    description: str | None | None = None
    latitude: float | None
    creation_date: str
    parent_name: str | None
    point_a_name: str | None = None
    point_b_name: str | None = None
    geometry: dict | None = None
    parameters: dict | None
    fuzzy_search_fields: dict | None = None
