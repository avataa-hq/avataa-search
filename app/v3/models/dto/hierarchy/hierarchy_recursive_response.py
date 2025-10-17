from pydantic import BaseModel, Field


class HierarchyRecursiveResponse(BaseModel):
    is_target_response: bool
    data: list[dict] = Field(default_factory=list)
    parent_ids: set[str] = Field(default_factory=set)


class HierarchyRecursiveFilterDto(BaseModel):
    mo_ids_by_node_id: dict[str, set[int]]
    hier_response: dict[str, str]
    hier_without_children: list[str]
