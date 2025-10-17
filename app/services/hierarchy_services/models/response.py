from pydantic import BaseModel
from services.hierarchy_services.models.dto import HierarchyDTO


class CountAndListOfHierarchiesResponse(BaseModel):
    count: int
    items: list[HierarchyDTO]
