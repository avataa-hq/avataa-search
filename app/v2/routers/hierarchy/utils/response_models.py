from pydantic import BaseModel, ConfigDict


class HierarchyFilterHierarchyItem(BaseModel):
    total_hits: int = 0
    objects: list[dict] = list()


class HierarchyFilterInventoryItem(BaseModel):
    total_hits: int = 0
    objects: list[dict] = list()


class HierarchyFilterResults(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    hierarchy_results: HierarchyFilterHierarchyItem
    inventory_results: HierarchyFilterInventoryItem
    aggregation_by_ranges: dict = None
