from datetime import datetime

from pydantic import BaseModel


class MOItemResponseModel(BaseModel):
    """Mo Item without permissions and fields for fuzzy search"""

    id: int
    p_id: int | None = None
    parent_name: str | None = None
    name: str
    description: str | None = None
    tmo_id: int
    active: bool
    version: int
    point_a_id: int | None = None
    point_b_id: int | None = None
    latitude: float | None = None
    longitude: float | None = None
    status: str | None = None
    geometry: dict | None = None
    model: str | None = None
    pov: dict | None = None
    document_count: int | None = None
    processInstanceId: int | None = None
    processDefinitionKey: str | None = None
    processDefinitionId: int | None = None
    startDate: datetime | None = None
    state: str | None = None
    endDate: datetime | None = None
    duration: int | None = None
    parameters: dict | None = None
    creation_date: datetime | None = None
    modification_date: datetime | None = None
    groupName: str | None = None
    group_type: str | None = None
    groups: str | list | None = None
    point_a_name: str | None = None
    point_b_name: str | None = None
    label: str | None = None
