from typing import List

from pydantic import BaseModel, Field

from services.inventory_services.response_models import MOItemResponseModel


class MOLinkInfo(BaseModel):
    mo_id: int
    mo_name: str
    parent_mo_name: str | None = None
    parent_mo_id: int | None = None
    tmo_id: int
    tmo_name: str
    tprm_id: int
    tprm_name: str
    multiple: bool
    value: list[int]


class TMOInfo(BaseModel):
    tmo_id: int
    tmo_name: str
    tprm_id: list[int]


class MOLinkInfoResponse(BaseModel):
    mo_link_info: list[MOLinkInfo]
    additional_info: list[TMOInfo]
    total: int


class MOLinkInInfoResponse(BaseModel):
    mo_link_info_response: MOLinkInfoResponse
    mo_id: int


class MOLinkInListInfoResponse(BaseModel):
    list_info: list[MOLinkInInfoResponse]


class MOInfo(BaseModel):
    mo_id: int = Field(alias="id")
    mo_name: str | None = Field(None, alias="name")
    p_id: int | None = None
    parent_name: str | None = None
    tmo_id: int

    class Config:
        populate_by_name = True


class MOLinkOutWithTPRMInfo(BaseModel):
    tprm_id: int
    tprm_name: str
    multiple: bool
    mo_out_info: MOInfo


class MOLinkOut(BaseModel):
    mo_id: int
    mo_out_data: list[MOLinkOutWithTPRMInfo]


class MOLinkOutInfoResponse(BaseModel):
    out_mo_link_info: list[MOLinkOut] | None = None


class FuzzySearchRequestModel(BaseModel):
    tmo_id: int | None = None
    limit: int | None = Field(1000, gt=0, le=1000)
    offset: int | None = Field(0, ge=0)
    search_value: str = Field(min_length=2)


class PaginationMetaData(BaseModel):
    step_count: int
    limit: int
    offset: int
    total_hits: int


class MOWithParametersResponseModel(BaseModel):
    metadata: PaginationMetaData
    objects: List[MOItemResponseModel]
