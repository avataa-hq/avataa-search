from typing import Literal

from pydantic import BaseModel, Field, field_validator

from elastic.pydantic_models import FilterColumn


class Processes(BaseModel):
    rows: list[dict] = Field(...)
    total_count: int = Field(..., alias="totalCount")


class SortColumn(BaseModel):
    column_name: str = Field(..., alias="columnName")
    ascending: bool = True


class Limit(BaseModel):
    limit: int = Field(15, ge=1)
    offset: int = Field(0, ge=0)


class FilterDataInput(BaseModel):
    filter_name: str = Field(..., alias="filterName")
    column_filters: list[FilterColumn] | None = Field(
        None, alias="columnFilters"
    )
    tmo_id: int = Field(..., alias="tmoId")
    severity_direction: Literal["asc", "desc"] | None = Field(default="asc")

    @field_validator("column_filters")
    def check_column_filters(cls, v):
        if not v:
            return v
        columns = [v_.column_name for v_ in v]
        if len(columns) != len(set(columns)):
            raise ValueError("The column must be unique in the list")
        return v

    class Config:
        populate_by_name = True


class ResponseSeverityItem(BaseModel):
    filter_name: str = Field(...)
    count: int = Field(..., ge=0)
    max_severity: int | float = Field(..., ge=0)


class Ranges(BaseModel):
    ranges: dict[str, frozenset[FilterColumn]]
    severity_direction: Literal["asc", "desc"] | None = Field(default="asc")

    @field_validator("ranges")
    @classmethod
    def check_ranges(cls, v, **kwargs):
        if not v:
            return v
        for values_list in v.values():
            if not v:
                raise ValueError("The filter list cannot be empty")
            column_names = []
            for filter_column in values_list:
                if filter_column.rule != "and":
                    raise ValueError("Rule must be AND")
                column_names.append(filter_column.column_name)
        return v
