import json
from typing import Annotated

from pydantic import BaseModel, BeforeValidator, Field

from elastic.pydantic_models import FilterColumn
from v2.routers.severity.models import Ranges, Limit, SortColumn


def validate_filter_list(
    v: list[FilterColumn] | list[str] | list[dict] | None,
) -> list[FilterColumn] | None:
    if v is None or not v:
        return None
    values: list[FilterColumn] = []
    for value in v:
        if isinstance(value, str):
            value = json.loads(value)
            value = FilterColumn.model_validate(value)
        elif isinstance(value, dict):
            value = FilterColumn.model_validate(value)
        values.append(value)
    return values


def replace_empty_int(v: int) -> int | None:
    if v == 0:
        v = None
    return v


def validate_ranges(v: Ranges | str) -> Ranges | None:
    if v is None:
        return
    if isinstance(v, str):
        v = json.loads(v)
        v = Ranges.model_validate(v)
    elif isinstance(v, dict):
        v = Ranges.model_validate(v)
    return v


def validate_limit(v: Limit | str) -> Limit:
    if isinstance(v, str):
        v = json.loads(v)
        v = Limit.model_validate(v)
    return v


def validate_sort(v: list[SortColumn] | list[str]) -> list[SortColumn] | None:
    if v is None or not v:
        return None
    values = []
    for value in v:
        if isinstance(value, str):
            value = json.loads(value)
        value = SortColumn.model_validate(value)
        values.append(value)
    return values


class ApiByRangesInput(BaseModel):
    tmo_id: int
    ranges_object: Annotated[Ranges, BeforeValidator(validate_ranges)]
    filters_list: Annotated[
        list[FilterColumn] | None, BeforeValidator(validate_filter_list)
    ] = None
    find_by_value: str | None = None


class ApiGetProcessesInput(BaseModel):
    tmo_id: Annotated[int | None, BeforeValidator(replace_empty_int)] = None
    find_by_value: str | None = None
    filters_list: Annotated[
        list[FilterColumn] | None, BeforeValidator(validate_filter_list)
    ] = None
    with_groups: bool | None = True
    ranges_object: Annotated[
        Ranges | None, BeforeValidator(validate_ranges)
    ] = None
    sort: Annotated[list[SortColumn] | None, BeforeValidator(validate_sort)] = (
        None
    )
    limit: Annotated[Limit, BeforeValidator(validate_limit)] = Field(
        default_factory=lambda: Limit()
    )
