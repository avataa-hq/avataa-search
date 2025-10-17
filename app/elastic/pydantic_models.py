import datetime
from typing import Any, Annotated, List

from fastapi import HTTPException
from pydantic import (
    BaseModel,
    field_validator,
    model_validator,
    Field,
    BeforeValidator,
    ConfigDict,
)

from elastic.enum_models import (
    SearchOperator,
    LogicalOperator,
    ElasticAggregationType,
)
from elastic.utils import convert_int_to_str
from services.inventory_services.converters.val_type_converter import (
    get_convert_function_by_val_type,
    get_convert_function_by_val_type_for_multiple_values,
)


class FilterItem(BaseModel, frozen=True, use_enum_values=True):
    operator: SearchOperator
    value: Any

    @field_validator("value")
    def check_value(cls, v):
        if isinstance(v, list):
            v = tuple(v)
        return v


class FilterColumn(BaseModel, frozen=True, use_enum_values=True):
    column_name: Annotated[str, BeforeValidator(convert_int_to_str)] = Field(
        ..., alias="columnName"
    )  # only str!
    rule: LogicalOperator
    filters: frozenset[FilterItem]

    @field_validator("filters")
    @classmethod
    def check_filters(cls, v):
        if len(v) == 0:
            raise ValueError("Filters not set")
        filters = []
        for f in v:
            if f.operator != SearchOperator.IN_PERIOD.value:
                filters.append(f)
                continue
            period_in_minutes = int(f.value)
            now = datetime.datetime.utcnow() - datetime.timedelta(
                minutes=period_in_minutes
            )
            new_item = FilterItem(
                operator=SearchOperator.IN_PERIOD.value,
                value=now.strftime("%Y-%m-%dT%H:%M:%S.000+0000"),
            )
            filters.append(new_item)
        return frozenset(filters)


class SortColumn(BaseModel):
    column_name: str = Field(..., alias="columnName")
    ascending: bool = True


class SearchModel(BaseModel):
    column_name: str
    operator: Any
    column_type: Any
    multiple: bool
    value: Any

    @model_validator(mode="after")
    def check_and_convert_value(self):
        operators_with_empty_values = {
            SearchOperator.IS_EMPTY.value,
            SearchOperator.IS_NOT_EMPTY.value,
        }
        if self.operator not in operators_with_empty_values:
            if hasattr(self.value, "__iter__") and not isinstance(
                self.value, str
            ):
                convert_function = (
                    get_convert_function_by_val_type_for_multiple_values(
                        self.column_type
                    )
                )
            else:
                convert_function = get_convert_function_by_val_type(
                    self.column_type
                )
            try:
                value = convert_function(self.value)
            except Exception:
                raise HTTPException(
                    status_code=422,
                    detail=f"Cant convert value - {self.value} as val_type - {self.column_type}"
                    f", multiple - {self.multiple},"
                    f" for column - {self.column_name}",
                )
        else:
            value = None
        self.value = value
        return self


class SortModel(BaseModel):
    column_name: str
    ascending: bool = True
    column_type: Any = "str"


class HierarchyFilter(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    filter_columns: List[FilterColumn] = None
    search_by_value: str = None
    tmo_id: int


class AggregationItems(BaseModel):
    aggr_name: str
    aggr_filters: frozenset[FilterColumn]


class AggregationByRanges(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    aggr_items: List[AggregationItems]
    aggr_by_tprm_id: int | None = None
    aggregation_type: ElasticAggregationType | None = None


class InventoryResAdditionalConditions(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    return_results: bool = True
    sort_by: list[SortColumn] = None
    with_groups: bool = False
    limit: int = Field(2000000, ge=0, le=2000000)
    offset: int = Field(0, ge=0)
    should_filter_conditions: List[List[FilterColumn]] = None
    aggregation_by_ranges: AggregationByRanges = None
