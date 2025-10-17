from dataclasses import dataclass

from pydantic import BaseModel, field_validator, Field

from elastic.enum_models import ElasticAggregationType


class AllResAsListQueryModel(BaseModel):
    items_per_step: int = Field(10_000, gt=0)
    query_condition: dict
    sort_cond: dict
    source_conditions: dict | None = None
    offset: int | None = Field(0, gte=0)
    limit: int | None = Field(None, gt=0, le=2000000)

    def get_search_body(self):
        body = dict()
        body.update(self.query_condition)

        if self.sort_cond:
            body.update(self.sort_cond)

        if self.source_conditions:
            body.update(self.source_conditions)

        body["track_total_hits"] = True

        if self.offset:
            body["from"] = self.offset

        if self.limit:
            body["size"] = self.limit
        else:
            body["size"] = self.items_per_step
        return body

    @field_validator("query_condition")
    @classmethod
    def check_query(cls, v: dict) -> dict:
        if "query" in v:
            if v["query"]:
                return v
            else:
                raise ValueError(
                    'The query_condition attribute must be as dict with "query" key.'
                    'Value of the "query" key can`t be empty'
                )
        raise ValueError(
            'The query_condition attribute must be as dict with "query" key'
        )

    @field_validator("sort_cond")
    @classmethod
    def check_sort_cond(cls, v: dict) -> dict:
        if "sort" in v:
            if v["sort"]:
                return v
            else:
                raise ValueError(
                    'The sort_cond attribute must be as dict with "sort" key.'
                    'Value of the "sort" key can`t be empty'
                )
        raise ValueError(
            'The sort_cond attribute must be as dict with "sort" key'
        )

    @field_validator("source_conditions")
    @classmethod
    def check_source_cond(cls, v: dict) -> dict:
        if not v:
            return v
        if "_source" in v:
            if v["_source"]:
                return v
            else:
                raise ValueError(
                    'The source_conditions attribute must be as dict with "_source" key.'
                    'Value of the "_source" key can`t be empty'
                )


class AllResAsDictQueryModel(AllResAsListQueryModel):
    key_attr_for_dict: str | None = "id"


@dataclass
class CountAndItemsAsDict:
    count: int
    items: dict


@dataclass
class CountAndItemsAsList:
    count: int
    items: list


class HierarchyAggregateByTPRM(BaseModel):
    tprm_id: int
    aggregation_type: ElasticAggregationType

    class Config:
        use_enum_values = True


class HierarchyAggregateByTMO(BaseModel):
    tprm_id: int
    tmo_id: int
    aggregation_type: ElasticAggregationType

    class Config:
        use_enum_values = True
