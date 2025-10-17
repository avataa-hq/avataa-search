import json
from typing import List, Union

from elastic.enum_models import SearchOperator, LogicalOperator
from elastic.pydantic_models import SearchModel, SortModel
from elastic.query_builder_service.inventory_index.utils.parameter_search_utils import (
    get_where_condition_function_for_parameter_fields_by_inventory_val_type,
)

from elastic.query_builder_service.inventory_index.utils.search_utils import (
    get_field_name_for_tprm_id,
)

from elastic.query_builder_service.search_operators.first_depth_fields.utils import (
    get_where_condition_function_for_first_depth_fields,
)


class InventoryIndexQueryBuilder:
    must_not_operators = {
        SearchOperator.NOT_EQUALS.value,
        SearchOperator.IS_EMPTY.value,
        SearchOperator.NOT_CONTAINS.value,
        SearchOperator.IS_NOT_ANY_OF.value,
    }

    def __init__(
        self,
        logical_operator: LogicalOperator = "and",
        search_list_order: List[SearchModel] = None,
    ):
        self.logical_operator = logical_operator
        self.search_list_order = search_list_order

    @classmethod
    def get_separated_fields(
        cls, item_list: Union[List[SearchModel], List[SortModel]]
    ):
        """Returns dict with first_depth_fields and parameter fields"""
        first_depth_fields = []
        parameter_fields = []

        for field_filter in item_list:
            if field_filter.column_name.isdigit():
                parameter_fields.append(field_filter)
            else:
                first_depth_fields.append(field_filter)
        return {
            "first_depth_fields": first_depth_fields,
            "parameter_fields": parameter_fields,
        }

    def _create_queries_for_parameter_fields(
        self, parameter_fields: List[SearchModel]
    ):
        """Returns list of queries for MO parameters fields"""

        IGNORE_LIST_IF_OPERATOR_IS_CASES = {
            SearchOperator.IS_NOT_ANY_OF.value,
            SearchOperator.IS_ANY_OF.value,
        }

        result = {"must": list(), "must_not": list()}

        for field in parameter_fields:
            query_former = get_where_condition_function_for_parameter_fields_by_inventory_val_type(
                operator_name=field.operator, val_type=field.column_type
            )
            parameter_field_name = get_field_name_for_tprm_id(field.column_name)

            list_of_values = [field.value]

            if (
                hasattr(field.value, "__iter__")
                and not isinstance(field.value, str)
                and field.operator not in IGNORE_LIST_IF_OPERATOR_IS_CASES
            ):
                list_of_values = field.value

            for field_value in list_of_values:
                query = query_former(
                    column_name=parameter_field_name, value=field_value
                )
                if query:
                    if field.operator in self.must_not_operators and query:
                        if field.operator == "isNotAnyOf":
                            for q in query:
                                if q.get("exists", None):
                                    result["must"].append([q])
                                else:
                                    result["must_not"].append([q])
                        else:
                            result["must_not"].append(query)
                    else:
                        result["must"].append(query)

        return result

    def _create_queries_for_first_depth_fields(
        self, first_depth_fields: List[SearchModel]
    ):
        """Returns list of queries for first_level fields"""

        result = {"must": list(), "must_not": list()}
        for field in first_depth_fields:
            query_former = get_where_condition_function_for_first_depth_fields(
                operator_name=field.operator, val_type=field.column_type
            )

            query = query_former(
                column_name=field.column_name, value=field.value
            )
            if query:
                if field.operator in self.must_not_operators:
                    result["must_not"].append(query)
                else:
                    result["must"].append(query)

        return result

    def create_query_as_dict(self):
        """Returns elasticsearch query as dict"""

        query = {"must": list(), "must_not": list()}

        fields = self.get_separated_fields(self.search_list_order)

        first_depth_queries = self._create_queries_for_first_depth_fields(
            fields["first_depth_fields"]
        )

        parameter_fields_queries = self._create_queries_for_parameter_fields(
            fields["parameter_fields"]
        )

        if self.logical_operator == "and":
            for k, v in first_depth_queries.items():
                for query_list in v:
                    query[k].extend(query_list)

            for k, v in parameter_fields_queries.items():
                for query_list in v:
                    query[k].extend(query_list)

            res = {"bool": {k: v for k, v in query.items() if v}}

        else:
            should_query_list = []

            for k, v in parameter_fields_queries.items():
                if v:
                    query[k].extend(v)

            for k, v in first_depth_queries.items():
                if v:
                    query[k].extend(v)

            for k, list_of_queries in query.items():
                for query in list_of_queries:
                    if query:
                        should_query_list.append({"bool": {k: query}})

            res = {
                "bool": {
                    "must": [
                        {
                            "bool": {
                                "should": should_query_list,
                                "minimum_should_match": 1,
                            }
                        }
                    ]
                }
            }
        return res

    def create_query_as_json(self):
        """Returns elasticsearch query as json"""
        return json.dumps(self.create_query_as_dict())
