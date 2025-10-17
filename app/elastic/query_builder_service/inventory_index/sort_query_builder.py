import json
from typing import List

from elastic.pydantic_models import SortModel
from elastic.query_builder_service.inventory_index.utils.search_utils import (
    get_field_name_for_tprm_id,
)

from elastic.query_builder_service.sort_queries.first_depth_fields.utils import (
    get_sort_query_function_for_first_depth_fields,
)


class InventorySortQueryBuilder:
    def __init__(self, sort_order_list: List[SortModel]):
        self.sort_order_list = sort_order_list

    def create_sort_list_as_list_of_dicts(self):
        result = []

        for sort_column in self.sort_order_list:
            if sort_column.column_name.isdigit():
                sort_query_builder_func = (
                    get_sort_query_function_for_first_depth_fields()
                )
                column_name = get_field_name_for_tprm_id(
                    sort_column.column_name
                )
                sort_item = sort_query_builder_func(
                    column_name=column_name, ascending=sort_column.ascending
                )
            else:
                sort_query_builder_func = (
                    get_sort_query_function_for_first_depth_fields()
                )
                sort_item = sort_query_builder_func(
                    column_name=sort_column.column_name,
                    ascending=sort_column.ascending,
                )

            result.append(sort_item)
        return result

    def create_sort_list_as_json(self):
        return json.dumps(self.create_sort_list_as_list_of_dicts())
