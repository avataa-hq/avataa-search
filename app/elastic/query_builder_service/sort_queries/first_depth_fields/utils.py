from typing import Callable


def get_sort_query_function_for_first_depth_fields() -> Callable:
    def sort_func(column_name: str, ascending: bool):
        return {column_name: {"order": "asc" if ascending else "desc"}}

    return sort_func
