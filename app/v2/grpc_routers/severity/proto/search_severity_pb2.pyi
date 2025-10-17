from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class FilterInput(_message.Message):
    __slots__ = ("filters",)
    FILTERS_FIELD_NUMBER: _ClassVar[int]
    filters: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, filters: _Optional[_Iterable[str]] = ...) -> None: ...

class ResponseSeverityItem(_message.Message):
    __slots__ = ("filter_name", "count", "max_severity")
    FILTER_NAME_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    MAX_SEVERITY_FIELD_NUMBER: _ClassVar[int]
    filter_name: str
    count: int
    max_severity: float
    def __init__(self, filter_name: _Optional[str] = ..., count: _Optional[int] = ..., max_severity: _Optional[float] = ...) -> None: ...

class ListResponseSeverity(_message.Message):
    __slots__ = ("items",)
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[ResponseSeverityItem]
    def __init__(self, items: _Optional[_Iterable[_Union[ResponseSeverityItem, _Mapping]]] = ...) -> None: ...

class ByRangesInput(_message.Message):
    __slots__ = ("filters_list", "ranges_object", "tmo_id", "find_by_value")
    FILTERS_LIST_FIELD_NUMBER: _ClassVar[int]
    RANGES_OBJECT_FIELD_NUMBER: _ClassVar[int]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    FIND_BY_VALUE_FIELD_NUMBER: _ClassVar[int]
    filters_list: _containers.RepeatedScalarFieldContainer[str]
    ranges_object: str
    tmo_id: int
    find_by_value: str
    def __init__(self, filters_list: _Optional[_Iterable[str]] = ..., ranges_object: _Optional[str] = ..., tmo_id: _Optional[int] = ..., find_by_value: _Optional[str] = ...) -> None: ...

class ProcessesInput(_message.Message):
    __slots__ = ("filters_list", "ranges_object", "tmo_id", "sort", "limit", "find_by_value", "with_groups")
    FILTERS_LIST_FIELD_NUMBER: _ClassVar[int]
    RANGES_OBJECT_FIELD_NUMBER: _ClassVar[int]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    SORT_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    FIND_BY_VALUE_FIELD_NUMBER: _ClassVar[int]
    WITH_GROUPS_FIELD_NUMBER: _ClassVar[int]
    filters_list: _containers.RepeatedScalarFieldContainer[str]
    ranges_object: str
    tmo_id: int
    sort: _containers.RepeatedScalarFieldContainer[str]
    limit: str
    find_by_value: str
    with_groups: bool
    def __init__(self, filters_list: _Optional[_Iterable[str]] = ..., ranges_object: _Optional[str] = ..., tmo_id: _Optional[int] = ..., sort: _Optional[_Iterable[str]] = ..., limit: _Optional[str] = ..., find_by_value: _Optional[str] = ..., with_groups: bool = ...) -> None: ...

class ProcessesResponse(_message.Message):
    __slots__ = ("rows", "total_count")
    ROWS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedScalarFieldContainer[str]
    total_count: int
    def __init__(self, rows: _Optional[_Iterable[str]] = ..., total_count: _Optional[int] = ...) -> None: ...
