from typing import List, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Extra


@runtime_checkable
class EdgeItemProtocol(Protocol):
    id: int
    point_a: int = None
    point_b: int = None


@runtime_checkable
class VertexItemProtocol(Protocol):
    id: int
    latitude: float = None
    longitude: float = None


class EdgeItemImpl(BaseModel):
    class Config:
        extra = Extra.forbid

    id: int
    point_a: int = None
    point_b: int = None


class VertexItemImpl(BaseModel):
    class Config:
        extra = Extra.forbid

    id: int
    latitude: float
    longitude: float


class WayItem(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    start_point: VertexItemProtocol
    end_point: VertexItemProtocol | None = None
    current_vertex_instance: VertexItemProtocol
    visited_vertex_ids: set
    way_of_vertexes: List[VertexItemProtocol]
    way_of_edges: List[EdgeItemProtocol]


class WayItemResponseModel(BaseModel):
    class Config:
        extra = Extra.forbid
        arbitrary_types_allowed = True

    start_point: VertexItemImpl
    end_point: VertexItemImpl | None = None
    way_of_vertexes: List[VertexItemImpl]
    way_of_edges: List[EdgeItemImpl]
