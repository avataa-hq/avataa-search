import math
from typing import List

from pydantic import BaseModel
from typing_extensions import Protocol, runtime_checkable


@runtime_checkable
class DotsWithCoordsProtocol(Protocol):
    latitude: float
    longitude: float


class DotWithCoords(BaseModel):
    latitude: float
    longitude: float


def get_line_length_between_two_dots(
    point_a: DotsWithCoordsProtocol, point_b: DotsWithCoordsProtocol
) -> float:
    """Returns length between two dots"""
    return math.sqrt(
        (point_b.latitude - point_a.latitude) ** 2
        + (point_b.longitude - point_a.longitude) ** 2
    )


def get_vector_coords(
    point_a: DotsWithCoordsProtocol, point_b: DotsWithCoordsProtocol
) -> DotWithCoords:
    """Return vector coords"""
    return DotWithCoords(
        latitude=point_b.latitude - point_a.latitude,
        longitude=point_b.longitude - point_a.longitude,
    )


def vector_normalization(
    vector_coords: DotsWithCoordsProtocol,
) -> DotWithCoords:
    """Returns normalized vector"""
    divider = math.sqrt(vector_coords.latitude**2 + vector_coords.longitude**2)
    return DotWithCoords(
        latitude=vector_coords.latitude / divider,
        longitude=vector_coords.longitude / divider,
    )


def equidistant_distribution_of_objects_along_the_line(
    line_start_point: DotsWithCoordsProtocol,
    line_end_point: DotsWithCoordsProtocol,
    list_of_dots: List[DotsWithCoordsProtocol],
) -> List[DotsWithCoordsProtocol]:
    """Changes the coordinates of the points in list_of_dots so that they are evenly spaced on the line between
    line_start_point and line_end_point and returns list_of_dots with new coordinates"""
    vector_coords = get_vector_coords(
        point_a=line_start_point, point_b=line_end_point
    )
    norm_vector = vector_normalization(vector_coords=vector_coords)

    line_length = get_line_length_between_two_dots(
        line_start_point, line_end_point
    )
    step_len = line_length / (len(list_of_dots) + 1)

    divider = math.sqrt(norm_vector.latitude**2 + norm_vector.longitude**2)

    for i, point_item in enumerate(list_of_dots, start=1):
        len_from_start = i * step_len
        latitude = (
            line_start_point.latitude
            + (norm_vector.latitude * len_from_start) / divider
        )
        longitude = (
            line_start_point.longitude
            + (norm_vector.longitude * len_from_start) / divider
        )

        point_item.latitude = latitude
        point_item.longitude = longitude

    return list_of_dots
