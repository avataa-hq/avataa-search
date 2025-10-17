from typing import List

from services.inventory_services.coord_features.common_models import (
    EdgeItemProtocol,
    VertexItemProtocol,
    WayItem,
)
from services.inventory_services.coord_features.common_utils import (
    equidistant_distribution_of_objects_along_the_line,
)
from services.inventory_services.coord_features.way_finder.impl import (
    WayFinderByBFS,
)


def reform_all_connected_points_between_start_point_and_end_point_into_line(
    start_points: List[VertexItemProtocol],
    end_points: List[VertexItemProtocol],
    list_of_lines: List[EdgeItemProtocol],
    list_of_points: List[VertexItemProtocol],
) -> List[WayItem]:
    ways_finder = WayFinderByBFS(
        start_points,
        end_points=end_points,
        edges_lists=list_of_lines,
        vertex_list=list_of_points,
    )
    ways = ways_finder.find_ways()

    for way_item in ways:
        way_item.way_of_vertexes = (
            equidistant_distribution_of_objects_along_the_line(
                line_start_point=way_item.start_point,
                line_end_point=way_item.end_point,
                list_of_dots=way_item.way_of_vertexes,
            )
        )

    return ways
