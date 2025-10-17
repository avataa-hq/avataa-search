import copy
from collections import defaultdict
from typing import List
from services.inventory_services.coord_features.common_models import (
    EdgeItemProtocol,
    VertexItemProtocol,
    WayItem,
)


class WayFinderByBFS:
    def __init__(
        self,
        start_points: List[VertexItemProtocol],
        end_points: List[VertexItemProtocol],
        edges_lists: List[EdgeItemProtocol],
        vertex_list: List[VertexItemProtocol],
    ):
        self.start_points = start_points
        self.end_points = end_points
        self.edges_lists = edges_lists
        self.vertex_list = vertex_list

        # cache
        self.__points_connected_to_end_point = None  # set()
        self.__dict_of_adj_lists = None  # defaultdict(set)
        self.__end_points_by_id = None
        self.__vertex_by_id = None
        self.__edges_by_point_a_and_point_b = None  # dict()

    @staticmethod
    def __get_edge_key_by_point_a_and_point_b(
        point_a: int, point_b: int
    ) -> frozenset:
        return frozenset([point_a, point_b])

    def __group_edges_by_point_a_and_point_b(self, edge: EdgeItemProtocol):
        if edge.point_a and edge.point_b:
            key = self.__get_edge_key_by_point_a_and_point_b(
                edge.point_a, edge.point_b
            )
            self.__edges_by_point_a_and_point_b[key] = edge

    def __group_vertex_by_ids(self):
        self.__vertex_by_id = {v.id: v for v in self.vertex_list}

    def __group_end_points_by_ids(self):
        self.__end_points_by_id = {
            end_point.id: end_point for end_point in self.end_points
        }

    def __check_if_there_is_point_connected_to_end_point(self) -> bool:
        if self.__end_points_by_id.keys() & self.__dict_of_adj_lists.keys():
            return True
        return False

    def __add_edge_to_adj_lists(self, edge: EdgeItemProtocol):
        if edge.point_a and edge.point_b:
            self.__dict_of_adj_lists[edge.point_a].add(edge.point_b)
            self.__dict_of_adj_lists[edge.point_b].add(edge.point_a)

    def __create_cache_data(self):
        self.__points_connected_to_end_point = set()
        self.__dict_of_adj_lists = defaultdict(set)
        self.__edges_by_point_a_and_point_b = dict()

        self.__group_vertex_by_ids()
        self.__group_end_points_by_ids()
        for edge in self.edges_lists:
            self.__add_edge_to_adj_lists(edge)
            self.__group_edges_by_point_a_and_point_b(edge)

    def find_ways(self) -> List[WayItem]:
        """Returns ordered ways of points from  point_id_from to point_id_to"""

        self.__create_cache_data()

        all_successful_ways = list()

        if self.__check_if_there_is_point_connected_to_end_point is False:
            return all_successful_ways

        all_ways = []

        for start_p in self.start_points:
            neighbour_ids = self.__dict_of_adj_lists.get(start_p.id, [])
            for neighbour_id in neighbour_ids:
                current_vertex_instance = self.__vertex_by_id.get(neighbour_id)
                if not current_vertex_instance:
                    continue
                edge_key = self.__get_edge_key_by_point_a_and_point_b(
                    start_p.id, neighbour_id
                )
                connected_edge = self.__edges_by_point_a_and_point_b.get(
                    edge_key
                )
                way_item = WayItem(
                    start_point=start_p,
                    current_vertex_instance=current_vertex_instance,
                    visited_vertex_ids=set(),
                    way_of_vertexes=[current_vertex_instance],
                    way_of_edges=[connected_edge],
                )
                way_item.visited_vertex_ids.update(
                    [start_p.id, current_vertex_instance.id]
                )
                all_ways.append(way_item)

        if not all_ways:
            return all_successful_ways

        while len(all_ways) > 0:
            way_item = all_ways.pop()
            neighbour_ids = self.__dict_of_adj_lists.get(
                way_item.current_vertex_instance.id
            )
            if not neighbour_ids:
                continue

            intersection_with_end_points = (
                neighbour_ids & self.__end_points_by_id.keys()
            )

            if intersection_with_end_points:
                way_item.visited_vertex_ids.update(intersection_with_end_points)
                end_point_id = intersection_with_end_points.pop()
                way_item.end_point = self.__end_points_by_id.get(end_point_id)
                edge_key = self.__get_edge_key_by_point_a_and_point_b(
                    way_item.current_vertex_instance.id, end_point_id
                )
                connected_edge = self.__edges_by_point_a_and_point_b.get(
                    edge_key
                )
                way_item.way_of_edges.append(connected_edge)
                all_successful_ways.append(way_item)
            visited_and_end_points = set().union(
                self.__end_points_by_id.keys(), way_item.visited_vertex_ids
            )
            difference = neighbour_ids.difference(visited_and_end_points)

            if difference:
                for neighbour_id in difference:
                    vertex = self.__vertex_by_id.get(neighbour_id)
                    if not vertex:
                        continue

                    edge_key = self.__get_edge_key_by_point_a_and_point_b(
                        way_item.current_vertex_instance.id, neighbour_id
                    )
                    connected_edge = self.__edges_by_point_a_and_point_b.get(
                        edge_key
                    )

                    new_way = WayItem(
                        start_point=way_item.start_point,
                        current_vertex_instance=vertex,
                        visited_vertex_ids=copy.deepcopy(
                            way_item.visited_vertex_ids
                        ),
                        way_of_vertexes=copy.deepcopy(way_item.way_of_vertexes),
                        way_of_edges=copy.deepcopy(way_item.way_of_edges),
                    )
                    new_way.visited_vertex_ids.add(vertex.id)
                    new_way.way_of_vertexes.append(vertex)
                    new_way.way_of_edges.append(connected_edge)
                    all_ways.append(new_way)

        return all_successful_ways
