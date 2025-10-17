from geojson import (
    Point,
    MultiPoint,
    LineString,
    MultiLineString,
    Polygon,
    MultiPolygon,
)

GEOMETRY_MAPPER = {
    "Point": Point,
    "MultiPoint": MultiPoint,
    "LineString": LineString,
    "MultiLineString": MultiLineString,
    "Polygon": Polygon,
    "MultiPolygon": MultiPolygon,
}


def geometry_validation(item_to_validate: dict) -> bool:
    """Returns True if item_to_validate is valid geojson, False otherwise."""
    try:
        geometry_type = item_to_validate["type"]
        geometry_coordinates = item_to_validate["coordinates"]
        geometry_mapper = GEOMETRY_MAPPER[geometry_type]
        geometry_mapper = geometry_mapper(geometry_coordinates)
    except Exception:
        return False
    return geometry_mapper.is_valid
