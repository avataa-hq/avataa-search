from enum import Enum, EnumMeta


class MetaEnum(EnumMeta):
    def __contains__(cls, item):
        try:
            cls(item)
        except ValueError:
            return False
        return True


class InventoryMODefaultFields(Enum):
    ID = "id"
    P_ID = "p_id"
    NAME = "name"
    TMO_ID = "tmo_id"
    ACTIVE = "active"
    VERSION = "version"
    POINT_A_ID = "point_a_id"
    POINT_B_ID = "point_b_id"
    LATITUDE = "latitude"
    LONGITUDE = "longitude"
    STATUS = "status"
    GEOMETRY = "geometry"
    MODEL = "model"
    POV = "pov"
    DOCUMENT_COUNT = "document_count"
    CREATION_DATE = "creation_date"
    MODIFICATION_DATE = "modification_date"
    LABEL = "label"


class InventoryMOProcessedFields(Enum):
    PARENT_NAME = "parent_name"
    POINT_A_NAME = "point_a_name"
    POINT_B_NAME = "point_b_name"


class InventoryMOAdditionalFields(Enum):
    GROUPS = "groups"


class InventoryFuzzySearchFields(Enum, metaclass=MetaEnum):
    NAME = "name"
