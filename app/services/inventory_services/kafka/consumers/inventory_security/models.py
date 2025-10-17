from enum import Enum


class InventorySecurityEventStatus(Enum):
    CREATED = "security.created"
    UPDATED = "security.updated"
    DELETED = "security.deleted"


class InventorySecurityPermissionsScope(Enum):
    MO = "MOPermission"
    TMO = "TMOPermission"
    TPRM = "TPRMPermission"
    PRM = "PRMPermission"
