from enum import Enum


class SecurityClass(Enum):
    TMO = "TMOPermission"
    MO = "MOPermission"
    TPRM = "TPRMPermission"


class SecurityEvent(Enum):
    CREATED = "security.created"
    UPDATED = "security.updated"
    DELETED = "security.deleted"
