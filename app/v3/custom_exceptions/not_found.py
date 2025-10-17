from v3.custom_exceptions.base_exception import AppException


class NotFound(AppException):
    """
    Base exception class for not found exceptions
    """

    pass


# INVENTORY NOT FOUND


class TprmNotFound(NotFound):
    pass


class MoNotFound(NotFound):
    pass


class PrmNotFound(NotFound):
    pass


class TmoNotFound(NotFound):
    pass


# HIERARCHY NOT FOUND


class HierarchyNotFound(NotFound):
    pass


class LevelNotFound(NotFound):
    pass


class ObjNotFound(NotFound):
    pass


class NodeNotFound(NotFound):
    pass
