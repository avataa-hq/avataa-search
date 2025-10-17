from enum import Enum


class SearchOperator(Enum):
    CONTAINS = "contains"
    NOT_CONTAINS = "notContains"
    EQUALS = "equals"
    NOT_EQUALS = "notEquals"
    STARTS_WITH = "startsWith"
    ENDS_WITH = "endsWith"
    IS_EMPTY = "isEmpty"
    IS_NOT_EMPTY = "isNotEmpty"
    IS_ANY_OF = "isAnyOf"
    IS_NOT_ANY_OF = "isNotAnyOf"
    MORE = "more"
    MORE_OR_EQ = "moreOrEq"
    LESS = "less"
    LESS_OR_EQ = "lessOrEq"
    IN_PERIOD = "inPeriod"


class LogicalOperator(Enum):
    AND = "and"
    OR = "or"


class ElasticFieldValType(Enum):
    LONG = "long"
    TEXT = "text"
    INTEGER = "integer"
    BOOLEAN = "boolean"
    FLOAT = "float"
    OBJECT = "object"
    FLATTENED = "flattened"
    DATE = "date"
    KEYWORD = "keyword"
    DOUBLE = "double"


class InventoryFieldValType(Enum):
    STR = "str"
    DATE = "date"
    DATETIME = "datetime"
    FLOAT = "float"
    INT = "int"
    BOOL = "bool"
    MO_LINK = "mo_link"
    TWO_WAY_MO_LINK = "two-way link"
    ENUM = "enum"
    PRM_LINK = "prm_link"
    USER_LINK = "user_link"
    FORMULA = "formula"
    SEQUENCE = "sequence"


class SortingDirection(Enum):
    ASC = "asc"
    DESC = "desc"


class ElasticAggregationType(str, Enum):
    AVG = "avg"
    MIN = "min"
    MAX = "max"
