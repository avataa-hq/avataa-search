from enum import Enum

"""
Can be used for mapping and type conversion.
"""


class ValueType(Enum):
    INT = "int"
    FLOAT = "float"
    STRING = "str"
    BOOL = "bool"
    LIST = "list"
    DATE = "date"
    DATETIME = "datetime"
    SEQUENCE = "sequence"
    FORMULA = "formula"
