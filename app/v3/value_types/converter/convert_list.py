import json

from v3.custom_exceptions.convert_exception import ConvertException
from v3.value_types.converter.base_converter import BaseConverter


class List(BaseConverter):
    def __init__(self, value: list):
        if not isinstance(value, list):
            raise ConvertException(f"Value {value} is not a list")
        self.value = value

    def to_int(self):
        raise ConvertException("List value cannot be converted to integer.")

    def to_str(self):
        return json.dumps(self.value, default=str)

    def to_bool(self):
        raise ConvertException("List value cannot be converted to boolean.")

    def to_float(self):
        raise ConvertException("List value cannot be converted to float.")

    def to_list(self):
        return self.value

    def to_sequence(self):
        raise ConvertException("List value cannot be converted to sequence.")

    def to_datetime(self):
        raise ConvertException("List value cannot be converted to datetime.")

    def to_date(self):
        raise ConvertException("List value cannot be converted to date.")

    def to_formula(self):
        raise ConvertException("List value cannot be converted to formula.")
