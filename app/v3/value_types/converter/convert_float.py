from datetime import datetime

from v3.custom_exceptions.convert_exception import ConvertException
from v3.value_types.converter.base_converter import BaseConverter


class Float(BaseConverter):
    def __init__(self, value: float):
        if not isinstance(value, float):
            raise ConvertException(f"Value {value} is not a float")
        self.value = value

    def to_int(self):
        return int(self.value)

    def to_str(self):
        return str(self.value)

    def to_bool(self):
        return bool(self.value)

    def to_float(self):
        return self.value

    def to_list(self):
        raise ConvertException("Float value cannot be converted to list.")

    def to_sequence(self):
        return int(self.value)

    def to_datetime(self):
        try:
            return datetime.fromtimestamp(self.value)
        except OSError:
            raise ConvertException(
                "Float value cannot be converted to a datetime."
            )

    def to_date(self):
        try:
            return datetime.fromtimestamp(self.value).date()
        except OSError:
            raise ConvertException("Float value cannot be converted to a date.")

    def to_formula(self):
        raise ConvertException("Integer value cannot be converted to formula.")
