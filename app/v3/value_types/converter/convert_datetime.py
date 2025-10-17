from datetime import datetime

from v3.custom_exceptions.convert_exception import ConvertException
from v3.value_types.converter.base_converter import BaseConverter


class Datetime(BaseConverter):
    def __init__(self, value: datetime):
        if not isinstance(value, datetime):
            raise ConvertException(f"Value {value} is not a datetime")
        self.value = value

    def to_int(self):
        return int(round(self.value.timestamp()))

    def to_str(self):
        return self.value.isoformat()

    def to_bool(self):
        raise ConvertException("Datetime value cannot be converted to boolean.")

    def to_float(self):
        return self.value.timestamp()

    def to_list(self):
        raise ConvertException("Datetime value cannot be converted to list.")

    def to_sequence(self):
        return int(round(self.value.timestamp()))

    def to_datetime(self):
        return self.value

    def to_date(self):
        return self.value.date()

    def to_formula(self):
        raise ConvertException("Datetime value cannot be converted to formula.")
