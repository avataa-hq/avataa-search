from datetime import date, datetime

from v3.custom_exceptions.convert_exception import ConvertException
from v3.value_types.converter.base_converter import BaseConverter


class Date(BaseConverter):
    def __init__(self, value: date):
        if not isinstance(value, date):
            raise ConvertException(f"Value {value} is not a date")
        self.value = value

    def to_int(self):
        return int(
            round(datetime.combine(self.value, datetime.min.time()).timestamp())
        )

    def to_str(self):
        return self.value.isoformat()

    def to_bool(self):
        raise ConvertException("Date value cannot be converted to boolean.")

    def to_float(self):
        return datetime.combine(self.value, datetime.min.time()).timestamp()

    def to_list(self):
        raise ConvertException("Datetime value cannot be converted to list.")

    def to_sequence(self):
        return int(
            round(datetime.combine(self.value, datetime.min.time()).timestamp())
        )

    def to_datetime(self):
        return datetime.combine(self.value, datetime.min.time())

    def to_date(self):
        return self.value

    def to_formula(self):
        raise ConvertException("Datetime value cannot be converted to formula.")
