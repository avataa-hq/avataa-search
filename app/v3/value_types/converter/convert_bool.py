from v3.custom_exceptions.convert_exception import ConvertException
from v3.value_types.converter.base_converter import BaseConverter


class Bool(BaseConverter):
    def __init__(self, value: bool):
        if not isinstance(value, bool):
            raise ConvertException(f"Value {value} is not a bool")
        self.value = value

    def to_int(self):
        return int(self.value)

    def to_str(self):
        return str(self.value)

    def to_bool(self):
        return self.value

    def to_float(self):
        return float(self.value)

    def to_list(self):
        raise ConvertException("Boolean value cannot be converted to list.")

    def to_sequence(self):
        raise ConvertException("Boolean value cannot be converted to sequence.")

    def to_datetime(self):
        raise ConvertException("Boolean value cannot be converted to datetime.")

    def to_date(self):
        raise ConvertException("Boolean value cannot be converted to date.")

    def to_formula(self):
        raise ConvertException("Boolean value cannot be converted to formula.")
