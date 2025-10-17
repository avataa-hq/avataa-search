from ast import literal_eval

import dateutil.parser
from dateutil.parser import ParserError

from v3.custom_exceptions.convert_exception import ConvertException
from v3.value_types.converter.base_converter import BaseConverter


class Formula(BaseConverter):
    def __init__(self, value: str):
        if not isinstance(value, str):
            raise ConvertException(f"Value {value} is not a str")
        self.value = value

    def to_int(self):
        try:
            return int(self.value)
        except ValueError:
            raise ConvertException(f'Value "{self.value}" is not an integer')

    def to_str(self):
        return self.value

    def to_bool(self):
        return self.value.lower() in {"false", "0", ""}

    def to_float(self):
        try:
            return float(self.value)
        except ValueError:
            raise ConvertException(f'Value "{self.value}" is not a float')

    def to_list(self):
        try:
            value = literal_eval(self.value)
            if not isinstance(value, list):
                raise ConvertException(f'Value "{self.value}" is not a list')
            return value
        except ValueError:
            raise ConvertException(f'Value "{self.value}" is not a list')

    def to_sequence(self):
        try:
            return int(self.value)
        except ValueError:
            raise ConvertException(f'Value "{self.value}" is not an sequence')

    def to_datetime(self):
        try:
            return dateutil.parser.parse(self.value)
        except ParserError:
            raise ConvertException(
                f'Value "{self.value}" is not a valid datetime'
            )

    def to_date(self):
        try:
            return dateutil.parser.parse(self.value).date()
        except ParserError:
            raise ConvertException(f'Value "{self.value}" is not a valid date')

    def to_formula(self):
        return self.value
