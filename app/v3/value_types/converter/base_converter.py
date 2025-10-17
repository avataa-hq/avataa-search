from abc import ABC, abstractmethod


class BaseConverter(ABC):
    """
    Can be used for mapping and type conversion.
    """

    @abstractmethod
    def to_int(self):
        raise NotImplementedError

    @abstractmethod
    def to_str(self):
        raise NotImplementedError

    @abstractmethod
    def to_bool(self):
        raise NotImplementedError

    @abstractmethod
    def to_float(self):
        raise NotImplementedError

    @abstractmethod
    def to_list(self):
        raise NotImplementedError

    @abstractmethod
    def to_sequence(self):
        raise NotImplementedError

    @abstractmethod
    def to_datetime(self):
        raise NotImplementedError

    @abstractmethod
    def to_date(self):
        raise NotImplementedError

    @abstractmethod
    def to_formula(self):
        raise NotImplementedError
