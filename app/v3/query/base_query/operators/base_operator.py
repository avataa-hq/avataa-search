from abc import ABC, abstractmethod

"""
DTO for internal representation of queries. May be different from requests coming in from the outside
"""


class BaseOperator(ABC):
    @abstractmethod
    def create_query(self):
        raise NotImplementedError
