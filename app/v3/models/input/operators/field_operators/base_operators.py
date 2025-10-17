from abc import ABC

from pydantic import BaseModel

"""
Basic classes of operators for incoming data. Can be used to check type inheritance
The syntax is similar to MongoDb
"""

in_operator_prefix = "@"


class BaseOperator(BaseModel, ABC):
    model_config = {
        "populate_by_name": True,
    }


class BaseComparison(BaseOperator, ABC):
    pass


class BaseElement(BaseOperator, ABC):
    pass


class BaseEvaluation(BaseOperator, ABC):
    pass


class BaseLogical(BaseOperator, ABC):
    pass
