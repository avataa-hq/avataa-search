from abc import abstractmethod, ABC
from typing import get_origin

from pydantic.fields import FieldInfo

from v3.models.input.operators.field_operators.base_operators import (
    BaseOperator as InputBaseOperator,
)
from v3.models.input.operators.field_operators.comparison import Eq
from v3.models.input.operators.field import field
from v3.models.input.operators.logical_operators.logical import (
    BaseLogical as InputBaseLogical,
)
from v3.models.input.operators.field_operators.base_operators import (
    BaseOperator,
)
from v3.query.base_query.operators.comparison.base_comparison import (
    BaseComparison,
)
from v3.query.base_query.operators.element.base_element import BaseElement
from v3.query.base_query.operators.evaluation.base_evaluation import (
    BaseEvaluation,
)
from v3.query.base_query.operators.logical.base_logical import BaseLogical


class Parser(ABC):
    """
    Turns an incoming request into its DTO of a specific implementation
    """

    @abstractmethod
    def convert_operators(
        self, operator: type[InputBaseOperator]
    ) -> type[BaseOperator]:
        raise NotImplementedError

    def _parse_field(self, in_query: field) -> BaseOperator:
        key, value = list(in_query.items())[0]
        if not isinstance(value, BaseOperator):
            field_info: FieldInfo = Eq.model_fields["value"]
            value_key = (
                field_info.validation_alias or field_info.alias or "value"
            )
            value = Eq.model_validate({value_key: value})
        operator_type = type(value)
        operator: type[BaseComparison | BaseElement | BaseEvaluation] = (
            self.convert_operators(operator_type)
        )
        return operator(key=key, value=value.value)

    def _parse_logical(self, in_query: InputBaseLogical):
        operator_type = type(in_query)
        operator: type[BaseLogical] = self.convert_operators(operator_type)
        return operator(values=[self.parse(in_query=i) for i in in_query.value])

    def parse(self, in_query: field | InputBaseLogical):
        base_type = get_origin(field)
        if isinstance(in_query, base_type):
            return self._parse_field(in_query)
        elif isinstance(in_query, InputBaseLogical):
            return self._parse_logical(in_query)
        else:
            raise ValueError(f"Unsupported input type: {type(in_query)}")
