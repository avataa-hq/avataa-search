from v3.input_parser.base_parser import Parser
from v3.models.input.operators.field_operators.base_operators import (
    BaseOperator as InputBaseOperator,
)
from v3.models.input.operators.field_operators.comparison import (
    Eq as InEq,
    In as InIn,
    Gt as InGt,
    Gte as InGte,
    Lt as InLt,
    Lte as InLte,
    Ne as InNe,
    Nin as InNin,
)
from v3.models.input.operators.logical_operators.logical import (
    And as InAnd,
    Or as InOr,
    Nor as InNor,
)
from v3.models.input.operators.field_operators.element import Exists as InExists
from v3.query.base_query.operators.base_operator import BaseOperator
from v3.query.implementations.elasticsearch_query.operators.comparison.es_eq import (
    Eq as EsEq,
)
from v3.query.implementations.elasticsearch_query.operators.comparison.es_in import (
    In as EsIn,
)
from v3.query.implementations.elasticsearch_query.operators.comparison.es_gt import (
    Gt as EsGt,
)
from v3.query.implementations.elasticsearch_query.operators.comparison.es_gte import (
    Gte as EsGte,
)
from v3.query.implementations.elasticsearch_query.operators.comparison.es_lt import (
    Lt as EsLt,
)
from v3.query.implementations.elasticsearch_query.operators.comparison.es_lte import (
    Lte as EsLte,
)
from v3.query.implementations.elasticsearch_query.operators.comparison.es_ne import (
    Ne as EsNe,
)
from v3.query.implementations.elasticsearch_query.operators.comparison.es_nin import (
    Nin as EsNin,
)
from v3.query.implementations.elasticsearch_query.operators.logical.es_and import (
    And as EsAnd,
)
from v3.query.implementations.elasticsearch_query.operators.logical.es_or import (
    Or as EsOr,
)
from v3.query.implementations.elasticsearch_query.operators.logical.es_nor import (
    Nor as EsNor,
)
from v3.query.implementations.elasticsearch_query.operators.element.es_exists import (
    Exists as EsExists,
)


class ElasticsearchQueryParser(Parser):
    operators: dict[type[InputBaseOperator], type[BaseOperator]] = {
        InEq: EsEq,
        InIn: EsIn,
        InGt: EsGt,
        InGte: EsGte,
        InLt: EsLt,
        InLte: EsLte,
        InNe: EsNe,
        InNin: EsNin,
        InAnd: EsAnd,
        InOr: EsOr,
        InNor: EsNor,
        InExists: EsExists,
    }

    def convert_operators(
        self, operator: type[InputBaseOperator]
    ) -> type[BaseOperator]:
        result_operator = self.operators.get(operator)
        if result_operator is None:
            raise TypeError(f"Operator {operator} is not supported")
        return result_operator
