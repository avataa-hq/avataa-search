import os

from v3.models.find_subclass import find_subclasses_in_directory
from v3.models.input.operators.field_operators.base_operators import (
    BaseOperator,
    BaseComparison,
    BaseElement,
    BaseEvaluation,
)

input_directory = os.path.join(
    ".", "v3", "models", "input", "operators", "field_operators"
)

# Dynamic collection of operators at application startup
base_operators_subclasses = find_subclasses_in_directory(
    directory=input_directory, base_class=BaseOperator
)
comparison_operators_subclasses = find_subclasses_in_directory(
    directory=input_directory, base_class=BaseComparison
)
element_operators_subclasses = find_subclasses_in_directory(
    directory=input_directory, base_class=BaseElement
)
evaluation_operators_subclasses = find_subclasses_in_directory(
    directory=input_directory, base_class=BaseEvaluation
)
