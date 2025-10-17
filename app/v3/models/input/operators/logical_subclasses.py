import os

from v3.models.find_subclass import find_subclasses_in_directory
from v3.models.input.operators.field_operators.base_operators import BaseLogical

input_directory = os.path.join(
    ".", "v3", "models", "input", "operators", "logical_operators"
)

# Dynamic collection of operators at application startup
logical_operators_subclasses = find_subclasses_in_directory(
    directory=input_directory, base_class=BaseLogical
)
