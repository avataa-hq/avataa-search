from dataclasses import dataclass
from typing import Any


@dataclass
class PRMLinkChangeData:
    """Class for keeping track of an item in inventory."""

    tprm_id: str
    multiple: bool
    index_of_prm_in_array: int | None = None


@dataclass
class MOLinkChangeData:
    """Class for keeping track of an item in inventory."""

    prm_id: str
    tprm_id: str
    multiple: bool
    index_of_prm_in_array: int | None = None


@dataclass
class PRMChangeData:
    """Class for keeping track of an item in inventory."""

    prm_id: str
    value: Any
