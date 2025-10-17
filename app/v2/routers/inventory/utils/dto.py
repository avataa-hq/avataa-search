from dataclasses import dataclass


@dataclass
class DTOMOInfo:
    id: int
    name: str
    parent_name: str
    p_id: int
    tmo_id: int


@dataclass
class DTOTPRMInfo:
    id: int
    multiple: bool
    name: str
    tmo_id: int


@dataclass
class DTOMOLinkInfo:
    mo_id: int
    tprm_id: int
    # value: int | list[int]
    value: list[int]
