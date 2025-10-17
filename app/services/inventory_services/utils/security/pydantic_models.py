from pydantic import BaseModel


class AvailableTMOdata(BaseModel):
    data_available: bool
    tmo_data_as_dict: dict | None = None
    available_tprm_data: dict[int, dict] | None = None
