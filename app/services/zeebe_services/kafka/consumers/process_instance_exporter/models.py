from pydantic import BaseModel


class ClearedProcessInstanceMSGValue(BaseModel):
    process_definition_key: str
    process_instance_id: int
    process_definition_id: int
    timestamp: int
