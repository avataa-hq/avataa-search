from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class Process(_message.Message):
    __slots__ = ("mo_id", "process_instance_key", "tmo_id")
    MO_ID_FIELD_NUMBER: _ClassVar[int]
    PROCESS_INSTANCE_KEY_FIELD_NUMBER: _ClassVar[int]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    mo_id: int
    process_instance_key: int
    tmo_id: int
    def __init__(self, mo_id: _Optional[int] = ..., process_instance_key: _Optional[int] = ..., tmo_id: _Optional[int] = ...) -> None: ...
