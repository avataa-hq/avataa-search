from typing_extensions import Protocol, runtime_checkable


@runtime_checkable
class KafkaMSGProtocol(Protocol):
    def topic(self): ...

    def key(self): ...

    def value(self): ...
