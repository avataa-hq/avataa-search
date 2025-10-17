from typing import Protocol

from kafka_config.msg_protocol import KafkaMSGProtocol


class MSGHandlerProtocol(Protocol):
    def __init__(self, kafka_msg: KafkaMSGProtocol):
        self.msg = kafka_msg

    def __call__(self, *args, **kwargs): ...

    async def process_the_message(self): ...
