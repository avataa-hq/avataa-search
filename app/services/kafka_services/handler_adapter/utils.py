from typing import Union

from services.kafka_services.handler_adapter.configs import (
    MSG_HANDLERS_BY_MSG_TOPIC,
)
from services.kafka_services.protocols.msg_handler import MSGHandlerProtocol


class MSGHandlerAdapter:
    def __init__(self, msg_topic: str):
        self.msg_topic = msg_topic

    def get_corresponding_handler(self) -> Union[MSGHandlerProtocol, None]:
        """Returns corresponding handler, otherwise returns None and logs data"""

        handler = MSG_HANDLERS_BY_MSG_TOPIC.get(self.msg_topic, None)
        if handler is None:
            print(
                f"Kafka msg handler does not implemented for topic named '{self.msg_topic}'."
            )
            return
        else:
            return handler
