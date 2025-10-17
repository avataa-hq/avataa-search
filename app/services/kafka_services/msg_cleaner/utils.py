from typing import Union

from kafka_config.msg_protocol import KafkaMSGProtocol
from services.kafka_services.msg_cleaner.models import ClearedKafkaMSG


class ClearedKafkaMSGFactory:
    def __init__(self, msg: KafkaMSGProtocol):
        self.msg = msg

    def get_cleared_msg_data(self) -> Union[ClearedKafkaMSG, None]:
        """Returns instance of ClearedMSG, otherwise returns None"""
        msg_key = getattr(self.msg, "key", None)
        if msg_key is None:
            return

        msg_key = msg_key()
        if msg_key is None:
            return

        # msg_key in this case must be bytes
        msg_key = msg_key.decode("utf-8")
        if msg_key.find(":") == -1:
            return

        msg_class_name, msg_event = msg_key.split(":")
        if msg_class_name and msg_event:
            return ClearedKafkaMSG(
                msg=self.msg, msg_class_name=msg_class_name, msg_event=msg_event
            )
        return None
