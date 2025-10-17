from pydantic import BaseModel, ConfigDict

from kafka_config.msg_protocol import KafkaMSGProtocol


class ClearedKafkaMSG(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    msg: KafkaMSGProtocol
    msg_class_name: str
    msg_event: str
