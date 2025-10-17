from logging import getLogger
from typing import Callable

from kafka_config.config import KAFKA_ZEEBE_CHANGES_TOPIC
from kafka_config.msg_protocol import KafkaMSGProtocol
from services.zeebe_services.kafka.consumers.process_changes.configs import (
    PROCESS_CHANGES_PROTOBUF_DESERIALIZERS,
    PROCESS_CHANGES_HANDLER_BY_MSG_CLASS_NAME,
)


class ProcessChangesHandler:
    # updated and deleted not implemented:
    # - process_instance_key can not be updated - one for one mo
    # - delete process handles with MO:deleted (inventory.changes msg handler)

    def __init__(self, kafka_msg: KafkaMSGProtocol, elastic_client):
        self.msg = kafka_msg
        self.msg_instance_class_name = None
        self.msg_instance_event = None
        self.logger = getLogger(__name__)
        self.elastic_client = elastic_client

    def clear_msg_data(self):
        """Clears the message data, if successful, change self.msg_instance_class_name and self.msg_instance_event,
        otherwise self.msg_instance_class_name and self.msg_instance_event will be None"""
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
            self.msg_instance_class_name = msg_class_name
            self.msg_instance_event = msg_event

    def __from_bytes_to_python_proto_model_msg(self) -> KafkaMSGProtocol:
        deserializer_model = PROCESS_CHANGES_PROTOBUF_DESERIALIZERS.get(
            self.msg_instance_class_name
        )
        if deserializer_model:
            deserializer_instance = deserializer_model()
            deserializer_instance.ParseFromString(self.msg.value())
            return deserializer_instance

        raise NotImplementedError(
            f"Proto model deserializer does not implemented"
            f"for msg_class_name = '{self.msg_instance_class_name}'"
        )

    @staticmethod
    def __deserialize_to_dict(
        deserializer_instance: KafkaMSGProtocol,
        including_default_value_fields: bool = False,
    ):
        if including_default_value_fields is False:
            resp = {
                field.name: value
                for field, value in deserializer_instance.ListFields()
            }
        else:
            resp = {
                field: getattr(deserializer_instance, field)
                for field in deserializer_instance.DESCRIPTOR.fields_by_name.keys()
            }
        return resp

    def __get_event_handler(self) -> Callable:
        self.logger.warning("Get event handler")
        handlers_by_class_name = PROCESS_CHANGES_HANDLER_BY_MSG_CLASS_NAME.get(
            self.msg_instance_class_name
        )
        if not handlers_by_class_name:
            raise NotImplementedError(
                f"Does not implemented kafka msg handlers "
                f"for the {KAFKA_ZEEBE_CHANGES_TOPIC} topic "
                f"with msg_class_name = '{self.msg_instance_class_name}'"
            )

        event_handler = handlers_by_class_name.get(self.msg_instance_event)
        if event_handler:
            return event_handler

    async def process_the_message(self):
        self.clear_msg_data()
        if self.msg_instance_class_name:
            deserialized_msg = self.__from_bytes_to_python_proto_model_msg()
            if not deserialized_msg:
                return

            deserialized_msg = self.__deserialize_to_dict(
                deserializer_instance=deserialized_msg
            )

            handler = self.__get_event_handler()

            if handler:
                try:
                    await handler(
                        message_as_dict=deserialized_msg,
                        elastic_client=self.elastic_client,
                    )

                except Exception as e:
                    print("ProcessChangesHandler", type(e), e)
                    raise e
