from typing import Callable

from confluent_kafka import cimpl

from elastic.client import ElasticsearchManager
from kafka_config.config import KAFKA_HIERARCHY_CHANGES_TOPIC
from kafka_config.msg_protocol import KafkaMSGProtocol
from services.hierarchy_services.kafka.consumers.changes_topic.configs import (
    HIERARCHY_CHANGES_PROTOBUF_DESERIALIZERS,
    HIERARCHY_CHANGES_HANDLER_BY_MSG_CLASS_NAME,
)
from services.hierarchy_services.kafka.consumers.changes_topic.protobuf.custom_deserializer import (
    protobuf_kafka_msg_to_dict,
)


class HierarchyChangesTopicHandler:
    def __init__(self, kafka_msg: KafkaMSGProtocol):
        self.msg = kafka_msg
        self.msg_instance_class_name = None
        self.msg_instance_event = None

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

    def __from_bytes_to_python_proto_model_msg(self) -> cimpl.Message:
        deserializer_model = HIERARCHY_CHANGES_PROTOBUF_DESERIALIZERS.get(
            self.msg_instance_class_name
        )
        if deserializer_model:
            deserializer_instance = deserializer_model()
            deserializer_instance.ParseFromString(self.msg.value())
            return deserializer_instance
        else:
            raise NotImplementedError(
                f"Proto model deserializer does not implemented"
                f"for msg_class_name = '{self.msg_instance_class_name}'"
            )

    @staticmethod
    def __deserialize_to_dict(
        deserializer_instance: cimpl.Message,
        including_default_value_fields: bool = True,
    ):
        return protobuf_kafka_msg_to_dict(
            msg=deserializer_instance,
            including_default_value_fields=including_default_value_fields,
        )

    def __get_event_handler(self) -> Callable:
        handlers_by_class_name = (
            HIERARCHY_CHANGES_HANDLER_BY_MSG_CLASS_NAME.get(
                self.msg_instance_class_name
            )
        )
        if not handlers_by_class_name:
            raise NotImplementedError(
                f"Does not implemented kafka msg handlers "
                f"for the {KAFKA_HIERARCHY_CHANGES_TOPIC} topic "
                f"with msg_class_name = '{self.msg_instance_class_name}'"
            )

        event_handler = handlers_by_class_name.get(self.msg_instance_event)
        if event_handler:
            return event_handler
        else:
            raise NotImplementedError(
                f"Does not implemented kafka msg handlers "
                f"for the {KAFKA_HIERARCHY_CHANGES_TOPIC} topic "
                f"with msg_class_name = '{self.msg_instance_class_name}' and "
                f"msg_event = '{self.msg_instance_event}'"
            )

    @staticmethod
    async def __get_elastic_async_client():
        # async_client = anext(get_async_client())
        # return await async_client
        return ElasticsearchManager().get_client()

    async def process_the_message(self):
        self.clear_msg_data()
        if self.msg_instance_class_name:
            deserialized_msg = self.__from_bytes_to_python_proto_model_msg()

            if deserialized_msg:
                deserialized_msg = self.__deserialize_to_dict(
                    deserializer_instance=deserialized_msg
                )
            else:
                return

            elastic_client = await self.__get_elastic_async_client()
            handler = self.__get_event_handler()
            print("deserialized_msg", deserialized_msg)
            await handler(msg=deserialized_msg, elastic_client=elastic_client)
            # await elastic_client.close()
