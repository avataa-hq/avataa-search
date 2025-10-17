from asyncio import Semaphore
from logging import getLogger
from typing import Callable

from confluent_kafka import cimpl

from elastic.client import ElasticsearchManager
from kafka_config.config import KAFKA_GROUP_BUILDER_GROUP_TOPIC  # noqa

from kafka_config.msg_protocol import KafkaMSGProtocol
from services.group_builder.kafka.consumers.group.configs import (
    GROUP_TOPIC_PROTOBUF_DESERIALIZERS,
    GROUP_HANDLERS_BY_MSG_EVENT,
)
from services.group_builder.kafka.consumers.group.protobuf.custom_deserializer import (
    protobuf_kafka_group_group_msg_to_dict,
)


class GroupTopicHandler:
    # update_semaphore = Semaphore(max(2, os.cpu_count() * 2))
    update_semaphore = Semaphore(10)

    def __init__(self, kafka_msg: KafkaMSGProtocol):
        self.msg = kafka_msg
        self.msg_instance_class_name = None
        self.msg_instance_event = None
        self.logger = getLogger("GroupTopicHandler")

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
        deserializer_model = GROUP_TOPIC_PROTOBUF_DESERIALIZERS.get(
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
        including_default_value_fields: bool = False,
    ):
        return protobuf_kafka_group_group_msg_to_dict(
            msg=deserializer_instance,
            including_default_value_fields=including_default_value_fields,
        )

    def __get_event_handler(self) -> Callable:
        event_handler = GROUP_HANDLERS_BY_MSG_EVENT.get(self.msg_instance_event)
        if event_handler:
            return event_handler
        else:
            pass
            # raise NotImplementedError(f"Does not implemented kafka msg handlers "
            #                           f"for the {KAFKA_GROUP_BUILDER_GROUP_TOPIC} topic "
            #                           f"with msg_class_name = '{self.msg_instance_class_name}' and "
            #                           f"msg_event = '{self.msg_instance_event}'")

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
            if handler:
                async with self.update_semaphore:
                    self.logger.debug(
                        f"Semaphore caught. Free: {self.update_semaphore._value}"
                    )
                    await handler(
                        msg=deserialized_msg, async_client=elastic_client
                    )
            # await elastic_client.close()
