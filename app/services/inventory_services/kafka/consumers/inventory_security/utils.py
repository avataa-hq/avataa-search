from typing import List, Callable, Union

from elastic.client import ElasticsearchManager
from kafka_config.config import KAFKA_INVENTORY_SECURITY_TOPIC
from kafka_config.msg_protocol import KafkaMSGProtocol
from services.inventory_services.kafka.consumers.inventory_security.configs import (
    INVENTORY_SECURITY_HANDLER_BY_MSG_PERMISSION_SCOPE,
)

from services.inventory_services.protobuf_files.security.custom_deserializer import (
    protobuf_kafka_list_permission_msg_to_list_of_dicts,
)
from services.inventory_services.protobuf_files.security.transfer_pb2 import (
    ListPermission,
)


class InventorySecurityHandler:
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

    def __from_bytes_to_python_proto_model_msg(
        self,
    ) -> Union[ListPermission, None]:
        deserializer_instance = ListPermission()
        try:
            deserializer_instance.ParseFromString(self.msg.value())
        except Exception:
            return None
        return deserializer_instance

    @staticmethod
    def __deserialize_to_list(
        deserializer_instance: ListPermission,
        including_default_value_fields: bool = True,
    ) -> List[dict]:
        return protobuf_kafka_list_permission_msg_to_list_of_dicts(
            msg=deserializer_instance,
            including_default_value_fields=including_default_value_fields,
        )

    def __get_event_handler(self) -> Callable:
        handlers_by_class_name = (
            INVENTORY_SECURITY_HANDLER_BY_MSG_PERMISSION_SCOPE.get(
                self.msg_instance_class_name
            )
        )
        if not handlers_by_class_name:
            raise NotImplementedError(
                f"Does not implemented kafka msg handlers "
                f"for the {KAFKA_INVENTORY_SECURITY_TOPIC} topic "
                f"with msg_class_name = '{self.msg_instance_class_name}'"
            )

        event_handler = handlers_by_class_name.get(self.msg_instance_event)
        if event_handler:
            return event_handler
        else:
            raise NotImplementedError(
                f"Does not implemented kafka msg handlers "
                f"for the {KAFKA_INVENTORY_SECURITY_TOPIC} topic "
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
                deserialized_msg = self.__deserialize_to_list(
                    deserializer_instance=deserialized_msg
                )
            else:
                return

            elastic_client = await self.__get_elastic_async_client()
            handler = self.__get_event_handler()
            await handler(msg=deserialized_msg, async_client=elastic_client)
            # await elastic_client.close()
