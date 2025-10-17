import json
from typing import Callable

from elastic.client import ElasticsearchManager
from kafka_config.msg_protocol import KafkaMSGProtocol
from services.hierarchy_services.kafka.consumers.level_topic.configs import (
    EVENT_HANDLERS_BY_LEVEL_EVENT,
)


class HierarchyLevelsChangesTopicHandler:
    def __init__(self, kafka_msg: KafkaMSGProtocol):
        self.msg = kafka_msg
        self.msg_instance_class_name = "Level"
        self.msg_instance_event = None
        self.msg_payload_before = None
        self.msg_payload_after = None

    def clear_msg_data(self):
        """Clears the message data, if successful, change self.msg_instance_class_name and self.msg_instance_event,
        otherwise self.msg_instance_class_name and self.msg_instance_event will be None"""
        msg_value = getattr(self.msg, "value", None)
        if msg_value is None:
            return

        msg_value = msg_value()
        if not msg_value:
            return

        try:
            msg_value = json.loads(msg_value.decode("utf-8"))
        except Exception as e:
            print(e)
            return

        msg_payload = msg_value.get("payload")
        if not msg_payload:
            return

        self.msg_instance_event = msg_payload.get("op")
        self.msg_payload_before = msg_payload.get("before")
        self.msg_payload_after = msg_payload.get("after")

    def __get_event_handler(self) -> Callable:
        handlers_event_name = EVENT_HANDLERS_BY_LEVEL_EVENT.get(
            self.msg_instance_event
        )
        # does not raise an error if there is no specific handler, since there are can be msgs
        # with events that do not need to be handled
        return handlers_event_name

    @staticmethod
    async def __get_elastic_async_client():
        # async_client = anext(get_async_client())
        # return await async_client
        return ElasticsearchManager().get_client()

    async def process_the_message(self):
        self.clear_msg_data()

        if self.msg_instance_event:
            handler = self.__get_event_handler()
            if handler:
                elastic_client = await self.__get_elastic_async_client()
                try:
                    await handler(
                        payload_before=self.msg_payload_before,
                        payload_after=self.msg_payload_after,
                        elastic_client=elastic_client,
                    )
                except Exception as ex:
                    print(ex)
                # finally:
                #     await elastic_client.close()
