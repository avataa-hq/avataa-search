import json
from logging import getLogger
from typing import Callable

from elastic.client import ElasticsearchManager
from kafka_config.msg_protocol import KafkaMSGProtocol
from services.zeebe_services.kafka.consumers.process_instance_exporter.configs import (
    EVENT_HANDLERS_BY_BPMN_ELEMENT_TYPE,
    PROCESS_HANDLER_BY_EVENT,
)
from services.zeebe_services.kafka.consumers.process_instance_exporter.models import (
    ClearedProcessInstanceMSGValue,
)

AVAILABLE_HANDLERS = EVENT_HANDLERS_BY_BPMN_ELEMENT_TYPE.keys()
AVAILABLE_EVENTS = PROCESS_HANDLER_BY_EVENT.keys()


class ProcessInstanceChangesHandler:
    def __init__(self, kafka_msg: KafkaMSGProtocol):
        self.msg = kafka_msg
        self.msg_instance_class_name = None
        self.msg_instance_event = None
        self.msg_cleared_info = None
        self.logger = getLogger(__name__)
        self.elastic_client = ElasticsearchManager().get_client()

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

        msg_value_data = msg_value.get("value")
        if not msg_value_data:
            return

        msg_class_name = msg_value_data.get("bpmnElementType")
        msg_event = msg_value.get("intent")
        if (
            msg_class_name in AVAILABLE_HANDLERS
            and msg_event in AVAILABLE_EVENTS
        ):
            self.msg_instance_class_name = msg_class_name
            self.msg_instance_event = msg_event
            cleared_msg_value = ClearedProcessInstanceMSGValue(
                process_instance_id=msg_value_data.get("processInstanceKey"),
                process_definition_key=msg_value_data.get("bpmnProcessId"),
                process_definition_id=msg_value_data.get(
                    "processDefinitionKey"
                ),
                timestamp=msg_value.get("timestamp"),
            )
            self.msg_cleared_info = cleared_msg_value

    def __get_event_handler(self) -> Callable:
        handlers_by_bpmn_element_type = EVENT_HANDLERS_BY_BPMN_ELEMENT_TYPE.get(
            self.msg_instance_class_name
        )
        if handlers_by_bpmn_element_type:
            handler_be_event = handlers_by_bpmn_element_type.get(
                self.msg_instance_event
            )
            if handler_be_event:
                return handler_be_event

    async def process_the_message(self):
        self.clear_msg_data()
        if self.msg_instance_event and self.msg_instance_class_name:
            handler = self.__get_event_handler()
            if handler:
                try:
                    await handler(
                        msg_data=self.msg_cleared_info,
                        elastic_client=self.elastic_client,
                    )
                except Exception as ex:
                    print("ProcessInstanceChangesHandler", type(ex), ex)
                    raise ex
