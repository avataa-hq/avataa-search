import datetime
from threading import Event

from confluent_kafka import Consumer

from elastic.client import get_async_client
from services.inventory_services.kafka.consumers.inventory_changes.events.mo_msg import (
    on_create_mo,
    on_update_mo,
    on_delete_mo,
)
from services.inventory_services.kafka.consumers.inventory_changes.events.prm_msg import (
    on_create_prm,
    on_update_prm,
    on_delete_prm,
)
from services.inventory_services.kafka.consumers.inventory_changes.events.tmo_msg import (
    on_create_tmo,
    on_update_tmo,
    on_delete_tmo,
)
from services.inventory_services.kafka.consumers.inventory_changes.events.tprm_msg import (
    on_create_tprm,
    on_update_tprm,
    on_delete_tprm,
)

from . import config
from google.protobuf import json_format

from .utils import ObjClassNames, ObjEventStatus, consumer_config


async def read_kafka_topics(event: Event):
    consumer = Consumer(consumer_config(config.KAFKA_CONSUMER_CONNECT_CONFIG))
    consumer.subscribe(config.KAFKA_SUBSCRIBE_TOPICS)

    while True:
        if event.is_set():
            break
        print(f"readed at {datetime.datetime.now()}")
        msg = consumer.poll(2.0)
        if msg is None:
            continue

        try:
            msg_class_name, msg_event = msg.key().decode("utf-8").split(":")
        except ValueError:
            continue
        except AttributeError:
            continue

        if msg_class_name not in config.KAFKA_PROTOBUF_DESERIALIZERS.keys():
            continue

        msg_object = config.KAFKA_PROTOBUF_DESERIALIZERS[msg_class_name]()
        msg_object.ParseFromString(msg.value())
        if msg_object is not None:
            message_as_dict = json_format.MessageToDict(
                msg_object,
                # including_default_value_fields=False,
                preserving_proto_field_name=True,
            )

            await adapter_function(msg_class_name, msg_event, message_as_dict)
            consumer.commit(message=msg)

    consumer.close()


async def adapter_function(msg_class_name, msg_event, message_as_dict):
    async_client = anext(get_async_client())
    async_client = await async_client

    # TMO cases
    if msg_class_name == ObjClassNames.TMO.value:
        if msg_event == ObjEventStatus.CREATED.value:
            await on_create_tmo(msg=message_as_dict, async_client=async_client)
        elif msg_event == ObjEventStatus.UPDATED.value:
            await on_update_tmo(msg=message_as_dict, async_client=async_client)
        elif msg_event == ObjEventStatus.DELETED.value:
            await on_delete_tmo(msg=message_as_dict, async_client=async_client)

    # TPRM cases
    elif msg_class_name == ObjClassNames.TPRM.value:
        if msg_event == ObjEventStatus.CREATED.value:
            await on_create_tprm(msg=message_as_dict, async_client=async_client)
        elif msg_event == ObjEventStatus.UPDATED.value:
            await on_update_tprm(msg=message_as_dict, async_client=async_client)
        else:
            await on_delete_tprm(msg=message_as_dict, async_client=async_client)

    # MO cases
    elif msg_class_name == ObjClassNames.MO.value:
        if msg_event == ObjEventStatus.CREATED.value:
            print(message_as_dict)
            await on_create_mo(msg=message_as_dict, async_client=async_client)
        elif msg_event == ObjEventStatus.UPDATED.value:
            await on_update_mo(msg=message_as_dict, async_client=async_client)
        else:
            await on_delete_mo(msg=message_as_dict, async_client=async_client)

    elif msg_class_name == ObjClassNames.PRM.value:
        if msg_event == ObjEventStatus.CREATED.value:
            await on_create_prm(msg=message_as_dict, async_client=async_client)
        elif msg_event == ObjEventStatus.UPDATED.value:
            await on_update_prm(msg=message_as_dict, async_client=async_client)
        else:
            await on_delete_prm(msg=message_as_dict, async_client=async_client)
    await async_client.close()
