import asyncio
import json
from datetime import datetime

from confluent_kafka import Consumer

from kafka_config.utils import consumer_config
from security.kafka.models import SecurityClass, SecurityEvent
from security.kafka.utils import (
    create_permission,
    update_permission,
    delete_permission,
)
from security.security_config import (
    KAFKA_CONSUMER_CONNECT_CONFIG,
    KAFKA_SECURITY_TOPIC,
)


async def read_security_topic():
    consumer = Consumer(consumer_config(KAFKA_CONSUMER_CONNECT_CONFIG))
    consumer.subscribe([KAFKA_SECURITY_TOPIC])

    while True:
        print(f"readed security at {datetime.now()}")
        msg = consumer.poll(2.0)
        if not msg:
            continue

        msg_class, msg_event = msg.key().decode("utf-8").split(":")
        msg_dict = json.loads(msg.value())

        await apply_event(msg_class, msg_event, msg_dict)
        consumer.commit(message=msg)

    consumer.close()


def kafka_security_entry():
    asyncio.run(read_security_topic())


async def apply_event(msg_class, msg_event, msg_dict):
    if msg_class == SecurityClass.TMO.value:
        if msg_event == SecurityEvent.CREATED.value:
            await create_permission(msg_dict, "tmo")
        elif msg_event == SecurityEvent.UPDATED.value:
            await update_permission(msg_dict, "tmo")
        elif msg_event == SecurityEvent.DELETED.value:
            await delete_permission(msg_dict, "tmo")
    elif msg_class == SecurityClass.TPRM.value:
        if msg_event == SecurityEvent.CREATED.value:
            await create_permission(msg_dict, "tprm")
        elif msg_event == SecurityEvent.UPDATED.value:
            await update_permission(msg_dict, "tprm")
        elif msg_event == SecurityEvent.DELETED.value:
            await delete_permission(msg_dict, "tprm")
    elif msg_class == SecurityClass.MO.value:
        if msg_event == SecurityEvent.CREATED.value:
            await create_permission(msg_dict, "mo")
        elif msg_event == SecurityEvent.UPDATED.value:
            await update_permission(msg_dict, "mo")
        elif msg_event == SecurityEvent.DELETED.value:
            await delete_permission(msg_dict, "mo")
