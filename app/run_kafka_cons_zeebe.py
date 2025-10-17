import asyncio
import functools
import signal

from confluent_kafka import Consumer
from confluent_kafka import TopicPartition

from elastic.client import ElasticsearchManager
from kafka_config import config
from kafka_config.config import (
    KAFKA_ZEEBE_CHANGES_TOPIC,
)
from kafka_config.utils import consumer_config
from services.zeebe_services.kafka.consumers.process_changes.utils import (
    ProcessChangesHandler,
)

shutdown_event = asyncio.Event()


TOPIC_PRIORITIES = {
    KAFKA_ZEEBE_CHANGES_TOPIC: 1,
}


def _on_assign(consumer: Consumer, partitions: list[TopicPartition]) -> None:
    cons_id = consumer.memberid()
    for p in partitions:
        print(
            f"Consumer {cons_id} assigned to the topic: {p.topic}, partition {p.partition}."
        )


def handle_shutdown():
    shutdown_event.set()


async def run_kafka_cons_zeebe():
    if config.KAFKA_TURN_ON:
        loop = asyncio.get_running_loop()

        consumer = Consumer(
            consumer_config(config.KAFKA_CONSUMER_CONNECT_CONFIG)
        )
        consumer.subscribe(
            [config.KAFKA_ZEEBE_CHANGES_TOPIC], on_assign=_on_assign
        )
        elastic_client = ElasticsearchManager().get_client()

        try:
            while not shutdown_event.is_set():
                msg = await loop.run_in_executor(
                    None,
                    functools.partial(consumer.poll, 1.0),
                )
                if msg is None:
                    continue

                print(
                    f"Handle the message from topic={msg.topic()} part={msg.partition()} offset={msg.offset()}"
                )

                handler_inst = ProcessChangesHandler(
                    kafka_msg=msg, elastic_client=elastic_client
                )
                await handler_inst.process_the_message()
                consumer.commit(asynchronous=True, message=msg)
                print(
                    f"Committed topic={msg.topic()} part={msg.partition()} offset={msg.offset()}"
                )

        finally:
            print("Shutting down consumer [process.changes]...")
            consumer.close()
            print("Kafka consumer closed [process.changes].")


if __name__ == "__main__":
    print("Kafka consumer [process.changes] connect - start")

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_shutdown)

    try:
        loop.run_until_complete(run_kafka_cons_zeebe())
    finally:
        loop.close()
        print("Event loop closed.")
