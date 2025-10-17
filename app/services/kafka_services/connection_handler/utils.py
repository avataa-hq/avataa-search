import asyncio
import functools
import traceback
from sys import stderr
from typing import Callable
from collections import defaultdict

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    TopicPartition,
    cimpl,
)

from kafka_config import config
from kafka_config.config import (
    KAFKA_ZEEBE_CHANGES_TOPIC,
)
from kafka_config.utils import consumer_config
from services.base_single_tone.utils import SingletonMeta
from services.kafka_services.handler_adapter.utils import MSGHandlerAdapter
from services.kafka_services.msg_counter.model import ProtocolKafkaMSGCounter

TOPIC_PRIORITIES = {
    KAFKA_ZEEBE_CHANGES_TOPIC: 1,
}


class KafkaConnectionHandler(metaclass=SingletonMeta):
    def __init__(
        self,
        loop=None,
        async_message_handler_function: Callable = None,
        msg_counter: ProtocolKafkaMSGCounter = None,
    ):
        self.loop = loop or asyncio.get_event_loop()
        self.number_of_consecutive_empty_messages = -1
        self.__connected = False
        self.consumers = []
        self.message_handler_function = async_message_handler_function
        self.msg_counter = msg_counter

    @property
    def message_handler_function(self):
        return self._message_handler_function

    @message_handler_function.setter
    def message_handler_function(self, value):
        if value is None:

            async def noname(*args, **kwargs):
                return None

            self._message_handler_function = noname
        else:
            self._message_handler_function = value

    @property
    def is_connected(self):
        return self.__connected

    @staticmethod
    def on_connection_to_kafka_topic(*args, **kwargs):
        print(f"Connection to the topics: {config.KAFKA_SUBSCRIBE_TOPICS}")

    @staticmethod
    def on_disconnect_from_kafka_function():
        print("Disconnected from kafka")

    def _on_assign(
        self, consumer: Consumer, partitions: list[TopicPartition]
    ) -> None:
        cons_id = consumer.memberid()
        for p in partitions:
            print(
                f"Consumer {cons_id} assigned to the topic: {p.topic}, partition {p.partition}."
            )

    def connect_to_kafka_topic(self):
        if self.__connected:
            return
        self.on_connection_to_kafka_topic()

        priority_to_topics = defaultdict(list)
        for topic in config.KAFKA_SUBSCRIBE_TOPICS:
            pri = TOPIC_PRIORITIES.get(topic, 999)
            priority_to_topics[pri].append(topic)

        self.consumers = []
        for pri in sorted(priority_to_topics.keys()):
            topics = priority_to_topics[pri]
            if topics:
                cons = Consumer(
                    consumer_config(config.KAFKA_CONSUMER_CONNECT_CONFIG)
                )
                cons.subscribe(topics, on_assign=self._on_assign)
                self.consumers.append((pri, cons))

        self.loop.create_task(self.__start_to_read_connect_to_kafka_topic())

    async def __start_to_read_connect_to_kafka_topic(self):
        self.__connected = True

        while self.__connected:
            try:
                msg = None
                current_cons = None
                for pri, cons in self.consumers:
                    loop = asyncio.get_running_loop()
                    poll = functools.partial(cons.poll, 0.001)
                    msg: cimpl.Message = await loop.run_in_executor(None, poll)

                    if msg and not msg.error():
                        current_cons = cons
                        break

                if msg is None:
                    await asyncio.sleep(0.001)
                    continue

                if current_cons is None:
                    print(msg)
                    await asyncio.sleep(0.001)
                    continue

                self.msg_counter.plus_one()
                print(
                    f"Handle the message from topic={msg.topic()} part={msg.partition()} offset={msg.offset()}"
                )

                handler_adapter = MSGHandlerAdapter(msg_topic=msg.topic())
                if handler_adapter is None:
                    print(f"Handler not exist: {msg.error()=}")
                    continue

                handler_cls = handler_adapter.get_corresponding_handler()

                if handler_cls:
                    handler_inst = handler_cls(kafka_msg=msg)
                    await handler_inst.process_the_message()

                current_cons.commit(asynchronous=True, message=msg)
                print(f"Committed with offset={msg.offset()}")

            except KafkaError:
                print(traceback.format_exc(), file=stderr)
                await asyncio.sleep(60)
            except KafkaException:
                print(f"{msg=}")
                print(traceback.format_exc(), file=stderr)
                await asyncio.sleep(60)
            except Exception as e:
                self.disconnect_from_kafka_topic()
                raise e

    def disconnect_from_kafka_topic(self):
        for pri, cons in self.consumers:
            cons.close()
        self.__connected = False
        self.on_disconnect_from_kafka_function()


def sync_kafka_stopping_to_perform_a_function(func: Callable):
    """SYNC Decorator function. The decorator stops listening to Kafka while the function is executing"""

    def wrapper(*args, **kwargs):
        kafka_connection_handler = KafkaConnectionHandler()
        if kafka_connection_handler.is_connected:
            kafka_connection_handler.disconnect_from_kafka_topic()
            res = func(*args, **kwargs)
            kafka_connection_handler.connect_to_kafka_topic()
        else:
            res = func(*args, **kwargs)
        return res

    return wrapper
