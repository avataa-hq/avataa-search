import asyncio
import functools
import os
import signal
# from multiprocessing import Process

from confluent_kafka import Consumer
from resistant_kafka_avataa.common_schemas import KafkaSecurityConfig

# from resistant_kafka_avataa.consumer import process_kafka_connection
from confluent_kafka import TopicPartition

from kafka_config import config
from kafka_config.config import (
    KAFKA_TURN_ON,
    KAFKA_SECURED,
    KAFKA_SECURITY_PROTOCOL,
    KAFKA_SASL_MECHANISMS,
)

# ,KAFKA_SUBSCRIBE_TOPICS)
from kafka_config.utils import consumer_config
from services.inventory_services.kafka.consumers.inventory_changes.utils import (
    InventoryChangesHandler,
)
from services.kafka_services.kafka_connection_utils import (
    get_token_for_kafka_by_keycloak,
)
# from services.kafka_services.processors import *


shutdown_event = asyncio.Event()


def _on_assign(consumer: Consumer, partitions: list[TopicPartition]) -> None:
    # new_partitions = []
    # for partition in partitions:
    #     low, high = consumer.get_watermark_offsets(partition, cached=False)
    #     if partition.offset < low or partition.offset > high:
    #         print(
    #             f"Offset {partition.offset} out of range for {partition.topic}[{partition.partition}].
    #             Resetting to {low}."
    #         )
    #         partition.offset = low  # Reset to earliest
    #     new_partitions.append(partition)
    #
    # consumer.assign(new_partitions)
    cons_id = consumer.memberid()
    for p in partitions:
        print(
            f"Consumer {cons_id} assigned to the topic: {p.topic}, partition {p.partition}."
        )


def _on_lost(consumer: Consumer, partitions: list[TopicPartition]) -> None:
    cons_id = consumer.memberid()
    for p in partitions:
        print(
            f"Consumer {cons_id} lost the topic: {p.topic}, partition {p.partition}."
        )


def _on_revoke(consumer: Consumer, partitions: list[TopicPartition]) -> None:
    consumer.commit()
    cons_id = consumer.memberid()
    consumer.unassign()
    print(f"Consumer {cons_id} will be rebalanced.")


def handle_shutdown():
    shutdown_event.set()


async def run_kafka_cons_inv():
    if config.KAFKA_TURN_ON:
        loop = asyncio.get_running_loop()
        # Debug for check special offset in special partition in
        # from confluent_kafka.cimpl import TopicPartition
        # part_num =4
        # part_offset = 98735
        # partitions = [TopicPartition(config.KAFKA_INVENTORY_CHANGES_TOPIC, part_num, part_offset)]
        # kafka_inventory_changes_consumer.assign(partitions)
        kafka_inventory_changes_consumer = Consumer(
            consumer_config(config.KAFKA_CONSUMER_CONNECT_CONFIG)
        )
        kafka_inventory_changes_consumer.subscribe(
            [config.KAFKA_INVENTORY_CHANGES_TOPIC],
            on_assign=_on_assign,
            on_revoke=_on_revoke,
            on_lost=_on_lost,
        )
        try:
            while not shutdown_event.is_set():
                msg = await loop.run_in_executor(
                    None,
                    functools.partial(
                        kafka_inventory_changes_consumer.poll, 1.0
                    ),
                )
                if msg is None:
                    continue
                handler_inst = InventoryChangesHandler(kafka_msg=msg)
                await handler_inst.process_the_message()
                kafka_inventory_changes_consumer.commit(
                    asynchronous=True, message=msg
                )
        finally:
            print("Shutting down consumer...")
            kafka_inventory_changes_consumer.close()
            print("Kafka consumer closed.")


def init_kafka():
    print("Parent PID:", os.getpid())
    if KAFKA_TURN_ON:
        processes = []
        print(processes)
        security_config = None
        if KAFKA_SECURED:
            security_config = KafkaSecurityConfig(
                oauth_cb=get_token_for_kafka_by_keycloak,
                security_protocol=KAFKA_SECURITY_PROTOCOL,
                sasl_mechanisms=KAFKA_SASL_MECHANISMS,
            )
            print(security_config)
        # group_data_changes_processor = run_process_group_data_changes(security_config)
        # group_processor = run_process_group(security_config)
        # hierarchy_changes_processor = run_process_hierarchy_changes(security_config)
        # inventory_changes_part_processor = run_process_inventory_changes_part(security_config)
        # inventory_security_processor = run_process_inventory_security(security_config)
        # process_changes_processor = run_process_process_changes(security_config)
        # zeebe_process_instance_exporter_processor = run_process_zeebe_process_instance_exporter(security_config)

        # all_processors = [
        #     group_data_changes_processor,
        #     group_processor,
        #     hierarchy_changes_processor,
        #     inventory_changes_part_processor,
        #     inventory_security_processor,
        #     process_changes_processor,
        #     zeebe_process_instance_exporter_processor,
        # ]
        #
        # all_processors = [
        #     run_process_group_data_changes,
        #     run_process_group,
        #     # run_process_hierarchy_changes,
        #     # run_process_inventory_changes_part,
        #     # run_process_inventory_security,
        #     # run_process_process_changes,
        #     # run_process_zeebe_process_instance_exporter,
        # ]

        # for processor in all_processors:
        #     # p = Process(target=processor, args=(security_config,))
        # p = Process(target=run_process_group_data_changes, args=(security_config,))
        # p.start()
        # print(f"Started child process with PID: {p.pid}")
        # processes.append(p)
        #
        # for p in processes:
        #     p.join()

        # if KAFKA_SECURED:
        #     security_config = KafkaSecurityConfig(
        #         oauth_cb=get_token_for_kafka_by_keycloak,
        #         security_protocol=KAFKA_SECURITY_PROTOCOL,
        #         sasl_mechanisms=KAFKA_SASL_MECHANISMS,
        #     )
        #
        # inventory_changes_processor = run_process_inventory_changes(
        #     security_config=security_config
        # )
        #
        # groups_changes_processor = run_groups_changes(
        #     security_config=security_config
        # )
        #
        # asyncio.create_task(
        #     process_kafka_connection(
        #         [inventory_changes_processor, groups_changes_processor]
        #     )
        # )


if __name__ == "__main__":
    print("Kafka connect - start")

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_shutdown)

    try:
        loop.run_until_complete(run_kafka_cons_inv())
    finally:
        loop.close()
        print("Event loop closed.")
