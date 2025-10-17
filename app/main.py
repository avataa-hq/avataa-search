import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware


from init_app import create_app
from elastic.client import ElasticsearchManager
from elastic.utils import init_all_necessary_indexes
from kafka_config.config import KAFKA_TURN_ON
from kafka_config.protobuf_consumer import adapter_function
from services.kafka_services.connection_handler.utils import (
    KafkaConnectionHandler,
)
from services.kafka_services.msg_counter.utils import KafkaMSGCounter

from settings import config
from settings.config import PREFIX
from v2.database.database import init_tables as v2_init_tables
from v2.main import v2_app
from v3.api.fast_api.application import v3_app


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("startup")
    await v2_init_tables()
    print("DB initiation - successful")

    # def kafka_entrypoint(event):
    #     asyncio.run(read_kafka_topics(event))

    # stop_event = Event()

    # inventory_mapping = {'properties': {
    #     'params': {'type': 'nested'},
    #     'name': {'type': 'text'},
    #     'Inspection Date': {'type': 'date',
    #                         'format': "date_optional_time||strict_date_optional_time"},
    #     'status': {'type': 'text'},
    #     'Zip': {'type': 'text'},  # TODO dirty hack
    # }}
    # indexes = [PARAMS_INDEX, INVENTORY_INDEX, TMO_INDEX, HIERARCHY_INDEX, PERMISSION_INDEX]
    # titles = ['Params', 'Inventory', 'Object types', 'Hierarchies', 'Permissions']
    # mappings = [None, inventory_mapping, None, None, None]
    #
    # for index, title, mapping in zip(indexes, titles, mappings):
    #     init_index(client, index, title, mapping)

    # if KAFKA_TURN_ON:
    #     kafka_thread = Thread(target=kafka_entrypoint, name='kafka_thread', args=(stop_event,))
    #     kafka_thread.start()
    #
    #     if SECURITY_TYPE in ['KEYCLOAK', 'OPA-JWT-RAW', 'OPA-JWT-PARSED']:
    #         kafka_security_thread = Thread(target=kafka_security_entry, name='kafka_thread')
    #         kafka_security_thread.start()

    # wait for elastic connection:
    print("elastic connection - start")
    # async_client = anext(get_async_client())
    print("elastic connection - end")
    # async_client = await async_client
    print("elastic index initiation - start")
    await init_all_necessary_indexes(
        async_client=ElasticsearchManager().get_client()
    )
    print("elastic index initiation - end")
    # await async_client.close()
    if KAFKA_TURN_ON:
        print("Kafka connect - start")
        loop = asyncio.get_running_loop()
        kafka_connection_handler = KafkaConnectionHandler(
            loop=loop,
            async_message_handler_function=adapter_function,
            msg_counter=KafkaMSGCounter(),
        )

        kafka_connection_handler.connect_to_kafka_topic()
        print("Kafka connect - end")

    yield
    await ElasticsearchManager().close()
    # stop_event.set()


v1_version = "1"

main_options = dict(
    openapi_url=f"{PREFIX}/openapi.json",
    docs_url=f"{PREFIX}/docs",
    lifespan=lifespan,
    root_path=PREFIX,
    debug=config.DEBUG,
)

v1_options = dict(
    title=config.TITLE,
    version=v1_version,
    root_path=f"{PREFIX}/v{v1_version}",
    debug=config.DEBUG,
)

app = create_app(**main_options)
v1_app = create_app(**v1_options)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# v1_app.include_router(inventory.router)

# app.mount("/v1", v1_app)
app.mount("/v2", v2_app)
app.mount("/v3", v3_app)
