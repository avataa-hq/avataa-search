import sys
import os

import pytest_asyncio
from elasticsearch import AsyncElasticsearch
from fastapi.testclient import TestClient
from sqlalchemy import StaticPool
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from testcontainers.elasticsearch import ElasticSearchContainer


from pytest import fixture

from testcontainers.postgres import PostgresContainer

from settings.config import (
    TESTS_RUN_CONTAINER_POSTGRES_LOCAL,
    TEST_DATABASE_URL,
    TEST_LOCAL_DB_HOST,
)

sys.path.append(os.path.join(sys.path[0], "..", "app"))

from v2.database.schema import Base  # noqa

if TESTS_RUN_CONTAINER_POSTGRES_LOCAL:

    class DBContainer(PostgresContainer):
        @property
        def connection_url(self, host: str | None = None) -> str:
            if not host:
                host = TEST_LOCAL_DB_HOST
            return super().get_connection_url(host=host)

    def get_db_image_config():
        config = {
            "image": "postgres:latest",
            "port": 5432,
            "user": "search_admin",
            "password": "search_pass",
            "dbname": "search",
            "driver": "asyncpg",
        }
        return config

    @fixture(scope="session", autouse=True)
    def postgres_instance():
        config = get_db_image_config()
        postgres_container = DBContainer(**config)
        with postgres_container as container:
            yield container
            container.volumes.clear()

    @pytest_asyncio.fixture
    async def engine(postgres_instance):
        db_url = postgres_instance.get_connection_url()
        # db_url = db_url.replace("psycopg2", "asyncpg")
        db_engine = create_async_engine(
            db_url, poolclass=StaticPool, echo=False
        )
        yield db_engine

        await db_engine.dispose()

    @pytest_asyncio.fixture
    async def db_session(engine):
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

            session_maker = async_sessionmaker(engine, expire_on_commit=False)
            async with session_maker() as session:
                yield session

            await connection.run_sync(Base.metadata.drop_all)

else:

    @pytest_asyncio.fixture
    async def engine(postgres_instance):
        db_url = TEST_DATABASE_URL
        db_engine = create_async_engine(
            db_url, poolclass=StaticPool, echo=False
        )
        yield db_engine

        await db_engine.dispose()

    @pytest_asyncio.fixture
    async def db_session(engine):
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

            session_maker = async_sessionmaker(engine, expire_on_commit=False)
            async with session_maker() as session:
                yield session

            await connection.run_sync(Base.metadata.drop_all)


class ElasticContainer(ElasticSearchContainer):
    def get_url(self):
        host = TEST_LOCAL_DB_HOST
        port = self.get_exposed_port(self.port)
        return f"http://{host}:{port}"


def get_elastic_image_config():
    config = {"image": "elasticsearch:8.11.4", "port": 9200}
    return config


@fixture(scope="session", autouse=True)
def elastic_instance():
    config = get_elastic_image_config()
    # postgres_container = DBContainer(**config)
    elastic_container = ElasticContainer(**config)
    # elastic_container = elastic_container.with_name("test_elastic_container")
    #
    env = {
        "CLI_JAVA_OPTS": "-Xms512m -Xmx512m",
        "bootstrap.memory_lock": "true",
        "discovery.type": "single-node",
        "xpack.security.enabled": "false",
        "xpack.security.enrollment.enabled": "false",
        "xpack.security.transport.ssl.enabled": "false",
        "xpack.security.http.ssl.enabled": "false",
        "transport.host": "127.0.0.1",
        "http.host": "0.0.0.0",  # noqa: S104
    }
    # elastic_container.ports.update({9200: 9200})
    elastic_container.env.update(env)

    with elastic_container as container:
        yield container
        container.volumes.clear()


@pytest_asyncio.fixture
async def async_elastic_session(elastic_instance, mocker) -> AsyncElasticsearch:
    es_url = elastic_instance.get_url()
    mocker.patch("elastic.client.ES_URL", new=es_url)
    mocker.patch("elastic.client.ES_PROTOCOL", new="http")
    mocker.patch("elastic.config.ES_PORT", new="9200")
    async with AsyncElasticsearch(es_url) as client:
        yield client


@fixture(scope="function")
def client(db_session, engine):
    def get_session_override():
        return db_session

    from v2.database.database import get_session
    from main import v2_app, app

    v2_app.dependency_overrides[get_session] = get_session_override
    app.dependency_overrides[get_session] = get_session_override

    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()
