import logging

from elasticsearch import AsyncElasticsearch

from elastic.config import ES_PASS, ES_USER, ES_URL, ES_PROTOCOL

logging.getLogger("elastic_transport.transport").setLevel(logging.WARNING)
logging.getLogger("elasticsearch").setLevel(logging.WARNING)
logging.getLogger("elastic_transport").setLevel(logging.WARNING)


async def get_async_client():
    """Generator of elastic async session"""
    if ES_PROTOCOL == "https":
        async_client = AsyncElasticsearch(
            ES_URL,
            ca_certs="./elastic/ca.crt",
            http_auth=(ES_USER, ES_PASS),
            request_timeout=10000,
            retry_on_status=(500, 502, 503, 504),
            max_retries=5,
        )
    else:
        async_client = AsyncElasticsearch(
            ES_URL,
            request_timeout=10000,
        )

    async with async_client as elastic_client:
        yield elastic_client


class ElasticsearchManager:
    _instance = None
    _client = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def get_client(self):
        if self._client is None:
            if ES_PROTOCOL == "https":
                self._client = AsyncElasticsearch(
                    ES_URL,
                    ca_certs="./elastic/ca.crt",
                    http_auth=(ES_USER, ES_PASS),
                    request_timeout=10000,
                    retry_on_status=(500, 502, 503, 504),
                    max_retries=5,
                )
            else:
                self._client = AsyncElasticsearch(
                    ES_URL,
                    request_timeout=10000,
                )

        return self._client

    async def close(self):
        if self._client:
            await self._client.close()
            self._client = None
            self._initialized = False
