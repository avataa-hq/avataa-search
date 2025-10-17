from copy import deepcopy
from typing import AsyncIterator

from elasticsearch import AsyncElasticsearch


SIZE_PER_STEP = 10_000


async def scroll_search(
    elastic_client: AsyncElasticsearch,
    index: str,
    body: dict,
) -> list:
    """Implement search based on Scroll in Elastic"""
    output: list = []
    scroll_time = "2s"
    response = await elastic_client.search(
        index=index,
        body=body,
        ignore_unavailable=True,
        size=SIZE_PER_STEP,
        scroll=scroll_time,
    )
    print(f"{id(elastic_client)=}")
    scroll_id = response["_scroll_id"]
    hits = response["hits"]["hits"]
    output.extend(hits)
    if response["hits"]["total"]["value"] > SIZE_PER_STEP:
        while len(hits) > 0:
            search_res = await elastic_client.scroll(
                scroll_id=scroll_id,
                scroll=scroll_time,
            )
            print(f"Cycle: {id(elastic_client)=}")
            hits = search_res["hits"]["hits"]
            output.extend(hits)
    await elastic_client.clear_scroll(scroll_id=scroll_id)
    print(body)
    print(response)
    return output


async def search_after(
    elastic_client: AsyncElasticsearch,
    index: str,
    body: dict,
) -> list:
    """Implement search based on search_after in Elastic"""
    output: list = []
    body["size"] = SIZE_PER_STEP
    while True:
        response = await elastic_client.search(
            index=index, body=body, ignore_unavailable=True
        )

        total_hits = response["hits"]["total"]["value"]
        hits = response["hits"]["hits"]

        output.extend(hits)

        if total_hits < SIZE_PER_STEP:
            break

        if len(hits) == SIZE_PER_STEP:
            search_after_current = response["hits"]["hits"][-1]["sort"]
            body["search_after"] = search_after_current
        else:
            break
    return output


async def search_after_generator(
    elastic_client: AsyncElasticsearch, body: dict
) -> AsyncIterator:
    """Implement search based on search_after in Elastic after 10_000"""
    base_query = deepcopy(body)
    start_from: int = int(body.get("from_", 0))
    size: int = int(body.get("size", 0))
    base_query.pop("from_", None)
    base_query.pop("size", None)

    if "sort" not in base_query:
        raise ValueError("Field 'sort' must be included in query.")

    search_after_field = None
    total_yielded = 0
    total_skipped = 0

    # Skip documents before search
    if start_from > 0:
        while total_skipped < start_from:
            # Get start chunk
            chunk_size = min(SIZE_PER_STEP, start_from - total_skipped)

            query = deepcopy(base_query)
            query["size"] = chunk_size
            if search_after_field:
                query["search_after"] = search_after_field

            response = await elastic_client.search(**query)
            hits = response["hits"]["hits"]

            if not hits:
                return

            total_skipped += len(hits)
            search_after_field = hits[-1]["sort"]

    # Get correct data
    while total_yielded < size:
        remaining = size - total_yielded
        chunk_size = min(SIZE_PER_STEP, remaining)

        query = deepcopy(base_query)
        query["size"] = chunk_size
        if search_after_field:
            query["search_after"] = search_after_field

        response = await elastic_client.search(**query)
        hits = response["hits"]["hits"]

        if not hits:
            return

        for hit in hits:
            yield hit["_source"]
            total_yielded += 1
            if total_yielded >= size:
                return

        search_after_field = hits[-1]["sort"]


async def search_after_generator_with_total(
    elastic_client: AsyncElasticsearch, body: dict
) -> AsyncIterator:
    """Implement search based on search_after in Elastic after 10_000"""
    result = dict()
    base_query = deepcopy(body)
    start_from: int = int(body.get("from_", 0))
    size: int = int(body.get("size", 0))
    base_query.pop("from_", None)
    base_query.pop("size", None)

    if "sort" not in base_query:
        raise ValueError("Field 'sort' must be included in query.")

    search_after_field = None
    total_yielded = 0
    total_skipped = 0

    # Skip documents before search
    if start_from > 0:
        while total_skipped < start_from:
            # Get start chunk
            chunk_size = min(SIZE_PER_STEP, start_from - total_skipped)

            query = deepcopy(base_query)
            query["size"] = chunk_size
            if search_after_field:
                query["search_after"] = search_after_field

            response = await elastic_client.search(**query)
            hits = response["hits"]["hits"]
            result.update({"hits": response["hits"]["total"]["value"]})

            if not hits:
                return

            total_skipped += len(hits)
            search_after_field = hits[-1]["sort"]

    # Get correct data
    while total_yielded < size:
        remaining = size - total_yielded
        chunk_size = min(SIZE_PER_STEP, remaining)

        query = deepcopy(base_query)
        query["size"] = chunk_size
        if search_after_field:
            query["search_after"] = search_after_field

        response = await elastic_client.search(**query)
        hits = response["hits"]["hits"]
        if not result.get("update"):
            result.update({"hits": response["hits"]["total"]["value"]})

        if not hits:
            return

        for hit in hits:
            result.update({"data": hit["_source"]})
            yield result
            total_yielded += 1
            if total_yielded >= size:
                return

        search_after_field = hits[-1]["sort"]
