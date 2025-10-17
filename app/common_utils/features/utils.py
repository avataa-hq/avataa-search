from elasticsearch import AsyncElasticsearch

from common_utils.dto_models.models import (
    CountAndItemsAsList,
    AllResAsListQueryModel,
    CountAndItemsAsDict,
    AllResAsDictQueryModel,
)


async def get_count_and_all_items_as_list_from_special_index(
    index: str,
    query_model: AllResAsListQueryModel,
    async_client: AsyncElasticsearch,
) -> CountAndItemsAsList:
    """Returns all data from special indexes as CountAndItemsAsList"""
    result_list = list()

    search_body = query_model.get_search_body()
    search_body["track_total_hits"] = True

    if query_model.limit:
        search_res = await async_client.search(
            index=index, body=search_body, ignore_unavailable=True
        )
        total_hits = search_res["hits"]["total"]["value"]
        search_res = search_res["hits"]["hits"]
        result_list = [item["_source"] for item in search_res]

    else:
        while True:
            search_res = await async_client.search(
                index=index, body=search_body, ignore_unavailable=True
            )

            total_hits = search_res["hits"]["total"]["value"]

            search_res = search_res["hits"]["hits"]

            result_list.extend([item["_source"] for item in search_res])

            if total_hits < query_model.items_per_step:
                break

            if len(search_res) == query_model.items_per_step:
                search_after = search_res[-1]["sort"]
                search_body["search_after"] = search_after
            else:
                break

    return CountAndItemsAsList(count=total_hits, items=result_list)


async def get_count_and_all_items_as_dict_from_special_index(
    index: str,
    query_model: AllResAsDictQueryModel,
    async_client: AsyncElasticsearch,
) -> CountAndItemsAsDict:
    """Returns all data from special indexes as CountAndItemsAsDict"""
    result_dict = dict()

    search_body = query_model.get_search_body()
    search_body["track_total_hits"] = True
    key = query_model.key_attr_for_dict

    if query_model.limit:
        search_res = await async_client.search(
            index=index, body=search_body, ignore_unavailable=True
        )
        total_hits = search_res["hits"]["total"]["value"]
        search_res = search_res["hits"]["hits"]
        result_dict = {
            item_sr[key]: item_sr
            for item in search_res
            if (item_sr := item["_source"])
        }

    else:
        while True:
            search_res = await async_client.search(
                index=index, body=search_body, ignore_unavailable=True
            )

            total_hits = search_res["hits"]["total"]["value"]

            search_res = search_res["hits"]["hits"]

            step_dict = {
                item_sr[key]: item_sr
                for item in search_res
                if (item_sr := item["_source"])
            }
            result_dict.update(step_dict)

            if total_hits < query_model.items_per_step:
                break

            if len(search_res) == query_model.items_per_step:
                search_after = search_res[-1]["sort"]
                search_body["search_after"] = search_after
            else:
                break

    return CountAndItemsAsDict(count=total_hits, items=result_dict)
