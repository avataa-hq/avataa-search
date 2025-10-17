import copy

from elasticsearch import AsyncElasticsearch


async def get_all_data_from_special_index(
    index: str,
    body: dict,
    results_as_dict: bool,
    async_client: AsyncElasticsearch,
):
    """Returns all data from special indexes"""
    SIZE_PER_STEP = 10_000
    result_dict = dict()
    result_list = list()

    copy_of_body = copy.deepcopy(body)
    copy_of_body["size"] = SIZE_PER_STEP

    if "sort" not in copy_of_body:
        body["sort"] = {"id": {"order": "asc"}}

    copy_of_body["track_total_hits"] = True

    while True:
        search_res = await async_client.search(
            index=index, body=copy_of_body, ignore_unavailable=True
        )

        total_hits = search_res["hits"]["total"]["value"]

        search_res = search_res["hits"]["hits"]

        if results_as_dict:
            for item in search_res:
                item = item["_source"]
                item_id = item.get("id")
                result_dict[item_id] = item
        else:
            result_list.extend([item["_source"] for item in search_res])

        if total_hits < SIZE_PER_STEP:
            break

        if len(search_res) == SIZE_PER_STEP:
            search_after = search_res[-1]["sort"]
            copy_of_body["search_after"] = search_after
        else:
            break

    if results_as_dict:
        return result_dict
    return result_list
