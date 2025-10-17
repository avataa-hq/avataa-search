from elasticsearch import AsyncElasticsearch

from elastic.config import (
    DEFAULT_SETTING_FOR_MO_INDEXES,
    INVENTORY_TMO_INDEX_V2,
)
from elastic.query_builder_service.inventory_index.utils.index_utils import (
    get_index_name_by_tmo,
)
from indexes_mapping.inventory.mapping import INVENTORY_OBJ_INDEX_MAPPING


async def on_create_tmo(msg, async_client: AsyncElasticsearch):
    tmo_ids = list()
    create_indexes = dict()

    for tmo_item in msg["objects"]:
        tmo_id = tmo_item["id"]
        index_name = get_index_name_by_tmo(tmo_id)
        index_exists = await async_client.indices.exists(index=index_name)
        if not index_exists:
            create_indexes[index_name] = tmo_item
        tmo_ids.append(tmo_id)

    for index_name in create_indexes.keys():
        await async_client.indices.create(
            index=index_name,
            mappings=INVENTORY_OBJ_INDEX_MAPPING,
            settings=DEFAULT_SETTING_FOR_MO_INDEXES,
        )

    # existing data in tmo_index
    search_query = {"terms": {"id": tmo_ids}}
    size = len(tmo_ids)
    tmo_data_in_tmo_index = await async_client.search(
        index=INVENTORY_TMO_INDEX_V2, query=search_query, size=size
    )
    existing_tmo_ids = {
        item["_source"]["id"] for item in tmo_data_in_tmo_index["hits"]["hits"]
    }

    for tmo_data in msg["objects"]:
        if tmo_data["id"] not in existing_tmo_ids:
            await async_client.index(
                index=INVENTORY_TMO_INDEX_V2,
                id=tmo_data["id"],
                document=tmo_data,
                refresh="true",
            )


async def on_update_tmo(msg, async_client: AsyncElasticsearch):
    size_per_step = 10000

    tmos = {}
    conditions = []
    # Create filter with conditions as id from msg and version less than stored in Elastic
    for item in msg["objects"]:
        conditions.append(
            {
                "bool": {
                    "must": [
                        {"match": {"id": item.get("id")}},
                        {"range": {"version": {"lt": item.get("version")}}},
                    ]
                }
            }
        )
        tmos.update({item.get("id"): item})
    search_query = {"bool": {"should": conditions, "minimum_should_match": 1}}
    tmo_search_res = await async_client.search(
        index=INVENTORY_TMO_INDEX_V2, query=search_query, size=size_per_step
    )

    existing_tmos = [
        item["_source"]["id"] for item in tmo_search_res["hits"]["hits"]
    ]

    for tmo_id in existing_tmos:
        new_tmo_data = tmos[tmo_id]
        await async_client.index(
            index=INVENTORY_TMO_INDEX_V2,
            id=tmo_id,
            document=new_tmo_data,
            refresh="true",
        )


async def on_delete_tmo(msg, async_client: AsyncElasticsearch):
    indexes_names_tmo_data = {
        get_index_name_by_tmo(item["id"]): item["id"] for item in msg["objects"]
    }

    await async_client.indices.delete(
        index=list(indexes_names_tmo_data.keys()), ignore_unavailable=True
    )

    delete_query = {"terms": {"id": list(indexes_names_tmo_data.values())}}

    await async_client.delete_by_query(
        index=INVENTORY_TMO_INDEX_V2,
        query=delete_query,
        ignore_unavailable=True,
        refresh=True,
    )
