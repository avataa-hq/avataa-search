from elasticsearch import AsyncElasticsearch, NotFoundError
from elasticsearch.helpers import async_bulk

from elastic.config import INVENTORY_TPRM_INDEX_V2
from elastic.enum_models import InventoryFieldValType
from elastic.query_builder_service.inventory_index.utils.convert_types_utils import (
    get_corresponding_elastic_data_type,
)
from elastic.query_builder_service.inventory_index.utils.index_utils import (
    get_index_name_by_tmo,
)
from indexes_mapping.inventory.mapping import INVENTORY_PARAMETERS_FIELD_NAME


async def on_create_tprm(msg, async_client: AsyncElasticsearch):
    size_per_step = 10000

    tpmr_id_tprm_data = {item["id"]: item for item in msg["objects"]}

    search_query = {"terms": {"id": list(tpmr_id_tprm_data.keys())}}
    search_result = await async_client.search(
        index=INVENTORY_TPRM_INDEX_V2, query=search_query, size=size_per_step
    )

    existing_tprms = {
        item["_source"]["id"] for item in search_result["hits"]["hits"]
    }

    tprm_ids_to_create = set(tpmr_id_tprm_data.keys()) - existing_tprms

    for tprm_id in tprm_ids_to_create:
        tprm_data = tpmr_id_tprm_data[tprm_id]
        await async_client.index(
            index=INVENTORY_TPRM_INDEX_V2,
            id=tprm_id,
            document=tprm_data,
            refresh="true",
        )

        # update mapping
        val_type = tprm_data["val_type"]
        tmo_index_name = get_index_name_by_tmo(tprm_data["tmo_id"])

        if val_type == InventoryFieldValType.PRM_LINK.value:
            tprm_data_constrain = tprm_data["constraint"]
            if not tprm_data_constrain:
                continue
            else:
                search_query = {"match": {"id": tprm_data_constrain}}
                search_result = await async_client.search(
                    index=INVENTORY_TPRM_INDEX_V2, query=search_query, size=1
                )
                cooresponding_tprm = [
                    item["_source"] for item in search_result["hits"]["hits"]
                ]
                if cooresponding_tprm:
                    cooresponding_tprm = cooresponding_tprm[0]
                    val_type = cooresponding_tprm["val_type"]
                else:
                    continue

        properties = {
            INVENTORY_PARAMETERS_FIELD_NAME: {
                "type": "object",
                "properties": {
                    f"{tprm_id}": {
                        "type": get_corresponding_elastic_data_type(val_type)
                    }
                },
            }
        }

        try:
            await async_client.indices.put_mapping(
                index=tmo_index_name, properties=properties
            )
        except NotFoundError:
            continue


async def on_update_tprm(msg, async_client: AsyncElasticsearch):
    size_per_step = 10000
    tpmr_id_tprm_data = {}
    conditions = []
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
        tpmr_id_tprm_data.update({item.get("id"): item})

    search_query = {"bool": {"should": conditions, "minimum_should_match": 1}}
    search_result = await async_client.search(
        index=INVENTORY_TPRM_INDEX_V2, query=search_query, size=size_per_step
    )
    existing_tprm_ids = [
        item["_source"]["id"] for item in search_result["hits"]["hits"]
    ]

    actions = list()
    for tprm_id in existing_tprm_ids:
        new_tprm_data = tpmr_id_tprm_data[tprm_id]
        action_item = dict(
            _index=INVENTORY_TPRM_INDEX_V2,
            _op_type="update",
            _id=tprm_id,
            doc=new_tprm_data,
        )
        actions.append(action_item)

    if actions:
        await async_bulk(client=async_client, refresh="true", actions=actions)


async def on_delete_tprm(msg, async_client: AsyncElasticsearch):
    tpmr_ids = {item["id"] for item in msg["objects"]}

    if tpmr_ids:
        delete_query = {"terms": {"id": list(tpmr_ids)}}
        await async_client.delete_by_query(
            index=INVENTORY_TPRM_INDEX_V2,
            query=delete_query,
            ignore_unavailable=True,
            refresh=True,
        )
