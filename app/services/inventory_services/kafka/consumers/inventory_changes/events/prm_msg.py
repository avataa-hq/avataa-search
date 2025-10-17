from collections import defaultdict

from elasticsearch import AsyncElasticsearch

from elastic.config import (
    ALL_MO_OBJ_INDEXES_PATTERN,
    INVENTORY_PRM_LINK_INDEX,
    INVENTORY_MO_LINK_INDEX,
    INVENTORY_PRM_INDEX,
)

from services.inventory_services.kafka.consumers.inventory_changes.helpers.prm_utils import (
    PRMCreateHandler,
)


async def on_create_prm(msg, async_client: AsyncElasticsearch):
    prm_create_handler = PRMCreateHandler(msg=msg, async_client=async_client)
    await prm_create_handler.on_prm_create()


async def on_update_prm(msg, async_client: AsyncElasticsearch):
    prm_create_handler = PRMCreateHandler(msg=msg, async_client=async_client)
    await prm_create_handler.on_prm_update()


async def on_delete_prm(msg, async_client: AsyncElasticsearch):
    tprm_ids = set()
    prm_ids = set()

    group_mo_ids_by_tprm_id = defaultdict(list)

    for prm_data in msg["objects"]:
        tprm_id = prm_data["tprm_id"]
        mo_id = prm_data["mo_id"]
        group_mo_ids_by_tprm_id[tprm_id].append(mo_id)
        tprm_ids.add(tprm_id)

        prm_ids.add(prm_data["id"])

    # delete parameters
    for tprm_id, mo_ids in group_mo_ids_by_tprm_id.items():
        search_query = {"terms": {"id": mo_ids}}
        update_script = {
            "source": "if (ctx._source.parameters.containsKey(params.param_name)) "
            "{ ctx._source.parameters.remove(params.param_name) }",
            "lang": "painless",
            "params": {"param_name": str(tprm_id)},
        }
        try:
            await async_client.update_by_query(
                index=ALL_MO_OBJ_INDEXES_PATTERN,
                query=search_query,
                script=update_script,
                requests_per_second=1,
                # scroll_size=5,
                refresh=True,
            )
        except Exception as ex:
            print(ex, msg)

    # delete from all prm indexes
    delete_query = {"terms": {"id": list(prm_ids)}}
    all_prm_indexes = [
        INVENTORY_PRM_INDEX,
        INVENTORY_MO_LINK_INDEX,
        INVENTORY_PRM_LINK_INDEX,
    ]
    await async_client.delete_by_query(
        index=all_prm_indexes,
        query=delete_query,
        ignore_unavailable=True,
        refresh=True,
    )
