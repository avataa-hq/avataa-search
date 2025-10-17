import math

from elasticsearch import AsyncElasticsearch
from pytest_asyncio import fixture

from elastic.config import (
    INVENTORY_PRM_INDEX,
    DEFAULT_SETTING_FOR_PRM_INDEX,
    INVENTORY_TMO_INDEX_V2,
    DEFAULT_SETTING_FOR_TMO_INDEX,
    INVENTORY_TPRM_INDEX_V2,
    DEFAULT_SETTING_FOR_TPRM_INDEX,
    INVENTORY_OBJ_INDEX_PREFIX,
    INVENTORY_MO_LINK_INDEX,
    INVENTORY_PRM_LINK_INDEX,
)
from indexes_mapping.inventory.mapping import (
    INVENTORY_PRM_INDEX_MAPPING,
    INVENTORY_TMO_INDEX_MAPPING,
    INVENTORY_TPRM_INDEX_MAPPING,
    INVENTORY_PRM_AND_MO_LINK_INDEX_MAPPING,
)


@fixture(scope="session", autouse=True)
async def create_base_indexes(elastic_instance):
    es_url = elastic_instance.get_url()

    async with AsyncElasticsearch(es_url) as client:
        all_indexes = await client.indices.get_alias(index="*")
        set_of_indexes = set(all_indexes)

        # if INVENTORY_PRM_INDEX in set_of_indexes:
        #     await client.indices.delete(index=INVENTORY_PRM_INDEX, ignore_unavailable=True)
        #
        # if INVENTORY_TMO_INDEX_V2 in set_of_indexes:
        #     await client.indices.delete(index=INVENTORY_TMO_INDEX_V2, ignore_unavailable=True)
        #
        # if INVENTORY_TPRM_INDEX_V2 in set_of_indexes:
        #     await client.indices.delete(index=INVENTORY_TPRM_INDEX_V2, ignore_unavailable=True)
        #
        # if INVENTORY_MO_LINK_INDEX in set_of_indexes:
        #     await client.indices.delete(index=INVENTORY_MO_LINK_INDEX, ignore_unavailable=True)
        #
        # if INVENTORY_PRM_LINK_INDEX in set_of_indexes:
        #     await client.indices.delete(index=INVENTORY_PRM_LINK_INDEX, ignore_unavailable=True)

        if INVENTORY_PRM_INDEX not in set_of_indexes:
            await client.indices.create(
                index=INVENTORY_PRM_INDEX,
                mappings=INVENTORY_PRM_INDEX_MAPPING,
                settings=DEFAULT_SETTING_FOR_PRM_INDEX,
            )

        if INVENTORY_TMO_INDEX_V2 not in set_of_indexes:
            await client.indices.create(
                index=INVENTORY_TMO_INDEX_V2,
                mappings=INVENTORY_TMO_INDEX_MAPPING,
                settings=DEFAULT_SETTING_FOR_TMO_INDEX,
            )

        if INVENTORY_TPRM_INDEX_V2 not in set_of_indexes:
            await client.indices.create(
                index=INVENTORY_TPRM_INDEX_V2,
                mappings=INVENTORY_TPRM_INDEX_MAPPING,
                settings=DEFAULT_SETTING_FOR_TPRM_INDEX,
            )
        if INVENTORY_MO_LINK_INDEX not in set_of_indexes:
            await client.indices.create(
                index=INVENTORY_MO_LINK_INDEX,
                mappings=INVENTORY_PRM_AND_MO_LINK_INDEX_MAPPING,
                settings=DEFAULT_SETTING_FOR_PRM_INDEX,
            )
        if INVENTORY_PRM_LINK_INDEX not in set_of_indexes:
            await client.indices.create(
                index=INVENTORY_PRM_LINK_INDEX,
                mappings=INVENTORY_PRM_AND_MO_LINK_INDEX_MAPPING,
                settings=DEFAULT_SETTING_FOR_PRM_INDEX,
            )


@fixture(scope="function")
async def async_elastic_session(
    async_elastic_session: AsyncElasticsearch,
) -> AsyncElasticsearch:
    """Clear all data"""

    yield async_elastic_session

    # delete all new  inventory_obj_tmo indexes
    all_indexes = await async_elastic_session.indices.get_alias(index="*")
    indexes_to_delete = [
        index_name
        for index_name in all_indexes
        if index_name.startswith(INVENTORY_OBJ_INDEX_PREFIX)
    ]
    if indexes_to_delete:
        delete_per_step = 10
        count_of_indexes = len(indexes_to_delete)
        steps = math.ceil(count_of_indexes / delete_per_step)
        for step in range(steps):
            start = step * delete_per_step
            end = start + delete_per_step
            to_delete = indexes_to_delete[start:end]
            await async_elastic_session.indices.delete(
                index=to_delete, ignore_unavailable=True
            )

    # clear all tprm data in INVENTORY_TPRM_INDEX_V2
    delete_query = {"match_all": {}}
    await async_elastic_session.delete_by_query(
        index=INVENTORY_TPRM_INDEX_V2,
        query=delete_query,
        ignore_unavailable=True,
        refresh=True,
    )
    # clear all tmo data in INVENTORY_TMO_INDEX_V2
    delete_query = {"match_all": {}}
    await async_elastic_session.delete_by_query(
        index=INVENTORY_TMO_INDEX_V2,
        query=delete_query,
        ignore_unavailable=True,
        refresh=True,
    )

    # clear all tmo data in INVENTORY_MO_LINK_INDEX
    delete_query = {"match_all": {}}
    await async_elastic_session.delete_by_query(
        index=INVENTORY_MO_LINK_INDEX,
        query=delete_query,
        ignore_unavailable=True,
        refresh=True,
    )

    # clear all tmo data in INVENTORY_PRM_LINK_INDEX
    delete_query = {"match_all": {}}
    await async_elastic_session.delete_by_query(
        index=INVENTORY_PRM_LINK_INDEX,
        query=delete_query,
        ignore_unavailable=True,
        refresh=True,
    )

    # clear all tmo data in INVENTORY_PRM_INDEX
    delete_query = {"match_all": {}}
    await async_elastic_session.delete_by_query(
        index=INVENTORY_PRM_INDEX,
        query=delete_query,
        ignore_unavailable=True,
        refresh=True,
    )
