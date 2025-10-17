from elasticsearch import AsyncElasticsearch
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from elastic.client import get_async_client

from services.hierarchy_services.reload.hierarchy_data import (
    HierarchyIndexesReloader,
)

from v2.database.database import get_session

from v2.routers.hierarchy.configs import HIERARCHY_RELOAD_ROUTER_PREFIX

router = APIRouter(
    prefix=HIERARCHY_RELOAD_ROUTER_PREFIX, tags=["Hierarchy indexes: Reload"]
)


@router.get("/reload_all_hierarchy_indexes", status_code=200)
async def reload_all_hierarchy_indexes(
    elastic_client: AsyncElasticsearch = Depends(get_async_client),
    db_session: AsyncSession = Depends(get_session),
):
    reloader = HierarchyIndexesReloader(elastic_client, db_session)
    await reloader.refresh_all_hierarchies_indexes()
    return {"status": "ok"}


@router.get("/reload_all_data_for_special_hierarchy", status_code=200)
async def reload_all_data_for_special_hierarchy(
    hierarchy_id: int,
    elastic_client: AsyncElasticsearch = Depends(get_async_client),
    db_session: AsyncSession = Depends(get_session),
):
    reloader = HierarchyIndexesReloader(elastic_client, db_session)
    await reloader.refresh_index_for_special_hierarchy(
        hierarchy_id=hierarchy_id
    )
    return {"status": "ok"}
