from typing import Annotated, Type

from elasticsearch import AsyncElasticsearch
from fastapi import APIRouter, Body, Depends, HTTPException
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY

from elastic.client import get_async_client
from security.security_data_models import UserData
from security.security_factory import security
from services.inventory_services.utils.security.filter_by_realm import (
    check_permission_is_admin,
    get_permissions_from_client_role,
)
from v3.custom_exceptions.not_found import NotFound
from v3.models.input.hierarchy.hierarchy_filter import HierarchyFilterModel
from v3.db.base_db import BaseTable
from v3.db.implementation.es.es_hierarchy_secured_db import (
    HierarchiesEsSecuredTable,
    LevelsEsSecuredTable,
    NodesEsSecuredTable,
    ObjectsEsSecuredTable,
)
from v3.db.implementation.es.es_inventory_secured_db import (
    TmoEsSecuredTable,
    MoEsSecuredTable,
)
from v3.tasks.hierarchy_task import InvByHierarchyTask, Hierarchy
from v3.tasks.level_task import LevelWayTask

"""
This is where endpoints are stored that combine filtering or responses from multiple microservices
"""

router = APIRouter(prefix="/combine", tags=["combine"])


@router.post("/inv_by_hierarchy")
async def inv_by_hierarchy(
    query: Annotated[HierarchyFilterModel, Body(embed=True)],
    es_connection: Annotated[AsyncElasticsearch, Depends(get_async_client)],
    user_data: UserData = Depends(security),
):
    """
    Endpoint to retrieve inventory data with the possibility of combining hierarchy and inventory filters
    You can retrieve data corresponding to the level identifier
    """
    is_admin = check_permission_is_admin(client_role=user_data.realm_access)
    user_permissions = get_permissions_from_client_role(
        client_role=user_data.realm_access
    )

    hierarchy_table: BaseTable = HierarchiesEsSecuredTable(
        connection=es_connection,
        is_admin=is_admin,
        permissions=user_permissions,
    )
    tmo_table: BaseTable = TmoEsSecuredTable(
        connection=es_connection,
        is_admin=is_admin,
        permissions=user_permissions,
    )
    level_table: BaseTable = LevelsEsSecuredTable(
        connection=es_connection,
        tmo_table=tmo_table,
        is_admin=is_admin,
        permissions=user_permissions,
    )

    level_task = LevelWayTask(
        hierarchy_id=query.filters.hierarchy_id,
        level_id=query.show_data_from_level_id,
        hierarchy_table=hierarchy_table,
        level_table=level_table,
    )
    level_way = await level_task.execute()

    mo_table_class: Type[BaseTable] = MoEsSecuredTable
    node_table_class: Type[BaseTable] = NodesEsSecuredTable
    hierarchy_obj_table_class: Type[BaseTable] = ObjectsEsSecuredTable

    hierarchy_task = InvByHierarchyTask(
        query=query,
        level_way=level_way,
        hierarchy_obj_table_class=hierarchy_obj_table_class,
        node_table_class=node_table_class,
        connection=es_connection,
        mo_table=mo_table_class,
        is_admin=is_admin,
        permissions=user_permissions,
    )

    response = await hierarchy_task.execute()
    return response


@router.post("/hierarchy_obj")
async def hierarchy_obj(
    query: Annotated[HierarchyFilterModel, Body(embed=True)],
    es_connection: Annotated[AsyncElasticsearch, Depends(get_async_client)],
    user_data: UserData = Depends(security),
):
    """
    Endpoint for hierarchical data retrieval with the possibility of combining hierarchy and inventory filters
    You can retrieve data corresponding to the level identifier
    """
    is_admin = check_permission_is_admin(client_role=user_data.realm_access)
    user_permissions = get_permissions_from_client_role(
        client_role=user_data.realm_access
    )

    hierarchy_table: BaseTable = HierarchiesEsSecuredTable(
        connection=es_connection,
        is_admin=is_admin,
        permissions=user_permissions,
    )
    tmo_table: BaseTable = TmoEsSecuredTable(
        connection=es_connection,
        is_admin=is_admin,
        permissions=user_permissions,
    )
    level_table: BaseTable = LevelsEsSecuredTable(
        connection=es_connection,
        tmo_table=tmo_table,
        is_admin=is_admin,
        permissions=user_permissions,
    )

    level_task = LevelWayTask(
        hierarchy_id=query.filters.hierarchy_id,
        level_id=query.show_data_from_level_id,
        hierarchy_table=hierarchy_table,
        level_table=level_table,
    )
    try:
        level_way = await level_task.execute()
    except NotFound as e:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e)
        )

    mo_table_class: Type[BaseTable] = MoEsSecuredTable
    node_table_class: Type[BaseTable] = NodesEsSecuredTable
    hierarchy_obj_table_class: Type[BaseTable] = ObjectsEsSecuredTable

    hierarchy_task = Hierarchy(
        query=query,
        level_way=level_way,
        hierarchy_obj_table_class=hierarchy_obj_table_class,
        node_table_class=node_table_class,
        connection=es_connection,
        mo_table=mo_table_class,
        is_admin=is_admin,
        permissions=user_permissions,
    )
    try:
        response = await hierarchy_task.execute()
    except NotFound as e:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e)
        )

    return response
