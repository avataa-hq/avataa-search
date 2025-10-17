from typing import Annotated

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
from v3.db.base_db import BaseTable
from v3.db.implementation.es.es_hierarchy_secured_db import (
    HierarchiesEsSecuredTable,
    LevelsEsSecuredTable,
    ObjectsEsSecuredTable,
    NodesEsSecuredTable,
)
from v3.db.implementation.es.es_inventory_secured_db import (
    TmoEsSecuredTable,
    MoEsSecuredTable,
)
from v3.models.input.operators.input_union import base_operators_union


"""
This is where the endpoints for filtering hierarchy entities are stored
"""

router = APIRouter(prefix="/hierarchy", tags=["hierarchy"])


@router.post("/hierarchies")
async def hierarchies(
    query: Annotated[base_operators_union, Body(embed=True)],
    es_connection: Annotated[AsyncElasticsearch, Depends(get_async_client)],
    user_data: UserData = Depends(security),
):
    """
    This endpoint is used to search for hierarchy entities
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

    try:
        response = [i async for i in hierarchy_table.find_by_query(query=query)]
    except NotFound as e:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e)
        )

    return response


@router.post("/levels")
async def levels(
    query: Annotated[base_operators_union, Body(embed=True)],
    es_connection: Annotated[AsyncElasticsearch, Depends(get_async_client)],
    user_data: UserData = Depends(security),
):
    """
    This endpoint is used to search for level entities
    """
    is_admin = check_permission_is_admin(client_role=user_data.realm_access)
    user_permissions = get_permissions_from_client_role(
        client_role=user_data.realm_access
    )
    tmo_table = TmoEsSecuredTable(
        connection=es_connection,
        is_admin=is_admin,
        permissions=user_permissions,
    )
    levels_table: BaseTable = LevelsEsSecuredTable(
        connection=es_connection,
        tmo_table=tmo_table,
        is_admin=is_admin,
        permissions=user_permissions,
    )

    try:
        response = [i async for i in levels_table.find_by_query(query=query)]
    except NotFound as e:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e)
        )

    return response


@router.post("/objects")
async def objects(
    tmo_id: Annotated[int, Body(embed=True, ge=1)],
    query: Annotated[base_operators_union, Body(embed=True)],
    es_connection: Annotated[AsyncElasticsearch, Depends(get_async_client)],
    user_data: UserData = Depends(security),
):
    """
    This endpoint is used to search for object entities
    """
    is_admin = check_permission_is_admin(client_role=user_data.realm_access)
    user_permissions = get_permissions_from_client_role(
        client_role=user_data.realm_access
    )
    mo_table = MoEsSecuredTable(
        connection=es_connection,
        table_id=tmo_id,
        is_admin=is_admin,
        permissions=user_permissions,
    )
    nodes_table: BaseTable = NodesEsSecuredTable(
        connection=es_connection,
        is_admin=is_admin,
        permissions=user_permissions,
        mo_table=mo_table,
    )
    objects_table: BaseTable = ObjectsEsSecuredTable(
        connection=es_connection,
        is_admin=is_admin,
        permissions=user_permissions,
        nodes_table=nodes_table,
    )

    try:
        response = [i async for i in objects_table.find_by_query(query=query)]
    except NotFound as e:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e)
        )

    return response


@router.post("/nodes")
async def nodes(
    tmo_id: Annotated[int, Body(embed=True, ge=1)],
    query: Annotated[base_operators_union, Body(embed=True)],
    es_connection: Annotated[AsyncElasticsearch, Depends(get_async_client)],
    user_data: UserData = Depends(security),
):
    """
    This endpoint is used to search for node entities
    """
    is_admin = check_permission_is_admin(client_role=user_data.realm_access)
    user_permissions = get_permissions_from_client_role(
        client_role=user_data.realm_access
    )
    mo_table = MoEsSecuredTable(
        connection=es_connection,
        table_id=tmo_id,
        is_admin=is_admin,
        permissions=user_permissions,
    )
    nodes_table: BaseTable = NodesEsSecuredTable(
        connection=es_connection,
        is_admin=is_admin,
        permissions=user_permissions,
        mo_table=mo_table,
    )

    try:
        response = [i async for i in nodes_table.find_by_query(query=query)]
    except NotFound as e:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e)
        )

    return response
