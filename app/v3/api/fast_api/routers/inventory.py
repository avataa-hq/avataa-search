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
from v3.db.implementation.es.es_inventory_secured_db import (
    TmoEsSecuredTable,
    TprmEsSecuredTable,
    MoEsSecuredTable,
)
from v3.models.input.operators.input_union import base_operators_union

"""
This is where the endpoints for filtering inventory entities are stored
"""

router = APIRouter(prefix="/inventory", tags=["inventory"])


@router.post("/tmos")
async def tmos(
    query: Annotated[base_operators_union, Body(embed=True)],
    es_connection: Annotated[AsyncElasticsearch, Depends(get_async_client)],
    user_data: UserData = Depends(security),
):
    """
    This endpoint is used to search for object type (TMO) entities
    """
    is_admin = check_permission_is_admin(client_role=user_data.realm_access)
    user_permissions = get_permissions_from_client_role(
        client_role=user_data.realm_access
    )
    tmo_table: BaseTable = TmoEsSecuredTable(
        connection=es_connection,
        is_admin=is_admin,
        permissions=user_permissions,
    )

    try:
        response = [i async for i in tmo_table.find_by_query(query=query)]
    except NotFound as e:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e)
        )

    return response


@router.post("/tprms")
async def tprms(
    query: Annotated[base_operators_union, Body(embed=True)],
    es_connection: Annotated[AsyncElasticsearch, Depends(get_async_client)],
    user_data: UserData = Depends(security),
):
    """
    This endpoint is used to search for parameter type (TPRM) entities
    """
    is_admin = check_permission_is_admin(client_role=user_data.realm_access)
    user_permissions = get_permissions_from_client_role(
        client_role=user_data.realm_access
    )
    tprm_table: BaseTable = TprmEsSecuredTable(
        connection=es_connection,
        is_admin=is_admin,
        permissions=user_permissions,
    )

    try:
        response = [i async for i in tprm_table.find_by_query(query=query)]
    except NotFound as e:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e)
        )

    return response


@router.post("/mos")
async def mos(
    tmo_id: Annotated[int, Body(embed=True, ge=1)],
    query: Annotated[base_operators_union, Body(embed=True)],
    es_connection: Annotated[AsyncElasticsearch, Depends(get_async_client)],
    user_data: UserData = Depends(security),
):
    """
    This endpoint is used to search for object (MO) entities
    """
    is_admin = check_permission_is_admin(client_role=user_data.realm_access)
    user_permissions = get_permissions_from_client_role(
        client_role=user_data.realm_access
    )
    mo_table: BaseTable = MoEsSecuredTable(
        connection=es_connection,
        is_admin=is_admin,
        permissions=user_permissions,
        table_id=tmo_id,
    )

    try:
        response = [i async for i in mo_table.find_by_query(query=query)]
    except NotFound as e:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e)
        )

    return response
