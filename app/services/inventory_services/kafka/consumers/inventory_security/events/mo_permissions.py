from typing import List
from collections import defaultdict
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk, BulkIndexError

from elastic.config import ALL_MO_OBJ_INDEXES_PATTERN
from indexes_mapping.inventory.mapping import INVENTORY_PERMISSIONS_FIELD_NAME
from services.inventory_services.elastic.security.configs import (
    INVENTORY_SECURITY_MO_PERMISSION_INDEX,
)


async def with_mo_permissions_create(
    msg: List[dict], async_client: AsyncElasticsearch
):
    """Kafka event handler for MO permission crete event"""
    actions = list()
    mo_ids_grouped_by_permissions = defaultdict(list)

    for item in msg:
        item_id = item.get("id")
        mo_id = item.get("parent_id")
        permission = item.get("permission")
        read = item.get("read")
        if item_id:
            action_item = dict(
                _index=INVENTORY_SECURITY_MO_PERMISSION_INDEX,
                _op_type="index",
                _id=item_id,
                _source=item,
            )
            actions.append(action_item)

            if all([read, mo_id, permission]):
                mo_ids_grouped_by_permissions[permission].append(mo_id)

    if actions:
        try:
            await async_bulk(
                client=async_client, refresh="true", actions=actions
            )
        except BulkIndexError as e:
            print(e.errors)
            raise e

    for k, v in mo_ids_grouped_by_permissions.items():
        search_query = {"terms": {"id": v}}
        update_script = {
            "source": "def permissions=ctx._source.%(field_name)s;"
            "if(permissions == null)"
            "{ctx._source.%(field_name)s = new ArrayList()}"
            "else if (permissions instanceof String)"
            "{ctx._source.%(field_name)s = new ArrayList();"
            "ctx._source.%(field_name)s.add(permissions)}"
            "ctx._source.%(field_name)s.add(params['perm_name'])"
            % {"field_name": INVENTORY_PERMISSIONS_FIELD_NAME},
            "lang": "painless",
            "params": {"perm_name": k},
        }

        await async_client.update_by_query(
            index=ALL_MO_OBJ_INDEXES_PATTERN,
            query=search_query,
            script=update_script,
            refresh=True,
        )


async def with_mo_permissions_update(
    msg: List[dict], async_client: AsyncElasticsearch
):
    """Kafka event handler for MO permission update event"""
    actions = list()
    mo_ids_grouped_by_permissions_read_true = defaultdict(list)
    mo_ids_grouped_by_permissions_read_false = defaultdict(list)

    for item in msg:
        item_id = item.get("id")
        mo_id = item.get("parent_id")
        permission = item.get("permission")
        read = item.get("read")
        if item_id:
            action_item = dict(
                _index=INVENTORY_SECURITY_MO_PERMISSION_INDEX,
                _op_type="index",
                _id=item_id,
                _source=item,
            )
            actions.append(action_item)

            if all([mo_id, permission]):
                if read:
                    mo_ids_grouped_by_permissions_read_true[permission].append(
                        mo_id
                    )
                else:
                    mo_ids_grouped_by_permissions_read_false[permission].append(
                        mo_id
                    )

    if actions:
        try:
            await async_bulk(
                client=async_client, refresh="true", actions=actions
            )
        except BulkIndexError as e:
            print(e.errors)
            raise e

    for k, v in mo_ids_grouped_by_permissions_read_true.items():
        search_query = {"terms": {"id": v}}
        update_script = {
            "source": "def permissions=ctx._source.%(field_name)s;"
            "if(permissions == null)"
            "{ctx._source.%(field_name)s = new ArrayList()}"
            "else if (permissions instanceof String)"
            "{ctx._source.%(field_name)s = new ArrayList();"
            "ctx._source.%(field_name)s.add(permissions)}"
            "ctx._source.%(field_name)s.add(params['perm_name'])"
            % {"field_name": INVENTORY_PERMISSIONS_FIELD_NAME},
            "lang": "painless",
            "params": {"perm_name": k},
        }

        await async_client.update_by_query(
            index=ALL_MO_OBJ_INDEXES_PATTERN,
            query=search_query,
            script=update_script,
            refresh=True,
        )

    for k, v in mo_ids_grouped_by_permissions_read_false.items():
        # search_query = {"terms": {"id": v}}
        search_query = {
            "bool": {
                "must": [
                    {"terms": {"id": v}},
                    {"exists": {"field": INVENTORY_PERMISSIONS_FIELD_NAME}},
                    {"term": {INVENTORY_PERMISSIONS_FIELD_NAME: k}},
                ]
            }
        }
        update_script = {
            "source": "def permissions=ctx._source.%(field_name)s;"
            "if (permissions instanceof ArrayList)"
            "{ctx._source.%(field_name)s.removeIf(item -> item == params['perm_name']);"
            "int size = ctx._source.%(field_name)s.size();"
            "if (size == 1)"
            "{ctx._source.%(field_name)s = ctx._source.%(field_name)s[0]}"
            "}"
            "else if (permissions instanceof String)"
            "{ctx._source.%(field_name)s = params['str_default_value']}"
            % {"field_name": INVENTORY_PERMISSIONS_FIELD_NAME},
            "lang": "painless",
            "params": {"perm_name": k, "str_default_value": None},
        }

        await async_client.update_by_query(
            index=ALL_MO_OBJ_INDEXES_PATTERN,
            query=search_query,
            script=update_script,
            refresh=True,
        )


async def with_mo_permissions_delete(
    msg: List[dict], async_client: AsyncElasticsearch
):
    """Kafka event handler for MO permission delete event"""
    ids_to_delete = list()

    mo_ids_grouped_by_permissions = defaultdict(list)

    for item in msg:
        item_id = item.get("id")
        mo_id = item.get("parent_id")
        permission = item.get("permission")
        read = item.get("read")
        if item_id:
            ids_to_delete.append(item_id)

            if all([read, mo_id, permission]):
                mo_ids_grouped_by_permissions[permission].append(mo_id)

    if ids_to_delete:
        delete_query = {
            "bool": {
                "should": [
                    {"terms": {"id": ids_to_delete}},
                    {"terms": {"parent_id": ids_to_delete}},
                ],
                "minimum_should_match": 1,
            }
        }
        await async_client.delete_by_query(
            index=INVENTORY_SECURITY_MO_PERMISSION_INDEX, query=delete_query
        )

    for k, v in mo_ids_grouped_by_permissions.items():
        # search_query = {"terms": {"id": v}}

        search_query = {
            "bool": {
                "must": [
                    {"terms": {"id": v}},
                    {"exists": {"field": INVENTORY_PERMISSIONS_FIELD_NAME}},
                    {"term": {INVENTORY_PERMISSIONS_FIELD_NAME: k}},
                ]
            }
        }

        update_script = {
            "source": "def permissions=ctx._source.%(field_name)s;"
            "if (permissions instanceof ArrayList)"
            "{ctx._source.%(field_name)s.removeIf(item -> item == params['perm_name']);"
            "int size = ctx._source.%(field_name)s.size();"
            "if (size == 1)"
            "{ctx._source.%(field_name)s = ctx._source.%(field_name)s[0]}"
            "}"
            "else if (permissions instanceof String)"
            "{ctx._source.%(field_name)s = params['str_default_value']}"
            % {"field_name": INVENTORY_PERMISSIONS_FIELD_NAME},
            "lang": "painless",
            "params": {"perm_name": k, "str_default_value": None},
        }

        await async_client.update_by_query(
            index=ALL_MO_OBJ_INDEXES_PATTERN,
            query=search_query,
            script=update_script,
            refresh=True,
        )
