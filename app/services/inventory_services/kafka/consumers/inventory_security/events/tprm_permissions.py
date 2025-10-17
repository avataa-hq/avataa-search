from collections import defaultdict
from typing import List

from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk, BulkIndexError

from elastic.config import INVENTORY_TPRM_INDEX_V2
from indexes_mapping.inventory.mapping import INVENTORY_PERMISSIONS_FIELD_NAME
from services.inventory_services.elastic.security.configs import (
    INVENTORY_SECURITY_TPRM_PERMISSION_INDEX,
)


async def with_tprm_permissions_create(
    msg: List[dict], async_client: AsyncElasticsearch
):
    """Kafka event handler for TPRM permission crete event"""
    actions = list()

    tprm_ids_grouped_by_permissions = defaultdict(list)

    for item in msg:
        item_id = item.get("id")
        tprm_id = item.get("parent_id")
        permission = item.get("permission")
        read = item.get("read")
        if item_id:
            action_item = dict(
                _index=INVENTORY_SECURITY_TPRM_PERMISSION_INDEX,
                _op_type="index",
                _id=item_id,
                _source=item,
            )
            actions.append(action_item)

            if all([read, tprm_id, permission]):
                tprm_ids_grouped_by_permissions[permission].append(tprm_id)

    if actions:
        try:
            await async_bulk(
                client=async_client, refresh="true", actions=actions
            )
        except BulkIndexError as e:
            print(e.errors)
            raise e

    for k, v in tprm_ids_grouped_by_permissions.items():
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
            index=INVENTORY_TPRM_INDEX_V2,
            query=search_query,
            script=update_script,
            refresh=True,
        )


async def with_tprm_permissions_update(
    msg: List[dict], async_client: AsyncElasticsearch
):
    """Kafka event handler for TPRM permission update event"""
    actions = list()

    tprm_ids_grouped_by_permissions_read_true = defaultdict(list)
    tprm_ids_grouped_by_permissions_read_false = defaultdict(list)

    for item in msg:
        item_id = item.get("id")
        tprm_id = item.get("parent_id")
        permission = item.get("permission")
        read = item.get("read")
        if item_id:
            action_item = dict(
                _index=INVENTORY_SECURITY_TPRM_PERMISSION_INDEX,
                _op_type="index",
                _id=item_id,
                _source=item,
            )
            actions.append(action_item)

            if all([tprm_id, permission]):
                if read:
                    tprm_ids_grouped_by_permissions_read_true[
                        permission
                    ].append(tprm_id)
                else:
                    tprm_ids_grouped_by_permissions_read_false[
                        permission
                    ].append(tprm_id)

    if actions:
        try:
            await async_bulk(
                client=async_client, refresh="true", actions=actions
            )
        except BulkIndexError as e:
            print(e.errors)
            raise e

    for k, v in tprm_ids_grouped_by_permissions_read_true.items():
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
            index=INVENTORY_TPRM_INDEX_V2,
            query=search_query,
            script=update_script,
            refresh=True,
        )

    for k, v in tprm_ids_grouped_by_permissions_read_false.items():
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
            index=INVENTORY_TPRM_INDEX_V2,
            query=search_query,
            script=update_script,
            refresh=True,
        )


async def with_tprm_permissions_delete(
    msg: List[dict], async_client: AsyncElasticsearch
):
    """Kafka event handler for TPRM permission delete event"""

    ids_to_delete = list()

    tprm_ids_grouped_by_permissions = defaultdict(list)

    for item in msg:
        item_id = item.get("id")
        tprm_id = item.get("parent_id")
        permission = item.get("permission")
        read = item.get("read")
        if item_id:
            ids_to_delete.append(item_id)

            if all([read, tprm_id, permission]):
                tprm_ids_grouped_by_permissions[permission].append(tprm_id)

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
            index=INVENTORY_SECURITY_TPRM_PERMISSION_INDEX, query=delete_query
        )

    for k, v in tprm_ids_grouped_by_permissions.items():
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
            index=INVENTORY_TPRM_INDEX_V2,
            query=search_query,
            script=update_script,
            refresh=True,
        )
