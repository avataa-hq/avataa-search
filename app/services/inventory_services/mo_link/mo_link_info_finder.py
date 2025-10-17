from collections import defaultdict
from logging import getLogger
from typing import Iterable

from elasticsearch import AsyncElasticsearch

from services.inventory_services.mo_link.common import async_timing_decorator
from services.inventory_services.utils.security.filter_by_realm import (
    get_only_available_to_read_tmo_ids_for_special_client,
)
from utils_by_services.inventory.mo_features import (
    get_info_mos,
    get_mo_link_data_by_mo_ids,
    get_mo_link_data_by_value,
    get_info_about_tmos,
)
from utils_by_services.inventory.tprm_features import get_list_of_mo_link_tprm
from v2.routers.inventory.utils.dto import DTOMOInfo, DTOTPRMInfo, DTOMOLinkInfo
from v2.routers.inventory.utils.models import (
    MOLinkOutInfoResponse,
    MOLinkOutWithTPRMInfo,
    MOLinkOut,
    MOInfo,
    MOLinkInfoResponse,
    TMOInfo,
    MOLinkInfo,
    MOLinkInListInfoResponse,
    MOLinkInInfoResponse,
)


class MOLinkInfoFinder(object):
    _enable_timing = True

    def __init__(self, elastic_client: AsyncElasticsearch):
        self.elastic_client = elastic_client
        self.logger = getLogger("MO Link Info Finder")

    @async_timing_decorator(enable=_enable_timing)
    async def get_out_mo_link(
        self,
        mo_ids: list[int],
        is_admin: bool,
        user_permissions: list[str],
    ) -> MOLinkOutInfoResponse:
        try:
            # Get info about root MOs to correct TPRM search
            tmo_data = await self._get_info_about_tmos_with_mos(
                mo_ids=mo_ids,
                is_admin=is_admin,
                user_permissions=user_permissions,
            )
            unique_tmo_ids = set(tmo_data.keys())

            if not unique_tmo_ids:
                self.logger.info("Can't find unique tmo id.")
                return MOLinkOutInfoResponse(out_mo_link_info=[])

            # Get info about TPRM name
            tprm_info: dict = await self._get_info_about_tprms(
                tmo_ids=unique_tmo_ids,
                is_admin=is_admin,
                user_permissions=user_permissions,
            )

            # Get info about mo_link
            tprm_ids = [k for el in tprm_info.values() for k in el]
            (
                tprm_id_out_mo_id_connection,
                root_mo_id_out_mo_id_connection,
            ) = await self._get_info_about_out_mo_link(
                mo_ids=mo_ids,
                tprm_ids=tprm_ids,
                is_admin=is_admin,
                user_permissions=user_permissions,
            )
            if not tprm_id_out_mo_id_connection:
                self.logger.info("Can't find info about mo links.")
                return MOLinkOutInfoResponse(out_mo_link_info=[])

            # Get info about out linked mo
            out_mo_data = await self._get_info_about_tmos_with_mos(
                mo_ids=[
                    mo_id
                    for v in tprm_id_out_mo_id_connection.values()
                    for mo_id in v
                ],
                is_admin=is_admin,
                user_permissions=user_permissions,
            )

            # Generate response from data
            total_tprm_mo_info = MOLinkInfoFinder._generate_mo_info(
                out_mo_data=out_mo_data,
                tprm_ids=tprm_ids,
                tprm_id_out_mo_id_conn=tprm_id_out_mo_id_connection,
            )

            mo_link_out: list[MOLinkOut] = (
                MOLinkInfoFinder._generate_mo_link_out_info(
                    tmo_data=tmo_data,
                    tprm_info=tprm_info,
                    tprm_mo_info=total_tprm_mo_info,
                    root_mo_out_mo_relation=root_mo_id_out_mo_id_connection,
                )
            )
            result = MOLinkOutInfoResponse(out_mo_link_info=mo_link_out)
            return result
        except KeyError as ex:
            self.logger.exception(ex)
            raise ValueError(f"Incorrect data for mo ids: {mo_ids}")

    @async_timing_decorator(enable=_enable_timing)
    async def get_in_mo_link(
        self,
        mo_id: int,
        is_admin: bool,
        user_permissions: list[str],
    ) -> MOLinkInfoResponse:
        # get available mo_links
        # Get info about mo_link in_mo_id, in_tprm_id, in_value
        (
            root_mo_in_mo_data,
            existed_tprm,
            _,
            in_tmo_mo_data,
            in_tmo_data,
            tprm_info,
        ) = await self._gather_info_about_in_mo_link(
            mo_ids=[mo_id], is_admin=is_admin, user_permissions=user_permissions
        )
        # get linked mos
        mo_link_data: list[MOLinkInfo] = []
        additional_info: list[TMOInfo] = []
        for tmo_id, tmo_name in in_tmo_data.items():  # type: int, dict
            # get additional data tmo_id, tmo_name, tprm_id
            additional_info.append(
                TMOInfo(
                    tmo_id=tmo_id,
                    tmo_name=tmo_name,
                    tprm_id=[
                        tprm_id
                        for tprm_id in tprm_info[tmo_id]
                        if tprm_id in existed_tprm[mo_id]
                    ],
                )
            )
            for tprm_id in tprm_info[tmo_id]:  # type: dict
                for mo_data in in_tmo_mo_data[tmo_id]:  # type: DTOMOInfo
                    for mo_link_info in root_mo_in_mo_data[mo_id]:  # type: DTOMOLinkInfo
                        if (
                            mo_link_info.tprm_id == tprm_id
                            and mo_data.id == mo_link_info.mo_id
                        ):
                            mo_link_data.append(
                                MOLinkInfo(
                                    mo_id=mo_data.id,
                                    mo_name=mo_data.name,
                                    parent_mo_name=mo_data.parent_name,
                                    parent_mo_id=mo_data.p_id,
                                    tmo_id=tmo_id,
                                    tmo_name=tmo_name,
                                    tprm_id=tprm_info[tmo_id][tprm_id].id,
                                    tprm_name=tprm_info[tmo_id][tprm_id].name,
                                    multiple=tprm_info[tmo_id][
                                        tprm_id
                                    ].multiple,
                                    value=mo_link_info.value,
                                )
                            )
        response = MOLinkInfoResponse(
            mo_link_info=mo_link_data,
            additional_info=additional_info,
            total=sum([len(el) for el in root_mo_in_mo_data.values()]),
        )
        return response

    @async_timing_decorator(enable=_enable_timing)
    async def get_in_list_mo_link(
        self,
        mo_ids: list[int],
        is_admin: bool,
        user_permissions: list[str],
    ) -> MOLinkInListInfoResponse:
        (
            root_mo_in_mo_data,
            root_mo_tprm_relation,
            _,
            in_tmo_mo_data,
            in_tmo_data,
            tprm_info,
        ) = await self._gather_info_about_in_mo_link(
            mo_ids=mo_ids, is_admin=is_admin, user_permissions=user_permissions
        )

        list_info = list()
        for root_mo_id in mo_ids:
            mo_link_data: list[MOLinkInfo] = []
            additional_info: list[TMOInfo] = []
            for tmo_id, tmo_name in in_tmo_data.items():  # type: int, dict
                # get additional data tmo_id, tmo_name, tprm_id
                set_suitable_tprm_for_root_mo: set = root_mo_tprm_relation[
                    root_mo_id
                ]
                # for current_tprm
                list_tprm_from_tmo = {tprm_id for tprm_id in tprm_info[tmo_id]}
                for current_tprm_in in set_suitable_tprm_for_root_mo:
                    if current_tprm_in in list_tprm_from_tmo:
                        additional_info.append(
                            TMOInfo(
                                tmo_id=tmo_id,
                                tmo_name=tmo_name,
                                tprm_id=[
                                    tprm_id for tprm_id in tprm_info[tmo_id]
                                ],
                            )
                        )
                        for mo_data in in_tmo_mo_data[tmo_id]:  # type: DTOMOInfo
                            for mo_link_info in root_mo_in_mo_data[root_mo_id]:  # type: DTOMOLinkInfo
                                if (
                                    mo_link_info.tprm_id == current_tprm_in
                                    and mo_data.id == mo_link_info.mo_id
                                ):
                                    mo_link_data.append(
                                        MOLinkInfo(
                                            mo_id=mo_data.id,
                                            mo_name=mo_data.name,
                                            parent_mo_name=mo_data.parent_name,
                                            parent_mo_id=mo_data.p_id,
                                            tmo_id=tmo_id,
                                            tmo_name=tmo_name,
                                            tprm_id=tprm_info[tmo_id][
                                                current_tprm_in
                                            ].id,
                                            tprm_name=tprm_info[tmo_id][
                                                current_tprm_in
                                            ].name,
                                            multiple=tprm_info[tmo_id][
                                                current_tprm_in
                                            ].multiple,
                                            value=mo_link_info.value,
                                        )
                                    )
            response = MOLinkInfoResponse(
                mo_link_info=mo_link_data,
                additional_info=additional_info,
                total=len(root_mo_in_mo_data[root_mo_id]),
            )
            mo_link_in_info = MOLinkInInfoResponse(
                mo_link_info_response=response, mo_id=root_mo_id
            )
            list_info.append(mo_link_in_info)
        output = MOLinkInListInfoResponse(list_info=list_info)
        return output

    async def _gather_info_about_in_mo_link(
        self,
        mo_ids: list[int],
        is_admin: bool,
        user_permissions: list[str],
    ) -> tuple[dict, dict, dict, dict, dict, dict]:
        (
            root_mo_in_mo_data,
            root_mo_tprm_relation,
            root_mo_in_mo_relation,
        ) = await self._get_info_about_in_mo_link(
            mo_ids=mo_ids,
            tprm_ids=[],
            is_admin=is_admin,
            user_permissions=user_permissions,
        )
        # Get info about TMO for objects linked to our mo
        in_mo_ids = [
            mo_link_info.mo_id
            for lst in root_mo_in_mo_data.values()
            for mo_link_info in lst
        ]
        in_tmo_mo_data = await self._get_info_about_tmos_with_mos(
            mo_ids=in_mo_ids,
            is_admin=is_admin,
            user_permissions=user_permissions,
        )
        # Additional information about TMO
        in_tmo_data = await self._get_info_about_tmo(
            tmo_ids=in_tmo_mo_data.keys(),
            user_permissions=user_permissions,
            is_admin=is_admin,
        )
        # Get info about TPRM for objects linked to our mo
        tprm_info: dict = await self._get_info_about_tprms(
            tmo_ids=set(in_tmo_mo_data.keys()),
            is_admin=is_admin,
            user_permissions=user_permissions,
        )
        return (
            root_mo_in_mo_data,
            root_mo_tprm_relation,
            root_mo_in_mo_relation,
            in_tmo_mo_data,
            in_tmo_data,
            tprm_info,
        )

    async def _get_info_about_tmo(
        self,
        tmo_ids: Iterable,
        is_admin: bool,
        user_permissions: list[str],
    ) -> dict:
        """return: {tmo_id: tmo_name}"""
        output = dict()
        returned_columns = ["id", "name"]
        tmos_data = await get_info_about_tmos(
            elastic_client=self.elastic_client,
            is_admin=is_admin,
            user_permissions=user_permissions,
            tmo_ids=list(tmo_ids),
            returned_columns=returned_columns,
        )
        for tmo in tmos_data:  # type: dict
            data = tmo["_source"]
            output[data["id"]] = data["name"]
        return output

    async def _get_info_about_tmos_with_mos(
        self,
        mo_ids: list[int],
        is_admin: bool,
        user_permissions: list[str],
    ) -> dict[int, list[int]]:
        """
        return: dict(tmo_id: list[int])
        """
        _, tmo_mo_data = await self._get_info_about_mos(
            is_admin=is_admin, user_permissions=user_permissions, mo_ids=mo_ids
        )
        unique_tmos = set(tmo_mo_data.keys())

        if is_admin:
            return tmo_mo_data

        # Check user permission for root TMOs
        available_tmos = (
            await get_only_available_to_read_tmo_ids_for_special_client(
                client_permissions=user_permissions,
                elastic_client=self.elastic_client,
                tmo_ids=list(unique_tmos),
            )
        )
        if set(available_tmos) == unique_tmos:
            return tmo_mo_data
        # In case we got difference between available TMO and TMO for root MO
        unique_tmos.intersection_update(available_tmos)
        tmo_mo_dict_updated = dict()
        for tmo_id in unique_tmos:
            if tmo_mo_data[tmo_id]:
                tmo_mo_dict_updated[tmo_id] = tmo_mo_data[tmo_id]
        return tmo_mo_dict_updated

    async def _get_info_about_tprms(
        self,
        tmo_ids: set[int],
        is_admin: bool,
        user_permissions: list[str],
    ) -> dict:
        """return: {tmo_id: {tprm_id: DTO tprm Info},}"""
        tprm_id_to_tprm_name: dict[int, dict[int, DTOTPRMInfo]] = dict()
        returned_columns = ["id", "name", "tmo_id", "multiple"]
        root_mo_tprms_mo_link = await get_list_of_mo_link_tprm(
            elastic_client=self.elastic_client,
            is_admin=is_admin,
            user_permissions=user_permissions,
            tmo_ids=list(tmo_ids),
            returned_columns=returned_columns,
            sorting={"id": {"order": "asc"}},
        )
        for tprm in root_mo_tprms_mo_link:  # type: dict
            data = DTOTPRMInfo(**tprm["_source"])
            tprm_id_to_tprm_name.setdefault(data.tmo_id, {})
            tprm_id_to_tprm_name[data.tmo_id][data.id] = data
        return tprm_id_to_tprm_name

    async def _get_info_about_out_mo_link(
        self,
        mo_ids: list[int],
        tprm_ids: list[int],
        is_admin: bool,
        user_permissions: list[str],
    ) -> tuple[dict, dict]:
        """
        Collect information about connection tprm -> out_mo_id and mo_id -> out_mo_id
        mo_ids: Search for values for these mo ids
        tprm_ids: Search for values for these tprm ids
        return: {tprm_id: {out_mo_id, out_mo_id}}, {mo_id: {out_mo_id, out_mo_id}}
        """
        returned_columns = ["mo_id", "tprm_id", "value"]
        tprm_id_out_mo_id_connection: dict = dict()
        root_mo_id_out_mo_id_connection: dict = dict()
        # Add Check prm permissions for user executed get_info_mos in _
        # root_mo_id_root_prm_id_connection: dict = dict()
        # returned_columns = ["id", "mo_id", "tprm_id", "value"]
        mo_links = await get_mo_link_data_by_mo_ids(
            elastic_client=self.elastic_client,
            mo_ids=mo_ids,
            tprm_ids=tprm_ids,
            is_admin=is_admin,
            user_permissions=user_permissions,
            returned_columns=returned_columns,
            sorting={"id": {"order": "asc"}},
        )
        for mo_link in mo_links:  # type: dict
            root_mo_id = mo_link["_source"]["mo_id"]
            # root_prm_id =  mo_link["_source"]["id"]
            tprm_id = mo_link["_source"]["tprm_id"]
            out_mo_id = mo_link["_source"]["value"]
            # root_mo_id_root_prm_id_connection.setdefault(root_mo_id, list()).extend(root_prm_id)
            if isinstance(out_mo_id, list):
                tprm_id_out_mo_id_connection.setdefault(tprm_id, set()).update(
                    out_mo_id
                )
                root_mo_id_out_mo_id_connection.setdefault(
                    root_mo_id, set()
                ).update(out_mo_id)
            else:
                tprm_id_out_mo_id_connection.setdefault(tprm_id, set()).add(
                    out_mo_id
                )
                root_mo_id_out_mo_id_connection.setdefault(
                    root_mo_id, set()
                ).add(out_mo_id)

        return tprm_id_out_mo_id_connection, root_mo_id_out_mo_id_connection

    async def _get_info_about_in_mo_link(
        self,
        mo_ids: list[int],
        tprm_ids: list[int],
        is_admin: bool,
        user_permissions: list[str],
    ) -> tuple[dict, dict, dict]:
        """
        return:
        {input mo_id: [MO link info, ]}
        {input mo_id: [in mo ids,]}
        {input mo_id: [in tprm_ids,]}
        """
        # Value for correct work with multiple TPRM
        returned_columns = ["mo_id", "tprm_id", "value"]
        root_mo_in_mo_data = defaultdict(list)
        # Add relation besides root mo and in tprm id
        root_mo_tprm_relation = defaultdict(set)
        # Add relation besides root mo and in mo id
        root_mo_in_mo_relation = defaultdict(list)
        in_mo_links = await get_mo_link_data_by_value(
            elastic_client=self.elastic_client,
            mo_ids=mo_ids,
            tprm_ids=tprm_ids,
            is_admin=is_admin,
            user_permissions=user_permissions,
            returned_columns=returned_columns,
            sorting={"id": {"order": "asc"}},
        )

        for mo_link in in_mo_links:
            data: dict = mo_link.get("_source")
            if data:
                if isinstance(data.get("value"), list):
                    temp_dto_mo_in_info = DTOMOLinkInfo(**data)
                elif isinstance(data.get("value"), int):
                    temp_dto_mo_in_info = DTOMOLinkInfo(
                        mo_id=data.get("mo_id"),
                        tprm_id=data.get("tprm_id"),
                        value=[data.get("value")],
                    )
                else:
                    raise ValueError(
                        f"Incorrect PRM value {data.get('value')} for mo link."
                    )
                for root_mo_id in temp_dto_mo_in_info.value:
                    if root_mo_id in mo_ids:
                        root_mo_in_mo_data[root_mo_id].append(
                            temp_dto_mo_in_info
                        )
                        root_mo_tprm_relation[root_mo_id].add(
                            temp_dto_mo_in_info.tprm_id
                        )
                        root_mo_in_mo_relation[root_mo_id].append(
                            temp_dto_mo_in_info.mo_id
                        )
        return root_mo_in_mo_data, root_mo_tprm_relation, root_mo_in_mo_relation

    async def _get_info_about_mos(
        self, is_admin: bool, user_permissions: list[str], mo_ids: list[int]
    ) -> tuple[dict, dict]:
        """return: {mo_id: DTOMOInfo}, {TMO: [DTOMOInfo]}"""
        output_mo = dict()
        output_tmo = dict()
        returned_columns = ["id", "parent_name", "p_id", "name", "tmo_id"]
        result = await get_info_mos(
            elastic_client=self.elastic_client,
            is_admin=is_admin,
            user_permissions=user_permissions,
            mo_ids=mo_ids,
            returned_columns=returned_columns,
            sorting={"id": {"order": "asc"}},
        )

        for mo_data in result:
            data = DTOMOInfo(**mo_data["_source"])
            output_mo[data.id] = data
            output_tmo.setdefault(data.tmo_id, []).append(data)
        return output_mo, output_tmo

    @staticmethod
    def _generate_mo_info(
        out_mo_data: dict, tprm_ids: list[int], tprm_id_out_mo_id_conn: dict
    ) -> dict:
        result = dict()
        for out_tmo_id, list_out_mo_data in out_mo_data.items():  # type: int, list
            for tprm_id in tprm_ids:
                for current_out_mo in list_out_mo_data:  # type: DTOMOInfo
                    if current_out_mo.id in tprm_id_out_mo_id_conn.get(
                        tprm_id, []
                    ):
                        cur_mo_info = MOInfo(**current_out_mo.__dict__)
                        result.setdefault(tprm_id, []).append(cur_mo_info)
                    else:
                        break
        return result

    @staticmethod
    def _generate_mo_link_out_info(
        tmo_data: dict,
        tprm_info: dict,
        tprm_mo_info: dict,
        root_mo_out_mo_relation: dict[int, set[int]],
    ) -> list[MOLinkOut]:
        mo_link_out: list[MOLinkOut] = list()
        # We must exclude_unused_tprm in tprm_info
        cleaned_tprm_info = dict()
        for tmo_id in tmo_data.keys():
            for tprm_id in tprm_mo_info.keys():
                cleaned_tprm_info.setdefault(tmo_id, []).append(
                    tprm_info[tmo_id][tprm_id]
                )

        # We must generate response only for TMOs who belongs cleaned TPRMs
        for root_tmo_id, all_tprm_data in cleaned_tprm_info.items():  # type: int, list[DTOTPRMInfo]
            for current_root_mo in tmo_data[root_tmo_id]:
                mo_out_data: list[MOLinkOutWithTPRMInfo] = list()
                if current_root_mo.id in root_mo_out_mo_relation.keys():
                    for tprm_data in all_tprm_data:  # type: DTOTPRMInfo
                        for cur_mo_out_info in tprm_mo_info[tprm_data.id]:  # type: MOInfo
                            if (
                                cur_mo_out_info.mo_id
                                in root_mo_out_mo_relation.get(
                                    current_root_mo.id, []
                                )
                            ):
                                current_mo_out_data: MOLinkOutWithTPRMInfo = (
                                    MOLinkOutWithTPRMInfo(
                                        tprm_id=tprm_data.id,
                                        tprm_name=tprm_data.name,
                                        multiple=tprm_data.multiple,
                                        mo_out_info=cur_mo_out_info,
                                    )
                                )
                                mo_out_data.append(current_mo_out_data)
                current_mo_link_out = MOLinkOut(
                    mo_id=current_root_mo.id, mo_out_data=mo_out_data
                )
                mo_link_out.append(current_mo_link_out)
        return mo_link_out
