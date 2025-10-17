from typing import List

from elasticsearch import AsyncElasticsearch
from pydantic import BaseModel

from services.hierarchy_services.elastic.configs import HIERARCHY_LEVELS_INDEX
from services.hierarchy_services.models.dto import LevelDTO


class DependentLevelsChain(BaseModel):
    upper_levels_ordered_by_depth_asc: List[LevelDTO]
    current_level: LevelDTO | None = None
    lower_levels_ordered_by_depth_asc: List[LevelDTO]


class LevelDataForChain(BaseModel):
    level_id: int
    level_level: int


class DependentLevelsChainFactory:
    def __init__(
        self,
        elastic_client: AsyncElasticsearch,
        hierarchy_id: int | str,
        parent_level_data: LevelDataForChain | None = None,
    ):
        self.elastic_client = elastic_client
        self.hierarchy_id = str(hierarchy_id)
        self.parent_level_data = parent_level_data

        # levels data
        self.__upper_levels_ordered_by_depth = list()
        self.__current_level = None
        self.__lower_levels_ordered_by_depth = list()

    async def __get_upper_levels_ordered_by_depth_and_current_level(self):
        """Fills in self.__upper_levels_ordered_by_depth and self.__current_level data"""
        if not self.parent_level_data:
            return

        size_per_step = 100_000
        search_body = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"hierarchy_id": self.hierarchy_id}},
                        {"exists": {"field": "level"}},
                        {
                            "range": {
                                "level": {
                                    "lte": self.parent_level_data.level_level
                                }
                            }
                        },
                    ]
                }
            },
            "sort": {"level": {"order": "desc"}},
            "track_total_hits": True,
            "size": size_per_step,
        }

        level_id = str(self.parent_level_data.level_id)
        res_list = list()

        while True:
            search_res = await self.elastic_client.search(
                index=HIERARCHY_LEVELS_INDEX,
                body=search_body,
                ignore_unavailable=True,
            )

            total_hits = search_res["hits"]["total"]["value"]

            search_res = search_res["hits"]["hits"]

            for item in search_res:
                item = item["_source"]
                if level_id == item["id"]:
                    level_dto = LevelDTO.model_validate(item)
                    res_list.insert(0, level_dto)

                    parent_id = item.get("parent_id")
                    if not parent_id:
                        break
                    level_id = parent_id

            if total_hits < size_per_step:
                break

            if len(search_res) == size_per_step:
                search_after = search_res[-1]["sort"]
                search_body["search_after"] = search_after
            else:
                break

        if res_list:
            self.__current_level = res_list.pop()
            self.__upper_levels_ordered_by_depth = res_list

    async def __get_lower_levels_ordered_by_depth_if_level_data(self):
        """Fills in self.__lower_levels_ordered_by_depth data if exists self.level_data"""
        size_per_step = 100_000

        parent_level_level = self.parent_level_data.level_level

        search_body = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"hierarchy_id": self.hierarchy_id}},
                        {"exists": {"field": "level"}},
                        {"range": {"level": {"gt": parent_level_level}}},
                    ]
                }
            },
            "sort": {"level": {"order": "asc"}},
            "track_total_hits": True,
            "size": size_per_step,
        }

        res_list = list()
        p_ids = {str(self.parent_level_data.level_id)}

        while True:
            search_res = await self.elastic_client.search(
                index=HIERARCHY_LEVELS_INDEX,
                body=search_body,
                ignore_unavailable=True,
            )

            total_hits = search_res["hits"]["total"]["value"]

            search_res = search_res["hits"]["hits"]

            for item in search_res:
                item = item["_source"]
                parent_id = item.get("parent_id")
                item_id = item.get("id")

                if parent_id in p_ids:
                    level_dto = LevelDTO.model_validate(item)
                    res_list.append(level_dto)
                    p_ids.add(item_id)

            if total_hits < size_per_step:
                break

            if len(search_res) == size_per_step:
                search_after = search_res[-1]["sort"]
                search_body["search_after"] = search_after
            else:
                break

        if res_list:
            self.__lower_levels_ordered_by_depth = res_list

    async def __get_lower_levels_ordered_by_depth_if_not_level_data(self):
        """Fills in self.__lower_levels_ordered_by_depth data if not exists self.level_data"""
        size_per_step = 100_000

        search_body = {
            "query": {
                "bool": {
                    "must": [{"match": {"hierarchy_id": self.hierarchy_id}}]
                }
            },
            "sort": {"level": {"order": "asc"}},
            "track_total_hits": True,
            "size": size_per_step,
        }

        res_list = list()

        while True:
            search_res = await self.elastic_client.search(
                index=HIERARCHY_LEVELS_INDEX,
                body=search_body,
                ignore_unavailable=True,
            )

            total_hits = search_res["hits"]["total"]["value"]

            search_res = search_res["hits"]["hits"]

            step_res = [
                LevelDTO.model_validate(item["_source"]) for item in search_res
            ]
            if res_list:
                res_list.extend(step_res)
            else:
                res_list = step_res

            if total_hits < size_per_step:
                break

            if len(search_res) == size_per_step:
                search_after = search_res[-1]["sort"]
                search_body["search_after"] = search_after
            else:
                break

        if res_list:
            self.__lower_levels_ordered_by_depth = res_list

    async def create_dependent_levels_chain_by(self) -> DependentLevelsChain:
        """Returns instance of DependentLevelsChain"""
        await self.__get_upper_levels_ordered_by_depth_and_current_level()
        if self.parent_level_data:
            await self.__get_lower_levels_ordered_by_depth_if_level_data()
        else:
            await self.__get_lower_levels_ordered_by_depth_if_not_level_data()

        return DependentLevelsChain(
            upper_levels_ordered_by_depth_asc=self.__upper_levels_ordered_by_depth,
            current_level=self.__current_level,
            lower_levels_ordered_by_depth_asc=self.__lower_levels_ordered_by_depth,
        )
