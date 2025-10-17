# from fastapi import HTTPException
#
# from elastic.client import client
# from exceptions import TMONotFoundError
# from elastic.config import (
#     PARAMS_INDEX,
#     INVENTORY_INDEX,
#     TMO_INDEX,
#     HIERARCHY_INDEX,
# )
# from security.security_config import ADMIN_ROLE
# from security.security_data_models import UserData
# from utils import (
#     is_complicated,
#     create_query,
#     get_parent_id,
#     clear_output,
#     define_conditions,
#     define_combine_rule,
#     get_match_score,
#     check_permissions,
#     check_conditional_permissions,
#     get_objects_id_by_permissions,
# )
#
#
# def search_function(
#     value: dict, locale: dict, user_data: UserData, tmo: str = None
# ) -> list[dict]:
#     """
#     Gateway function that decides what exactly search is needed and applies it
#     :param value: all search params such as actual value and conditions for conditional search
#     :param locale: defines what locale is used: en, de, etc.
#     :param tmo: name of tmo if it's defined beforehand
#     :return: objects that were found or empty list
#     """
#     if value["conditions"]:
#         result = search_with_conditions(
#             value["conditions"], locale, tmo=tmo, user_data=user_data
#         )
#         result = clear_output(data=result, conditional=True)
#     else:
#         value = " ".join(value["value"])
#         result = default_search(value, tmo=tmo, user_data=user_data)
#         result = clear_output(data=result, value=value.strip())
#
#     return check_permissions(result, user_data)
#
#
# def default_search(
#     value: str, user_data: UserData, tmo: str = None
# ) -> list[dict]:
#     """
#     Function for default search - search by value
#     :param value: keywords for search
#     :param tmo: tmo name that defined beforehand
#     :return: objects that were found by query or empty list
#     """
#     if value.strip() == "":
#         raise HTTPException(status_code=422, detail="Value not provided!")
#     query = [{"match": {"searchable": True}}]
#
#     # search TMO
#     if tmo:
#         tmo = search_tmo(tmo, user_data)
#         query.extend(tmo)
#
#     params = search_params(query, user_data)
#
#     params_by_id = [
#         {"match": {"params.tprm_id": param}} for param in params.keys()
#     ]
#
#     parsed_value = is_complicated(value)
#     if parsed_value["separator"]:
#         query_string = " or ".join(
#             [f"*{part}*".lower() for part in parsed_value["value"]]
#         )
#     else:
#         query_string = f"*{parsed_value['value']}*"
#
#     params_by_name = [
#         {"query_string": {"default_field": param, "query": query_string}}
#         for param in params.values()
#     ]
#     query = create_query(query_string, params_by_id, params_by_name, tmo)
#
#     parent_response = client.search(
#         index=INVENTORY_INDEX, query=query, size=20
#     )["hits"]["hits"]
#     parents_id = get_parent_id(parsed_value, parent_response)
#     if len(parents_id) > 0:
#         parent_response.extend(search_child(parents_id))
#
#     return parent_response
#
#
# def search_with_conditions(
#     value: str, locale: dict, user_data: UserData, tmo: str = None
# ) -> list[dict]:
#     """
#     Function that performs search with special conditions like =,<,> etc.
#     :param value: conditions to search with
#     :param locale: what locale must be used to define conditions interaction (or, and)
#     :param tmo: tmo name that defined beforehand
#     :return: list of objects that wa found or empty list
#     """
#     if tmo:
#         tmo = search_tmo(tmo, user_data)
#
#     combine_rule, value = define_combine_rule(value, locale)
#
#     if ADMIN_ROLE not in user_data.realm_access.roles:
#         value = check_conditional_permissions(value, user_data)
#
#     if len(value) == 0:
#         return []
#
#     conditions = define_conditions(value)
#     conditions.append(tmo[0])
#     query = {"bool": {combine_rule: conditions}}
#
#     parent_response = client.search(index=INVENTORY_INDEX, query=query)["hits"][
#         "hits"
#     ]
#     for parent in parent_response:
#         parent["_priority"] = 2
#
#     parents_id = [obj["_source"]["id"] for obj in parent_response]
#
#     if len(parents_id) > 0:
#         parent_response.extend(search_child(parents_id))
#
#     return parent_response
#
#
# def search_tmo(tmo: str, user_data: UserData):
#     """
#     Search tmo objects
#     :param tmo: name of tmo
#     :return: parsed to query format list of found tmo
#     """
#     if ADMIN_ROLE not in user_data.realm_access.roles:
#         tmos = get_objects_id_by_permissions(user_data, "tmo")
#         query = {
#             "bool": {
#                 "must": {"match": {"name": tmo}},
#                 "should": tmos,
#                 "minimum_should_match": 1,
#             }
#         }
#     else:
#         query = {"match": {"name": tmo}}
#
#     tmo = client.search(index=TMO_INDEX, query=query)["hits"]["hits"]
#     tmo = [{"match": {"tmo_id": tmo_item["_source"]["id"]}} for tmo_item in tmo]
#
#     if len(tmo) == 0:
#         raise TMONotFoundError
#
#     return tmo
#
#
# def search_child(parents_id: list[int]) -> list[dict]:
#     """
#     Search child objects of given parents
#     :param parents_id: list with id of parent objects
#     :return: list with child objects
#     """
#     parent_to_search = [{"match": {"p_id": p_id}} for p_id in parents_id]
#     result = client.search(
#         index=INVENTORY_INDEX,
#         query={"bool": {"should": parent_to_search, "minimum_should_match": 1}},
#     )["hits"]["hits"]
#
#     for res in result:
#         res["_priority"] = 1
#
#     return result
#
#
# def search_params(query: list[dict], user_data: UserData) -> dict:
#     """
#     Search for params that has property 'searchable'=true
#     :param query: completed query with 'searchable' criteria and list of tmo
#     :return: all parsed searchable parameters
#     """
#     if ADMIN_ROLE not in user_data.realm_access.roles:
#         tprms = get_objects_id_by_permissions(user_data, "tprm")
#         query = {
#             "bool": {"must": query, "should": tprms, "minimum_should_match": 1}
#         }
#     else:
#         query = {"bool": {"must": query}}
#     response = client.search(
#         index=PARAMS_INDEX, body={"_source": ["id", "name"], "query": query}
#     )
#     return {
#         res["_source"]["id"]: res["_source"]["name"]
#         for res in response["hits"]["hits"]
#     }
#
#
# def search_hierarchy(hierarchy: str) -> tuple[list[int], str]:
#     query = {"match": {"hierarchy_name": hierarchy}}
#     response = client.search(index=HIERARCHY_INDEX, query=query)["hits"]["hits"]
#     tmo_ids = []
#     hierarchy_name = None
#     for res in response:
#         if (
#             get_match_score(res["_source"]["hierarchy_name"].lower(), hierarchy)
#             > 0.8
#         ):
#             tmo_ids.append(res["_source"]["object_type_id"])
#             hierarchy_name = res["_source"]["hierarchy_name"]
#
#     return tmo_ids, hierarchy_name
