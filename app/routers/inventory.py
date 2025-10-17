# from typing import Annotated
#
# from fastapi import APIRouter, Query, HTTPException, Depends
#
# from exceptions import TMONotFoundError
#
# from localization import get_locale
# from search_functions import search_function, search_hierarchy
# from security.security_data_models import UserData
# from security.security_factory import security
#
# from utils import define_action
#
#
# router = APIRouter(prefix="/inventory")
#
#
# @router.get("/search/", description="Search inventory object by some value")
# def search(
#     value: Annotated[str, Query(min_length=1)],
#     locale: dict = Depends(get_locale),
#     user_data: UserData = Depends(security),
# ):
#     value = value.strip().lower().split(" ")
#     value = define_action(value, locale)
#
#     # attempt to extract TMO name from query
#     if len(value["value"]) > 1 and not value["tmo"]:
#         value["tmo"] = value["value"][0]
#         value["value"] = value["value"][1:]
#
#     try:
#         objects = search_function(
#             value, locale=locale, tmo=value["tmo"], user_data=user_data
#         )
#     except ConnectionError:
#         raise HTTPException(status_code=503, detail="Service unavailable!")
#     except TMONotFoundError:
#         objects = []
#
#     if (
#         any(
#             [
#                 value["action"].__contains__(var)
#                 for var in locale["hierarchy_action"]
#             ]
#         )
#         and len(objects) > 0
#     ):
#         tmo_ids, hierarchy = search_hierarchy(value["hierarchy"])
#         if hierarchy:
#             result = []
#             parents_to_delete = []
#             for obj in objects:
#                 if (
#                     obj["tmo_id"] not in tmo_ids
#                     or obj["p_id"] in parents_to_delete
#                 ):
#                     parents_to_delete.append(obj["id"])
#                     continue
#                 result.append(obj)
#             # result = list(filter(lambda x: x['tmo_id'] in tmo_ids, result))
#             value["action"] += hierarchy
#         else:
#             result = []
#             value["action"] = ""
#     else:
#         result = objects
#
#     return {
#         "total matches": len(result),
#         "result": result,
#         "action": value["action"],
#     }
