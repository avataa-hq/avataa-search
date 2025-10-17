from difflib import SequenceMatcher
import re

from elastic.client import client
from elastic.config import PERMISSION_INDEX, PARAMS_INDEX
from security.security_config import ADMIN_ROLE
from security.security_data_models import UserData


def contains_similar(value: str, part: str) -> bool:
    """Function checks if string contains substring or similar substring
    :param value: complicated string
    :param part: substring which matches will be searched
    :return: True if string contains substring, False if not
    """
    value = "".join(re.split("[ _-]", value)).lower()
    part = part.lower()
    value = value.lower()
    x = len(part)
    splitted = [value[y - x : y] for y in range(x, len(value) + x, x)]
    for tmp in splitted:
        s = SequenceMatcher(None, tmp, part)
        if s.ratio() > 0.5:
            return True

    value = value[::-1]
    splitted = [value[y - x : y] for y in range(x, len(value) + x, x)]
    splitted = [x[::-1] for x in splitted]
    for tmp in splitted:
        s = SequenceMatcher(None, tmp, part)
        if s.ratio() > 0.5:
            return True

    return False


def get_parent_id(parsed_value: dict, parents: list) -> list[int]:
    """Returns id of every object that was found by matching name
    Function also adds priority for every object depending on field object was found with
    :param parsed_value: query value with separator
    :param parents: inventory objects that was returned by query
    :return: list with objects id"""
    if isinstance(parsed_value["value"], str):
        parsed_value["value"] = [parsed_value["value"]]

    result = []
    for parent in parents:
        if any(
            [
                contains_similar(parent["_source"]["name"], part)
                for part in parsed_value["value"]
            ]
        ):
            result.append(parent["_source"]["id"])
            parent["_priority"] = 3
        else:
            parent["_priority"] = 2

    return result


def is_complicated(value: str) -> dict:
    """Function checks if query is the one word or several separated by some punctuation characters
    :param value: searching query
    :return: value and a separator by which query is separated
    """
    result = {"value": value, "separator": None}

    if parts := contains_separator(value, sep=" "):
        result["separator"] = " "
    elif parts := contains_separator(value, sep="_"):
        result["separator"] = "_"
    elif parts := contains_separator(value, sep="-"):
        result["separator"] = "-"

    if parts:
        result["value"] = parts

    return result


def create_query(
    value: str, params_id: list, params_names: list, tmo: list = None
) -> dict:
    """Function combine value and parameters allowed for search in one complicated query
    :param value: string that will be used for searching
    :param params: list with match constructions with id of searchable params
    :return: Elasticsearch query 'object'
    """
    query = {
        "bool": {
            "must": tmo if tmo else [],
            "should": [
                {
                    "nested": {
                        "path": "params",
                        "query": {
                            "bool": {
                                "must": {
                                    "query_string": {
                                        "query": f"{value}",
                                        "default_field": "params.value",
                                    },
                                },
                                "should": params_id,
                                "minimum_should_match": 1,
                            }
                        },
                    }
                },
                {
                    "query_string": {
                        "query": f"{value}",
                        "default_field": "name",
                    }
                },
            ],
            "minimum_should_match": 1,
        }
    }
    query["bool"]["should"].extend(params_names)

    return query


def contains_separator(value: str, sep: str) -> list[str] | None:
    """
    Function checks if string contains separated and separates it if it is
    If not function returns None
    If at least one part of string less than 3 symbols returns None
    :param value: string for searching
    :param sep:
    :return: list with separated string or None
    """
    if not value.__contains__(sep):
        return

    parts = value.split(sep)
    if any(len(part) < 3 for part in parts):
        return

    return parts


def description(priority: int):
    match priority:
        case 1:
            return "Found by parent object"
        case 2:
            return "Found by parameter"
        case 3:
            return "Found by name"


def get_match_score(value: str, search_value: str):
    s = SequenceMatcher(None, value.lower(), search_value)
    return s.ratio()


def clear_output(
    data: list[dict], value: str = None, conditional: bool = None
) -> list[dict]:
    for item in data:
        item["name"] = item["_source"].pop("name")
        item["id"] = item["_source"].pop("id")
        item["p_id"] = item["_source"].pop("p_id")
        if conditional:
            item["description"] = "Found by params"
        else:
            item["description"] = description(item["_priority"])
        if value and item["_priority"] != 1:
            item["match_score"] = get_match_score(item["name"], value)
        else:
            item["match_score"] = 0

    data = sorted(
        data,
        key=lambda x: (x["_priority"], x["_score"], x["match_score"]),
        reverse=True,
    )

    result = []
    parents_to_delete = []
    for item in data:
        if (
            item["match_score"] < 0.3
            and item["_priority"] == 3
            or item["p_id"] in parents_to_delete
        ):
            parents_to_delete.append(item["id"])
            continue
        del item["_priority"]
        del item["_score"]
        del item["match_score"]
        del item["_index"]
        item["tmo_id"] = item["_source"]["tmo_id"]
        del item["_source"]
        result.append(item)

    return result


def define_conditions(conditions: list[str]):
    result = []

    for condition in conditions:
        if condition.__contains__(">="):
            condition = condition.split(">=")
            result.append(
                {"range": {condition[0].strip(): {"gte": condition[1].strip()}}}
            )
        elif condition.__contains__("<="):
            condition = condition.split("<=")
            result.append(
                {"range": {condition[0].strip(): {"lte": condition[1].strip()}}}
            )
        elif condition.__contains__(">"):
            condition = condition.split(">")
            result.append(
                {"range": {condition[0].strip(): {"gt": condition[1].strip()}}}
            )
        elif condition.__contains__("<"):
            condition = condition.split("<")
            result.append(
                {"range": {condition[0].strip(): {"lt": condition[1].strip()}}}
            )
        elif condition.__contains__("="):
            condition = condition.split("=")
            result.append(
                {"match": {condition[0].strip(): condition[1].strip()}}
            )

    return result


def parse_vector(vec: dict, locale: dict, hierarchy: str = None):
    if vec["x"] == 1:
        action = locale["inventory_action"][0]
    elif vec["x"] == -1:
        action = locale["map_action"][0]
    else:
        action = locale["details_action"][0]

    if vec["x"] != 0 and hierarchy:
        action += locale["hierarchy_action"][0]
        # action += hierarchy

    return action


def define_action(words: list[str], locale: dict) -> dict:
    vec = dict.fromkeys(["x", "y"], 0)
    for word in words:
        if word in locale["inventory"]:
            vec["x"] = 1
        if word in locale["using"]:
            vec["y"] = 1
        if word in locale["details"]:
            vec["y"] = -1
        if word in locale["map"]:
            vec["x"] = -1

    value = {
        "value": [],
        "conditions": [],
        "tmo": None,
        "hierarchy": None,
    }

    for var in locale["show"]:
        if var in words:
            words.remove(var)
    for var in locale["with"]:
        if var in words:
            value["tmo"] = words[words.index(var) - 1]
            words.remove(value["tmo"])
            words.remove(var)
            # condition = ''
            for idx, word in enumerate(words):
                if (
                    any([word == var for var in locale["on"]])
                    or any([word == var for var in locale["in"]])
                    or any([word == var for var in locale["using"]])
                ):
                    break
                value["conditions"].append(word)
                # if any([word == var for var in locale['and']]) or any([word == var for var in locale['or']]):
                #     value['conditions'].append(condition.strip())
                #     condition = ''
                #     words.remove(word)
                # condition += f'{word} '

            # if condition != '':
            #     value['conditions'].append(condition.strip())
            # if word.__contains__('=') or word.__contains__('>') or word.__contains__('<'):
            #     value['conditions'].extend(words[idx - 1:idx + 2])
            # if any([word == var for var in locale['and']]) or any([word == var for var in locale['or']]):
            #     value['conditions'].append(word)

            for part in value["conditions"]:
                words.remove(part)

    for var in locale["using"]:
        if var in words:
            idx = words.index(var)
            value["hierarchy"] = " ".join(words[idx + 1 :])
            words = words[: idx + 1]

    value["action"] = parse_vector(vec, locale, hierarchy=value["hierarchy"])
    if any(
        value["action"].__contains__(f" {var} ") for var in locale["articles"]
    ):
        for var in locale["articles"]:
            if var in words:
                words.remove(var)

    for word in words:
        for idx, (key, var) in enumerate(locale.items()):
            if word in var and key != "articles":
                break
            if idx == len(locale.values()) - 1:
                value["value"].append(word)

    value["conditions"] = " ".join(value["conditions"])

    return value


def define_combine_rule(value: str, locale: dict) -> tuple:
    if value.__contains__(locale["or"][0]):
        combine_rule = "should"
        value = value.split(f" {locale['or'][0]} ")
    else:
        combine_rule = "must"
        value = value.split(f" {locale['and'][0]} ")

    return combine_rule, value


def check_permissions(data: list, user_data: UserData):
    if ADMIN_ROLE in user_data.realm_access.roles:
        result = data
    else:
        result = []
        mo_permissions = get_objects_id_by_permissions(user_data, "mo")
        parents_to_delete = []
        for item in data:
            if (
                item["id"] in mo_permissions
                and item["id"] not in parents_to_delete
            ):
                result.append(item)
            else:
                parents_to_delete.append(item["id"])

    return result


def check_conditional_permissions(conditions: str, user_data: UserData):
    tprms = get_objects_id_by_permissions(user_data, "tprm")

    result = []
    for condition in conditions:
        tprm_name = re.split(">=|>|<|<=|=", condition)[0].strip()

        query = {
            "bool": {
                "must": [
                    {"match": {"name": tprm_name}},
                    {"match": {"searchable": True}},
                ],
                "should": tprms,
                "minimum_should_match": 1,
            }
        }
        response = client.search(index=PARAMS_INDEX, query=query)
        ratios = [
            SequenceMatcher(None, tprm_name, res["_source"]["name"]).ratio()
            > 0.8
            for res in response["hits"]["hits"]
        ]
        if any(ratios):
            result.append(condition)

    return result


def get_objects_id_by_permissions(user_data: UserData, type_: str):
    permissions = []
    for role in user_data.realm_access.roles:
        permissions.append(
            {"match": {"permission": f"{user_data.realm_access.name}.{role}"}}
        )
    query = {
        "bool": {
            "must": [
                {
                    "bool": {
                        "should": [
                            {"match": {"read": True}},
                            {"match": {"admin": True}},
                        ],
                        "minimum_should_match": 1,
                    }
                },
                {"match": {"parent_type": type_}},
            ],
            "should": permissions,
            "minimum_should_match": 1,
        }
    }
    response = client.search(index=PERMISSION_INDEX, query=query)

    return [
        {"match": {"id": res["_source"]["parent_id"]}}
        for res in response["hits"]["hits"]
    ]
