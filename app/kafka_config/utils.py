import functools
import time
from enum import Enum

import requests
from requests.exceptions import ConnectionError
from fastapi import HTTPException

from kafka_config import config
from security.security_config import KEYCLOAK_TOKEN_URL


class ObjEventStatus(Enum):
    CREATED = "created"
    UPDATED = "updated"
    DELETED = "deleted"


class ObjClassNames(Enum):
    MO = "MO"
    TMO = "TMO"
    TPRM = "TPRM"
    PRM = "PRM"


def consumer_config(conf):
    if "sasl.mechanisms" in conf.keys():
        conf["oauth_cb"] = functools.partial(_get_token_for_kafka_producer)

    return conf


def _get_token_for_kafka_producer(conf):
    """Get token from Keycloak for MS Inventory kafka producer and returns it with expires time"""
    payload = {
        "grant_type": "client_credentials",
        "scope": str(config.KAFKA_KEYCLOAK_SCOPES),
    }

    attempt = 5
    while attempt > 0:
        try:
            resp = requests.post(
                KEYCLOAK_TOKEN_URL,
                auth=(
                    config.KAFKA_KEYCLOAK_CLIENT_ID,
                    config.KAFKA_KEYCLOAK_SECRET,
                ),
                data=payload,
                timeout=5,
            )
        except ConnectionError:
            print("Connection Error to Keycloak Server. Another attempting")
            time.sleep(1)
            attempt -= 1
        else:
            if resp.status_code == 200:
                break
            else:
                print(
                    f"Connection Error to Keycloak Server. {resp.status_code=} {resp.json()=}"
                )
                time.sleep(1)
                attempt -= 1
                continue
    else:
        raise HTTPException(
            status_code=503, detail="Token verification service unavailable"
        )
    token = resp.json()
    expires_in = float(token["expires_in"]) * 0.9
    # print(
    #     f"KEYCLOAK TOKEN FOR KAFKA: ...{token['access_token'][-3:]} EXPIRED_TIME:{expires_in}."
    # )
    return token["access_token"], time.time() + expires_in
