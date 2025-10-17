import time

from keycloak import KeycloakOpenID
from resistant_kafka_avataa.common_exceptions import TokenIsNotValid

from kafka_config.config import (
    KAFKA_KEYCLOAK_CLIENT_ID,
    KAFKA_KEYCLOAK_SECRET,
)
from security.security_config import KEYCLOAK_REALM, KEYCLOAK_TOKEN_URL


def get_token_for_kafka_by_keycloak(conf) -> tuple[str, float]:
    client = KeycloakOpenID(
        server_url=KEYCLOAK_TOKEN_URL,
        client_id=KAFKA_KEYCLOAK_CLIENT_ID,
        realm_name=KEYCLOAK_REALM,
        client_secret_key=KAFKA_KEYCLOAK_SECRET,
    )

    attempt = 5
    while attempt > 0:
        try:
            tkn = client.token(grant_type="client_credentials")
            token = tkn["access_token"]
            expires_in = float(tkn.get["expires_in", 0]) * 0.95
            return token, time.time() + expires_in
        except ConnectionError:
            time.sleep(1)
            attempt -= 1
    raise TokenIsNotValid("Token verification service unavailable")
