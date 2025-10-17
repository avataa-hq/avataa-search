from keycloak import KeycloakOpenID

from security.security_config import (
    KEYCLOAK_URL,
    KEYCLOAK_CLIENT_ID,
    KEYCLOAK_REALM,
)
from settings.config import INV_PASS, INV_USER


def get_auth_token():
    keycloak_openid = KeycloakOpenID(
        server_url=f"{KEYCLOAK_URL}",
        client_id=f"{KEYCLOAK_CLIENT_ID}",
        realm_name=f"{KEYCLOAK_REALM}",
    )
    return keycloak_openid.token(INV_USER, INV_PASS)
