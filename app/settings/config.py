import os

# AUTH CREDENTIALS
INV_USER = os.environ.get("INV_USER", "test_user")
INV_PASS = os.environ.get("INV_PASS", None)

# SERVICES
INVENTORY_PROTOCOL = os.environ.get("INVENTORY_PROTOCOL", "http")
INVENTORY_HOST = os.environ.get("INVENTORY_HOST", "inventory")
INVENTORY_PORT = os.environ.get("INVENTORY_PORT", "8000")
INVENTORY_GRPC_PORT = os.environ.get("INVENTORY_GRPC_PORT", "50051")
INVENTORY_URL = f"{INVENTORY_PROTOCOL}://{INVENTORY_HOST}"
if INVENTORY_PORT:
    INVENTORY_URL += f":{INVENTORY_PORT}"
INVENTORY_API_URL = f"{INVENTORY_URL}/api/inventory/v1"

HIERARCHY_PROTOCOL = os.environ.get("HIERARCHY_PROTOCOL", "http")
HIERARCHY_HOST = os.environ.get("HIERARCHY_HOST", "hierarchy")
HIERARCHY_PORT = os.environ.get("HIERARCHY_PORT", "8000")
HIERARCHY_GRPC_PORT = os.environ.get("HIERARCHY_GRPC_PORT", "50051")
HIERARCHY_URL = f"{HIERARCHY_PROTOCOL}://{HIERARCHY_HOST}"
if HIERARCHY_PORT:
    HIERARCHY_URL += f":{HIERARCHY_PORT}"
HIERARCHY_API_URL = f"{HIERARCHY_URL}/api/hierarchy/v1"


ZEEBE_CLIENT_HOST = os.environ.get("ZEEBE_CLIENT_HOST", "zeebe-client")
ZEEBE_CLIENT_GRPC_PORT = os.environ.get("ZEEBE_CLIENT_GRPC_PORT", "50051")

GROUP_BUILDER_HOST = os.environ.get("GROUP_BUILDER_HOST", "group_builder")
GROUP_BUILDER_GRPC_PORT = os.environ.get("GROUP_BUILDER_GRPC_PORT", "50051")

# OTHER
DEBUG = os.environ.get("DEBUG", "False").upper() in ("TRUE", "Y", "YES", "1")

# DB
DB_TYPE = os.environ.get("DB_TYPE", "postgresql+asyncpg")
DB_USER = os.environ.get("DB_USER", "search_admin")
DB_PASS = os.environ.get("DB_PASS", None)
DB_HOST = os.environ.get("DB_HOST", "pgbouncer")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "search")
DB_SCHEMA = os.environ.get("DB_SCHEMA", "public")

DATABASE_URL = f"{DB_TYPE}://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# APP
TITLE = "Search"
PREFIX = f"/api/{TITLE.replace(' ', '_').lower()}"

# DOCUMENTATION
DOCS_ENABLED = os.environ.get("DOCS_ENABLED", "True").upper() in (
    "TRUE",
    "Y",
    "YES",
    "1",
)
DOCS_CUSTOM_ENABLED = os.environ.get(
    "DOCS_CUSTOM_ENABLED", "False"
).upper() in ("TRUE", "Y", "YES", "1")
DOCS_SWAGGER_JS_URL = os.environ.get("DOCS_SWAGGER_JS_URL", None)
DOCS_SWAGGER_CSS_URL = os.environ.get("DOCS_SWAGGER_CSS_URL", None)
DOCS_REDOC_JS_URL = os.environ.get("DOCS_REDOC_JS_URL", None)

SERVER_GRPC_PORT = os.environ.get("SERVER_GRPC_PORT", "50051")

# TESTS
TEST_LOCAL_DB_HOST = os.environ.get("TEST_DOCKER_DB_HOST", "localhost")
TESTS_RUN_CONTAINER_POSTGRES_LOCAL = os.environ.get(
    "TESTS_RUN_CONTAINER_POSTGRES_LOCAL",
    "False",
).upper() in ("TRUE", "Y", "YES", "1")

TESTS_DB_HOST = os.environ.get("TESTS_DB_HOST")
TESTS_DB_PORT = os.environ.get("TESTS_DB_PORT", "5432")
TESTS_DB_USER = os.environ.get("TESTS_DB_USER")
TESTS_DB_PASS = os.environ.get("TESTS_DB_PASS")
TESTS_DB_NAME = os.environ.get("TESTS_DB_NAME", "airflow")
TESTS_DB_TYPE = os.environ.get("TESTS_DB_TYPE", "postgresql+asyncpg")

TEST_DATABASE_URL = f"{TESTS_DB_TYPE}://{TESTS_DB_USER}:{TESTS_DB_PASS}@{TESTS_DB_HOST}:{TESTS_DB_PORT}/{TESTS_DB_NAME}"
