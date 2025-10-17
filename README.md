# Search

## Environment variables

```toml
DB_HOST=<pgbouncer/postgres_host>
DB_NAME=<pgbouncer/postgres_search_db_name>
DB_PASS=<pgbouncer/postgres_search_password>
DB_PORT=<pgbouncer/postgres_port>
DB_TYPE=postgresql+asyncpg
DB_USER=<pgbouncer/postgres_search_user>
DOCS_CUSTOM_ENABLED=<True/False>
DOCS_REDOC_JS_URL=<redoc_js_url>
DOCS_SWAGGER_CSS_URL=<swagger_css_url>
DOCS_SWAGGER_JS_URL=<swagger_js_url>
ES_HOST=<elasticsearch_host>
ES_PASS=<elasticsearch_search_password>
ES_PORT=<elasticsearch_port>
ES_PROTOCOL=<elasticsearch_protocol>
ES_USER=<elasticsearch_search_user>
GROUP_BUILDER_GRPC_PORT=<group_builder_grpc_port>
GROUP_BUILDER_HOST=<group_builder_host>
HIERARCHY_GRPC_PORT=<hierarchy_grpc_port>
HIERARCHY_HOST=<hierarchy_host>
HIERARCHY_INDEX=<hierarchy_index>
HIERARCHY_PORT=<hierarchy_port>
HIERARCHY_PROTOCOL=<hierarchy_protocol>
INVENTORY_HOST=<inventory_host>
INVENTORY_INDEX=<inventory_index>
INVENTORY_PORT=<inventory_port>
INVENTORY_PROTOCOL=<inventory_protocol>
INV_PASS=<platform_read_password>
INV_USER=<platform_read_user>
KAFKA_CONSUMER_GROUP_ID=Search
KAFKA_CONSUMER_OFFSET=earliest
KAFKA_CONSUMER_WORKERS=<kafka_consumer_workers_number>
KAFKA_GROUP_BUILDER_GROUP_TOPIC=group
KAFKA_GROUP_STATISTIC_TOPIC=group_data.changes
KAFKA_HIERARCHY_CHANGES_TOPIC=hierarchy.changes
KAFKA_INVENTORY_CHANGES_TOPIC=inventory.changes.part
KAFKA_INVENTORY_SECURITY_TOPIC=inventory.security
KAFKA_KEYCLOAK_CLIENT_ID=<kafka_client>
KAFKA_KEYCLOAK_CLIENT_SECRET=<kafka_client_secret>
KAFKA_KEYCLOAK_SCOPES=profile
KAFKA_SECURED=<True/False>
KAFKA_SECURITY_OFFSET=latest
KAFKA_SECURITY_TOPIC=inventory.security
KAFKA_SUBSCRIBE_TOPICS=inventory.changes
KAFKA_TURN_ON=<True/False>
KAFKA_URL=<kafka_host>:<kafka_port>
KAFKA_ZEEBE_CHANGES_TOPIC=process.changes.part
KAFKA_ZEEBE_CONSUMER_WORKERS=<zeebe_client_kafka_consumer_workers_number>
KAFKA_ZEEBE_PROCESS_INSTANCE_EXPORTER_TOPIC=zeebe-process-instance-exporter
KEYCLOAK_CLIENT_ID=<keycloak_platform_client>
KEYCLOAK_HOST=<keycloak_host>
KEYCLOAK_PORT=<keycloak_port>
KEYCLOAK_PROTOCOL=<keycloak_protocol>
KEYCLOAK_REALM=avataa
KEYCLOAK_REDIRECT_HOST=<keycloak_external_host>
KEYCLOAK_REDIRECT_PORT=<keycloak_external_port>
KEYCLOAK_REDIRECT_PROTOCOL=<keycloak_external_protocol>
OPA_HOST=<opa_host>
OPA_POLICY=main
OPA_PORT=<opa_port>
OPA_PROTOCOL=<opa_protocol>
PARAMS_INDEX=<params_index>
PERMISSION_INDEX=<permission_index>
SECURITY_MIDDLEWARE_HOST=<security_middleware_host>
SECURITY_MIDDLEWARE_PORT=<security_middleware_port>
SECURITY_MIDDLEWARE_PROTOCOL=<security_middleware_protocol>
SECURITY_TYPE=<security_type>
TMO_INDEX=<tmo_index>
UVICORN_WORKERS=<uvicorn_workers_number>
ZEEBE_CLIENT_GRPC_PORT=<zeebe_client_grpc_port>
ZEEBE_CLIENT_HOST=<zeebe_client_host>
```

### Explanation

#### Elastic
- ES_PROTOCOL - network protocol (http/https)
- ES_HOST - elasticsearch host
- ES_PORT - elasticsearch port
- ES_USER - elasticsearch user
- ES_PASS - elasticsearch password
- INVENTORY_INDEX - name of index where inventory objects will be stored
- PARAMS_INDEX - name of index where param types will be stored
- TMO_INDEX - name of index where object types will be stored
- HIERARCHY_INDEX - name of index where hierarchies will be stored
- PERMISSION_INDEX - name of index where permissions will be stored
- INVENTORY_INDEX_V2 - name of index where inventory objects will be stored for API v2
#### SECURITY GENERAL
- SECURITY_TYPE - type of security
- ADMIN_ROLE - admin role from keycloak
#### KEYCLOAK
- KEYCLOAK_PROTOCOL - keycloak connection protocol (http/https)
- KECLOAK_HOST - keycloak host
- KECLOAK_PORT - keycloak port
- KECLOAK_REALM - realm (Avataa is default)
- KECLOAK_CLIENT_ID - client_id for this service (web is default)
- KEYCLOAK_CLIENT_SECRET
#### KEYCLOAK LOGIN PAGE
- KEYCLOAK_REDIRECT_PROTOCOL
- KEYCLOAK_REDIRECT_HOST
- KEYCLOAK_REDIRECT_PORT
#### OPA
- OPA_PROTOCOL
- OPA_HOST
- OPA_PORT
- OPA_POLICY
#### SERVICES CREDENTIALS
- INV_USER - user with permissions to fully read inventory
- INV_PASS - password of this user
#### INVENTORY
- INVENTORY_PROTOCOL - inventory connection protocol (http/https)
- INVENTORY_HOST - inventory service host
- INVENTORY_PORT - inventory service port
- INVENTORY_GRPC_PORT = (default: _50051_)
#### HIERARCHY
- HIERARCHY_PROTOCOL - hierarchy service connection protocol (http/https)
- HIERARCHY_HOST - hierarchy service host
- HIERARCHY_PORT - hierarchy service port
#### KAFKA-INVENTORY
- KAFKA_TURN_ON - enables kafka consumer
- KAFKA_URL - kafka url
- KAFKA_CONSUMER_GROUP_ID - name of consumers group, may be unique for each service
- KAFKA_CONSUMER_OFFSET - defines offset from where to read topics ('earliest' is default)
- KAFKA_INVENTORY_CHANGES_TOPIC - name of topic to subscribe - must be the same as topic where inventory publishes
- KAFKA_KEYCLOAK_SCOPES
- KAFKA_KEYCLOAK_CLIENT_ID - client id in keycloak for consumer auth
- KAFKA_KEYCLOAK_SECRET - client secret in keycloak for consumer auth
#### KAFKA-MS-ZEEBE
- KAFKA_ZEEBE_CHANGES_TOPIC - name of topic to subscribe - must be the same as topic where MS Zeebe publishes
- KAFKA_ZEEBE_PROCESS_INSTANCE_EXPORTER_TOPIC - name of topic to subscribe - must be the same as topic where camunda exporter publishes changes of process instances
#### KAFKA-MS-HIERARCHY
- KAFKA_HIERARCHY_HIERARCHIES_CHANGES_TOPIC - name of topic to subscribe - must be the same as topic where MS Hierarchy publishes changes of hierarchies
- KAFKA_HIERARCHY_LEVELS_CHANGES_TOPIC - name of topic to subscribe - must be the same as topic where MS Hierarchy publishes changes of levels
- KAFKA_HIERARCHY_OBJ_CHANGES_TOPIC - name of topic to subscribe - must be the same as topic where MS Hierarchy publishes changes of nodes
#### KAFKA-MS-GROUP-BUILDER
- KAFKA_GROUP_BUILDER_GROUP_TOPIC - name of topic to subscribe - must be the same as topic where MS GroupBuilder publishes changes to objects included in a group
- KAFKA_GROUP_STATISTIC_TOPIC - name of topic to subscribe - must be the same as topic where MS GroupBuilder publishes changes of group statistic
#### KAFKA-SECURITY
- KAFKA_SECURITY_OFFSET
- KAFKA_SECURITY_TOPIC
#### Database
- DB_TYPE = Type of database  (default: _postgresql+asyncpg_)
- DB_USER = Pre-created user in the database with rights to edit the database (default: _root_)
- DB_PASS = Database user password (default: _root_)
- DB_HOST = Database host (default: _localhost_)
- DB_PORT = Database port (default: _5432_)
- DB_NAME = Name of the previously created database (default: _search_)

#### Other
- DEBUG - changes startup configuration

#### Compose

- `REGISTRY_URL` - Docker regitry URL, e.g. `harbor.domain.com`
- `PLATFORM_PROJECT_NAME` - Docker regitry project Docker image can be downloaded from, e.g. `avataa`