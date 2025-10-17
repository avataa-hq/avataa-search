from init_app import create_app
from settings import config
from v2.routers.inventory.router import router as inventory_router
from v2.routers.severity.router import router as severity_router
from v2.routers.hierarchy.hierarchy_router import (
    router as ms_hierarchy_h_data_router,
)
from v2.routers.hierarchy.reload_router import (
    router as ms_hierarchy_reload_router,
)
from v2.routers.hierarchy.hierarchy_info_router import (
    router as ms_hierarchy_info_router,
)

v2_prefix = f"{config.PREFIX}/v2"

v2_app = create_app(
    title=config.TITLE,
    version="2",
    root_path=v2_prefix,
    debug=config.DEBUG,
)
v2_app.include_router(inventory_router)
v2_app.include_router(severity_router)
v2_app.include_router(ms_hierarchy_h_data_router)
v2_app.include_router(ms_hierarchy_reload_router)
v2_app.include_router(ms_hierarchy_info_router)
