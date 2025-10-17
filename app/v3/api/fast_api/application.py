from init_app import create_app
from settings import config
from v3.api.fast_api.routers import combine, hierarchy, inventory

"""
This is where the routers connect to the application
"""

v3_version = "3"
v3_prefix = f"{config.PREFIX}/v{v3_version}"

v3_options = dict(
    title=config.TITLE,
    version=v3_version,
    root_path=v3_prefix,
    debug=config.DEBUG,
)
v3_app = create_app(**v3_options)

v3_app.include_router(combine.router)
v3_app.include_router(hierarchy.router)
v3_app.include_router(inventory.router)
