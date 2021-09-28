from fastapi import FastAPI
from .routers import api_root
from .routers.v1 import api_data_download
from .routers.v2 import api_data_download as api_data_download_v2
from .routers.v2 import api_object_get as api_object_get

def api_registry(app: FastAPI):
    app.include_router(api_root.router)
    app.include_router(api_data_download.router, prefix="/v1")
    app.include_router(api_data_download_v2.router, prefix="/v2")
    app.include_router(api_object_get.router, prefix="/v2")