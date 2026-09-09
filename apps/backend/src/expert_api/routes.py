"""The same implemented public routes serve HTTP and deterministic OpenAPI."""
from fastapi import FastAPI

from .api.auth import router as auth_router
from .api.debug_capture import router as debug_capture_router
from .api.documents import router as document_router
from .api.document_tree import router as document_tree_router
from .api.jobs import router as job_router
from .api.job_events import router as job_events_router
from .api.library import router as library_router
from .api.purge import router as purge_router
from .api.runs import router as run_router
from .api.sources import router as source_router
from .api.system import router as system_router
from .api.version_commands import router as version_commands_router


def install_public_routes(app: FastAPI) -> None:
    for router in (system_router, auth_router, document_router, job_router, job_events_router,
                   library_router, run_router, source_router, version_commands_router, document_tree_router,
                   debug_capture_router, purge_router):
        app.include_router(router)
