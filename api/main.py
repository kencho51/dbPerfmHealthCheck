"""
FastAPI application factory.

Start the dev server from the project root:
    uv run fastapi dev api/main.py --port 8000

Or with uvicorn directly:
    uv run uvicorn api.main:app --reload --port 8000

Endpoints:
    Swagger UI  : http://127.0.0.1:8000/docs
    ReDoc       : http://127.0.0.1:8000/redoc
    Health check: http://127.0.0.1:8000/health
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.database import ASYNC_DATABASE_URL

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lifespan — runs once on startup / shutdown
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Schema is managed externally via migration.sql applied through the Neon REST API.
    # All SQL runs over Neon HTTPS REST API — no port 5432 connection at startup.
    logger.info("Starting up — Neon PostgreSQL via HTTPS REST API")
    yield
    logger.info("Shutting down …")


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def create_app() -> FastAPI:
    app = FastAPI(
        title="DB Performance Health Check API",
        description=(
            "Ingest, deduplicate, and analyse Splunk database performance CSV exports. "
            "Curate recurring query patterns for ML training."
        ),
        version="0.1.0",
        lifespan=lifespan,
    )

    # CORS — allow local Next.js dev server (ports 3000 / 3001)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://localhost:3000",
            "http://localhost:3001",
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Register routers (imported lazily so missing Phase 2+ files don't crash Phase 0)
    _register_routers(app)

    return app


def _register_routers(app: FastAPI) -> None:
    """Attach routers. Unimplemented phases are skipped gracefully."""
    try:
        from api.routers.analytics import router as analytics_router
        app.include_router(analytics_router, prefix="/api/analytics", tags=["analytics"])
    except ImportError:
        logger.debug("analytics router not yet available")

    try:
        from api.routers.queries import router as queries_router
        app.include_router(queries_router, prefix="/api/queries", tags=["queries"])
    except ImportError:
        logger.debug("queries router not yet available")

    try:
        from api.routers.labels import router as labels_router
        app.include_router(labels_router, prefix="/api/labels", tags=["labels"])
    except ImportError:
        logger.debug("labels router not yet available")

    try:
        from api.routers.curated import router as curated_router
        app.include_router(curated_router, prefix="/api/curated", tags=["curated"])
    except ImportError:
        logger.debug("curated router not yet available")

    try:
        from api.routers.upload import router as upload_router
        app.include_router(upload_router, prefix="/api", tags=["upload"])
    except ImportError:
        logger.debug("upload router not yet available")

    try:
        from api.routers.validate import router as validate_router
        app.include_router(validate_router, prefix="/api", tags=["validate"])
    except ImportError:
        logger.debug("validate router not yet available")

    try:
        from api.routers.export import router as export_router
        app.include_router(export_router, prefix="/api", tags=["export"])
    except ImportError:
        logger.debug("export router not yet available")


# ---------------------------------------------------------------------------
# Module-level app instance (uvicorn / fastapi dev entrypoint)
# ---------------------------------------------------------------------------

app = create_app()


# ---------------------------------------------------------------------------
# Health-check endpoint (always available)
# ---------------------------------------------------------------------------

@app.get("/health", tags=["system"])
async def health() -> dict:
    # Mask credentials in the displayed URL
    display_url = ASYNC_DATABASE_URL.split("@")[-1] if "@" in ASYNC_DATABASE_URL else ASYNC_DATABASE_URL
    return {
        "status": "ok",
        "db": f"postgresql+asyncpg://<credentials>@{display_url}",
    }
