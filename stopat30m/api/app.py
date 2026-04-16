"""FastAPI application factory."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from stopat30m.storage.database import init_db

from .v1.router import api_v1_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


def create_app(static_dir: str | None = None) -> FastAPI:
    app = FastAPI(
        title="stopat30m",
        description="A-share quantitative analysis and trading platform",
        version="0.1.0",
        lifespan=lifespan,
    )

    from stopat30m.config import get
    cfg_origins = get("server", "cors_origins", "http://localhost:5173,http://localhost:3000")
    origins = os.getenv("CORS_ORIGINS", cfg_origins).split(",")
    allow_all = os.getenv("CORS_ALLOW_ALL", "").lower() == "true"

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"] if allow_all else [o.strip() for o in origins],
        allow_credentials=not allow_all,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(api_v1_router)

    # Serve React build if available (production mode)
    spa_dir = Path(static_dir) if static_dir else Path(__file__).parent.parent / "web" / "frontend" / "dist"
    if spa_dir.is_dir() and (spa_dir / "index.html").exists():
        app.mount("/", StaticFiles(directory=str(spa_dir), html=True), name="spa")

    return app


app = create_app()
