"""Health check endpoint."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter

from stopat30m.data.provider import data_exists

router = APIRouter()


@router.get("/health")
def health_check() -> dict:
    return {
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat(),
        "data_available": data_exists(),
    }
