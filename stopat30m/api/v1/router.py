"""API v1 router aggregating all endpoint modules."""

from __future__ import annotations

from fastapi import APIRouter

from .endpoints import admin, analysis, auth, backtest, chat, health, market, signals, system, tasks, trading, watchlist

api_v1_router = APIRouter(prefix="/api/v1")
api_v1_router.include_router(health.router, tags=["health"])
api_v1_router.include_router(auth.router, tags=["auth"])
api_v1_router.include_router(admin.router, tags=["admin"])
api_v1_router.include_router(analysis.router, tags=["analysis"])
api_v1_router.include_router(market.router, tags=["market"])
api_v1_router.include_router(trading.router, tags=["trading"])
api_v1_router.include_router(signals.router, tags=["signals"])
api_v1_router.include_router(backtest.router, tags=["backtest"])
api_v1_router.include_router(system.router, tags=["system"])
api_v1_router.include_router(tasks.router, tags=["tasks"])
api_v1_router.include_router(watchlist.router, tags=["watchlist"])
api_v1_router.include_router(chat.router, tags=["chat"])
