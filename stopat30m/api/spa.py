"""Serve known browser routes without turning missing API/assets into HTML."""

from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException


class SPAStaticFiles(StaticFiles):
    routes = {
        "login", "register", "analysis", "watchlist", "market-review",
        "trading", "signals", "backtest", "model-lab", "users", "records", "settings", "operations", "model-settings",
    }

    async def get_response(self, path: str, scope):
        try:
            response = await super().get_response(path, scope)
        except HTTPException as exc:
            if exc.status_code != 404:
                raise
            response = None
        if response is not None and response.status_code != 404:
            return response
        route = path.strip("/")
        if scope["method"] in ("GET", "HEAD") and route in self.routes:
            return await super().get_response("index.html", scope)
        if response is not None:
            return response
        raise HTTPException(status_code=404)
