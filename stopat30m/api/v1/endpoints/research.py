"""Admin-only web access to the local, locked research workspace."""
from functools import lru_cache
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

from stopat30m.auth.deps import require_role
from stopat30m.research_workspace import ResearchWorkspace, WorkspaceError, clean_json

router = APIRouter(prefix="/research", dependencies=[Depends(require_role("admin"))])


@lru_cache(maxsize=1)
def get_workspace():
    try:
        return ResearchWorkspace()
    except (WorkspaceError, OSError, ValueError) as exc:
        raise HTTPException(status_code=409, detail=f"Research workspace initialization failed: {exc}") from exc


def _call(fn, *args, **kwargs):
    try:
        return clean_json(fn(*args, **kwargs))
    except WorkspaceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
    except (ValueError, KeyError, OSError) as exc:
        raise HTTPException(status_code=409, detail=f"Research artifact validation failed: {exc}") from exc


class DownloadRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    protocol_id: Literal["eastmoney_etf"] = "eastmoney_etf"
    refresh: bool = False
    min_interval: float | None = Field(default=None, ge=2, le=60, allow_inf_nan=False)
    max_attempts: int | None = Field(default=None, ge=1, le=6)
    request_timeout: float | None = Field(default=None, ge=1, le=60, allow_inf_nan=False)


class RunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    protocol_id: str = Field(min_length=1, max_length=50, pattern=r"^[a-z0-9_]+$")
    snapshot_id: str = Field(pattern=r"^[0-9a-f]{64}$")


@router.get("/protocols")
def protocols(workspace: ResearchWorkspace = Depends(get_workspace)):
    return _call(workspace.protocols)


@router.get("/snapshots")
def snapshots(workspace: ResearchWorkspace = Depends(get_workspace)):
    return _call(workspace.snapshots)


@router.get("/jobs")
def jobs(workspace: ResearchWorkspace = Depends(get_workspace)):
    return _call(workspace.jobs)


@router.get("/jobs/{job_id}")
def job(job_id: str, workspace: ResearchWorkspace = Depends(get_workspace)):
    return _call(workspace.job, job_id)


@router.post("/download", status_code=202)
def download(request: DownloadRequest, workspace: ResearchWorkspace = Depends(get_workspace)):
    values = request.model_dump()
    if any(values[key] is None for key in ("min_interval", "max_attempts", "request_timeout")):
        from stopat30m.settings_store import research_preferences, SettingsError
        try:
            preferences = research_preferences()
        except (SettingsError, OSError, ValueError):
            raise HTTPException(status_code=409, detail="Saved research download preferences are invalid or unavailable.") from None
        for key, value in preferences.items():
            if values[key] is None:
                values[key] = value
    return _call(workspace.start_download, **values)


@router.post("/run", status_code=202)
def run(request: RunRequest, workspace: ResearchWorkspace = Depends(get_workspace)):
    return _call(workspace.start_run, **request.model_dump())


@router.get("/runs")
def runs(workspace: ResearchWorkspace = Depends(get_workspace)):
    return _call(workspace.runs)


@router.get("/runs/{run_id}")
def run_detail(run_id: str, workspace: ResearchWorkspace = Depends(get_workspace)):
    return _call(workspace.run_detail, run_id)


@router.get("/runs/{run_id}/table/{name}")
def table(run_id: str, name: str, offset: int = Query(0, ge=0), limit: int = Query(50, ge=1, le=200),
          split: Literal["train", "validation", "final_oos"] = "final_oos",
          strategy: Literal["momentum", "equal_weight", "buy_hold"] = "momentum",
          workspace: ResearchWorkspace = Depends(get_workspace)):
    return _call(workspace.table, run_id, name, offset=offset, limit=limit, split=split, strategy=strategy)
