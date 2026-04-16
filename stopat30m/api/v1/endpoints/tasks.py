"""Async analysis task queue API (submit, list, SSE stream, status)."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from stopat30m.analysis.task_queue import get_task_queue
from stopat30m.auth.deps import get_current_user
from stopat30m.storage.models import User

router = APIRouter(prefix="/tasks")


class TaskSubmitBody(BaseModel):
    """Submit one or more stock codes for analysis."""

    code: str | None = Field(None, description="Single stock code")
    codes: list[str] | None = Field(None, description="Batch of stock codes")
    stock_name: str | None = None
    original_query: str | None = None
    force_refresh: bool = False


@router.post("")
def submit_tasks(body: TaskSubmitBody, user: User = Depends(get_current_user)) -> dict[str, Any]:
    """Enqueue analysis task(s). Returns accepted tasks and duplicate errors."""
    if body.codes:
        stock_list = list(body.codes)
    elif body.code:
        stock_list = [body.code]
    else:
        raise HTTPException(status_code=400, detail="Provide `code` or `codes`")

    q = get_task_queue()
    accepted, duplicates = q.submit_tasks_batch(
        stock_list,
        stock_name=body.stock_name,
        original_query=body.original_query,
        force_refresh=body.force_refresh,
        user_id=user.id,
    )
    return {
        "accepted": [t.to_dict() for t in accepted],
        "duplicates": [
            {"stock_code": d.stock_code, "existing_task_id": d.existing_task_id, "message": str(d)}
            for d in duplicates
        ],
    }


@router.get("")
def list_tasks(limit: int = 50, user: User = Depends(get_current_user)) -> dict[str, Any]:
    """List recent tasks (newest first)."""
    q = get_task_queue()
    tasks = q.list_all_tasks(limit=limit)
    return {"tasks": [t.to_dict() for t in tasks], "stats": q.get_task_stats()}


@router.get("/stream")
async def task_event_stream(user: User = Depends(get_current_user)) -> StreamingResponse:
    """Server-Sent Events: task_created, task_started, task_progress, task_completed, task_failed."""

    queue: asyncio.Queue = asyncio.Queue()
    tq = get_task_queue()
    tq.subscribe(queue)

    async def _gen():
        try:
            while True:
                event = await queue.get()
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
        finally:
            tq.unsubscribe(queue)

    return StreamingResponse(
        _gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@router.get("/{task_id}")
def get_task_status(task_id: str, user: User = Depends(get_current_user)) -> dict[str, Any]:
    """Get a single task by ID (includes `result` when completed)."""
    q = get_task_queue()
    task = q.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task.to_dict()
