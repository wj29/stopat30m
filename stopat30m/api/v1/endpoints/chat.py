"""Chat API endpoints — multi-turn agent conversation with tool calling."""

from __future__ import annotations

import json
import queue
import threading
import time
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from stopat30m.api.deps import get_db_session
from stopat30m.auth.deps import get_current_user
from stopat30m.storage.models import ChatMessage, ChatSession, User

router = APIRouter(prefix="/chat")

_STREAM_CHUNK_SIZE = 12
_STREAM_CHUNK_DELAY = 0.025


# ---------------------------------------------------------------------------
# Request / response schemas
# ---------------------------------------------------------------------------


class CreateSessionRequest(BaseModel):
    title: str = "新对话"
    stock_code: str | None = None
    stock_name: str | None = None


class SendMessageRequest(BaseModel):
    content: str = Field(..., min_length=1, max_length=4000)
    stock_code: str | None = None
    stock_name: str | None = None


class UpdateSessionRequest(BaseModel):
    title: str | None = None
    stock_code: str | None = None
    stock_name: str | None = None


# ---------------------------------------------------------------------------
# Session CRUD
# ---------------------------------------------------------------------------


@router.get("/sessions")
def list_sessions(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> list[dict[str, Any]]:
    rows = (
        db.query(ChatSession)
        .filter(ChatSession.user_id == user.id, ChatSession.is_deleted == False)  # noqa: E712
        .order_by(ChatSession.updated_at.desc())
        .limit(50)
        .all()
    )
    return [_session_to_dict(r) for r in rows]


@router.post("/sessions")
def create_session(
    req: CreateSessionRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> dict[str, Any]:
    session = ChatSession(
        user_id=user.id,
        title=req.title,
        stock_code=req.stock_code,
        stock_name=req.stock_name,
    )
    db.add(session)
    db.flush()
    return _session_to_dict(session)


@router.patch("/sessions/{session_id}")
def update_session(
    session_id: int,
    req: UpdateSessionRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> dict[str, Any]:
    session = _get_session(db, session_id, user.id)
    if req.title is not None:
        session.title = req.title
    if req.stock_code is not None:
        session.stock_code = req.stock_code
    if req.stock_name is not None:
        session.stock_name = req.stock_name
    session.updated_at = datetime.utcnow()
    return _session_to_dict(session)


@router.delete("/sessions/{session_id}")
def delete_session(
    session_id: int,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> dict[str, Any]:
    session = _get_session(db, session_id, user.id)
    session.is_deleted = True
    return {"deleted": True, "id": session_id}


# ---------------------------------------------------------------------------
# Message history
# ---------------------------------------------------------------------------


@router.get("/sessions/{session_id}/messages")
def list_messages(
    session_id: int,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> list[dict[str, Any]]:
    _get_session(db, session_id, user.id)
    rows = (
        db.query(ChatMessage)
        .filter(ChatMessage.session_id == session_id)
        .order_by(ChatMessage.created_at.asc())
        .all()
    )
    return [_message_to_dict(m) for m in rows]


# ---------------------------------------------------------------------------
# Send message (SSE streaming)
# ---------------------------------------------------------------------------


@router.post("/sessions/{session_id}/send")
def send_message(
    session_id: int,
    req: SendMessageRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> StreamingResponse:
    session = _get_session(db, session_id, user.id)

    if req.stock_code:
        session.stock_code = req.stock_code
    if req.stock_name:
        session.stock_name = req.stock_name

    user_msg = ChatMessage(
        session_id=session_id,
        role="user",
        content=req.content,
    )
    db.add(user_msg)
    db.flush()

    history = _load_history_for_llm(db, session_id, limit=20)

    context: dict[str, Any] = {}
    if session.stock_code:
        context["stock_code"] = session.stock_code
    if session.stock_name:
        context["stock_name"] = session.stock_name

    session.updated_at = datetime.utcnow()
    if session.title == "新对话" and len(req.content) > 0:
        session.title = req.content[:40]

    db.commit()

    return StreamingResponse(
        _generate_sse(
            session_id=session_id,
            message=req.content,
            history=history,
            context=context,
        ),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ---------------------------------------------------------------------------
# SSE generator — real-time streaming via thread-safe queue
# ---------------------------------------------------------------------------

_SENTINEL = object()


def _generate_sse(
    *,
    session_id: int,
    message: str,
    history: list[dict[str, Any]],
    context: dict[str, Any],
):
    from loguru import logger

    q: queue.Queue = queue.Queue()

    def _worker():
        try:
            from stopat30m.agent.factory import build_agent_executor

            executor = build_agent_executor()

            def _progress(evt: dict) -> None:
                q.put({"type": "progress", **evt})

            q.put({"type": "thinking", "message": "正在准备分析..."})

            result = executor.chat(
                message=message,
                session_id=str(session_id),
                progress_callback=_progress,
                context=context,
                history=history,
            )

            content = result.content or ""
            if not content and result.error:
                content = f"抱歉，分析过程出现错误: {result.error}"

            # Stream the answer in small chunks for a typing effect
            for i in range(0, len(content), _STREAM_CHUNK_SIZE):
                chunk = content[i : i + _STREAM_CHUNK_SIZE]
                q.put({"type": "answer_chunk", "content": chunk})

            q.put({
                "type": "answer_done",
                "model": result.model,
                "tokens_used": result.total_tokens,
                "tool_calls_count": result.total_steps,
            })

            _persist_assistant_message(
                session_id=session_id,
                content=content,
                tool_calls=result.tool_calls_log,
                tokens_used=result.total_tokens,
                model_used=result.model,
            )

        except Exception as exc:
            logger.error("Chat SSE error: %s", exc)
            q.put({"type": "error", "message": str(exc)})
        finally:
            q.put(_SENTINEL)

    t = threading.Thread(target=_worker, daemon=True)
    t.start()

    while True:
        try:
            item = q.get(timeout=0.1)
        except queue.Empty:
            continue
        if item is _SENTINEL:
            break
        yield _sse(item)
        if item.get("type") == "answer_chunk":
            time.sleep(_STREAM_CHUNK_DELAY)

    yield _sse({"type": "done"})


def _persist_assistant_message(
    *, session_id: int, content: str, tool_calls: list, tokens_used: int, model_used: str,
) -> None:
    """Save assistant response to DB in a separate session."""
    try:
        from stopat30m.storage.database import get_db

        with get_db() as db:
            msg = ChatMessage(
                session_id=session_id,
                role="assistant",
                content=content,
                tool_calls=tool_calls[:20] if tool_calls else None,
                tokens_used=tokens_used,
                model_used=model_used,
            )
            db.add(msg)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_session(db: Session, session_id: int, user_id: int) -> ChatSession:
    session = (
        db.query(ChatSession)
        .filter(ChatSession.id == session_id, ChatSession.user_id == user_id, ChatSession.is_deleted == False)  # noqa: E712
        .first()
    )
    if not session:
        raise HTTPException(status_code=404, detail="会话不存在")
    return session


def _load_history_for_llm(db: Session, session_id: int, limit: int = 20) -> list[dict[str, Any]]:
    rows = (
        db.query(ChatMessage)
        .filter(ChatMessage.session_id == session_id, ChatMessage.role.in_(["user", "assistant"]))
        .order_by(ChatMessage.created_at.desc())
        .limit(limit)
        .all()
    )
    rows.reverse()
    # Exclude the last user message (just added, will be sent separately)
    if rows and rows[-1].role == "user":
        rows = rows[:-1]
    return [{"role": m.role, "content": m.content} for m in rows]


def _session_to_dict(s: ChatSession) -> dict[str, Any]:
    return {
        "id": s.id,
        "title": s.title,
        "stock_code": s.stock_code,
        "stock_name": s.stock_name,
        "created_at": s.created_at.isoformat() if s.created_at else "",
        "updated_at": s.updated_at.isoformat() if s.updated_at else "",
    }


def _message_to_dict(m: ChatMessage) -> dict[str, Any]:
    return {
        "id": m.id,
        "role": m.role,
        "content": m.content,
        "tool_calls": m.tool_calls,
        "tokens_used": m.tokens_used,
        "model_used": m.model_used,
        "created_at": m.created_at.isoformat() if m.created_at else "",
    }


def _sse(data: dict) -> str:
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
