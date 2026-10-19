"""A single, observable data update per local dataset, across worker processes."""

from __future__ import annotations

import fcntl
import json
import os
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from stopat30m.data.health import inspect_data


class UpdateBusyError(RuntimeError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _paths(data_dir: Path) -> tuple[Path, Path]:
    return data_dir / ".update.lock", data_dir / ".update-status.json"


def _save(path: Path, job: dict[str, Any]) -> None:
    temporary = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(json.dumps(job, ensure_ascii=False, indent=2))
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def update_status(data_dir: Path) -> dict[str, Any] | None:
    lock_path, status_path = _paths(data_dir)
    if not status_path.exists():
        return None
    try:
        job = json.loads(status_path.read_text())
    except (OSError, ValueError):
        return {"status": "failed", "error": "无法读取上一次数据更新状态。"}
    if job.get("status") == "running":
        with lock_path.open("a+") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                pass
            else:
                # The worker may have completed (or a new job may have run)
                # between our initial read and obtaining the lock.
                try:
                    latest = json.loads(status_path.read_text())
                except (OSError, ValueError):
                    latest = job
                job = latest
                if job.get("status") == "running":
                    job.update(status="interrupted", completed_at=_now(), error="更新进程已结束，任务没有正常完成；可重新启动以继续增量更新。")
                    _save(status_path, job)
                fcntl.flock(lock, fcntl.LOCK_UN)
    return job


def start_update(
    data_dir: Path, *, source: str, start_date: str | None = None,
    end_date: str | None = None, runner: Callable[..., None] | None = None,
) -> dict[str, Any]:
    """Start an explicitly requested download; never downloads at application startup."""
    data_dir.mkdir(parents=True, exist_ok=True)
    lock_path, status_path = _paths(data_dir)
    lock = lock_path.open("a+")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock.close()
        raise UpdateBusyError("已有数据更新任务正在运行。") from None
    job = {
        "id": uuid.uuid4().hex, "status": "running", "source": source,
        "started_at": _now(), "completed_at": None, "error": None,
        "requested_start_date": start_date, "requested_end_date": end_date,
    }
    try:
        _save(status_path, job)
    except Exception:
        lock.close()
        raise

    def run() -> None:
        final = dict(job)
        try:
            download = runner
            if download is None:
                from stopat30m.data.provider import download_cn_data
                download = download_cn_data
            download(source=source, target_dir=str(data_dir), start_date=start_date, end_date=end_date, parallel=False)
            health = inspect_data(data_dir)
            if not health["data_available"]:
                raise RuntimeError("下载函数已返回，但本地仍缺少有效日历、股票池或价格文件。")
            final.update(status="completed", data_status=health)
        except Exception as exc:
            final.update(status="failed", error=str(exc))
        finally:
            final["completed_at"] = _now()
            try:
                _save(status_path, final)
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)
                lock.close()

    worker = threading.Thread(target=run, name="data-update", daemon=True)
    try:
        worker.start()
    except Exception:
        job.update(status="failed", completed_at=_now(), error="无法启动更新线程。")
        _save(status_path, job)
        lock.close()
        raise
    return job
