# -*- coding: utf-8 -*-
"""
异步分析任务队列

职责：
1. 管理异步分析任务的生命周期
2. 防止相同股票代码重复提交
3. 提供 SSE 事件广播机制
"""

from __future__ import annotations

import asyncio
import threading
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Tuple

from loguru import logger

from stopat30m.data.normalize import normalize_instrument

if TYPE_CHECKING:
    from asyncio import Queue as AsyncQueue


def _dedupe_stock_code_key(stock_code: str) -> str:
    """Duplicate-detection key: canonical A-share instrument code."""
    return normalize_instrument(stock_code.strip())


class TaskStatus(str, Enum):
    """Task status enumeration"""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class TaskInfo:
    """Task information dataclass."""

    task_id: str
    stock_code: str
    stock_name: Optional[str] = None
    status: TaskStatus = TaskStatus.PENDING
    progress: int = 0
    message: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    original_query: Optional[str] = None
    user_id: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "task_id": self.task_id,
            "stock_code": self.stock_code,
            "stock_name": self.stock_name,
            "status": self.status.value,
            "progress": self.progress,
            "message": self.message,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error": self.error,
            "original_query": self.original_query,
        }
        if self.result is not None:
            d["result"] = self.result
        return d

    def copy(self) -> TaskInfo:
        return TaskInfo(
            task_id=self.task_id,
            stock_code=self.stock_code,
            stock_name=self.stock_name,
            status=self.status,
            progress=self.progress,
            message=self.message,
            result=self.result,
            error=self.error,
            created_at=self.created_at,
            started_at=self.started_at,
            completed_at=self.completed_at,
            original_query=self.original_query,
            user_id=self.user_id,
        )


class DuplicateTaskError(Exception):
    """Raised when the same stock is already being analyzed."""

    def __init__(self, stock_code: str, existing_task_id: str):
        self.stock_code = stock_code
        self.existing_task_id = existing_task_id
        super().__init__(f"股票 {stock_code} 正在分析中 (task_id: {existing_task_id})")


class AnalysisTaskQueue:
    """Singleton async analysis task queue."""

    _instance: Optional[AnalysisTaskQueue] = None
    _instance_lock = threading.Lock()

    def __new__(cls, *args: Any, **kwargs: Any) -> AnalysisTaskQueue:
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, max_workers: int = 3):
        if hasattr(self, "_initialized") and self._initialized:
            return

        self._max_workers = max_workers
        self._executor: Optional[ThreadPoolExecutor] = None

        self._tasks: Dict[str, TaskInfo] = {}
        self._analyzing_stocks: Dict[str, str] = {}
        self._futures: Dict[str, Future] = {}

        self._subscribers: List[AsyncQueue] = []
        self._subscribers_lock = threading.Lock()

        self._main_loop: Optional[asyncio.AbstractEventLoop] = None

        self._data_lock = threading.RLock()

        self._max_history = 100

        self._initialized = True
        logger.info(f"[TaskQueue] 初始化完成，最大并发: {max_workers}")

    @property
    def executor(self) -> ThreadPoolExecutor:
        if self._executor is None:
            self._executor = ThreadPoolExecutor(
                max_workers=self._max_workers,
                thread_name_prefix="analysis_task_",
            )
        return self._executor

    @property
    def max_workers(self) -> int:
        return self._max_workers

    def _has_inflight_tasks_locked(self) -> bool:
        if self._analyzing_stocks:
            return True
        return any(
            task.status in (TaskStatus.PENDING, TaskStatus.PROCESSING) for task in self._tasks.values()
        )

    def sync_max_workers(
        self,
        max_workers: int,
        *,
        log: bool = True,
    ) -> Literal["applied", "unchanged", "deferred_busy"]:
        try:
            target = max(1, int(max_workers))
        except (TypeError, ValueError):
            if log:
                logger.warning("[TaskQueue] 忽略非法 MAX_WORKERS 值: {!r}", max_workers)
            return "unchanged"

        executor_to_shutdown: Optional[ThreadPoolExecutor] = None
        previous: int
        with self._data_lock:
            previous = self._max_workers
            if target == previous:
                return "unchanged"

            if self._has_inflight_tasks_locked():
                if log:
                    logger.info(
                        "[TaskQueue] 最大并发调整延后: 当前繁忙 ({} -> {})",
                        previous,
                        target,
                    )
                return "deferred_busy"

            self._max_workers = target
            executor_to_shutdown = self._executor
            self._executor = None

        if executor_to_shutdown is not None:
            executor_to_shutdown.shutdown(wait=False)

        if log:
            logger.info("[TaskQueue] 最大并发已更新: {} -> {}", previous, target)
        return "applied"

    def is_analyzing(self, stock_code: str) -> bool:
        dedupe_key = _dedupe_stock_code_key(stock_code)
        with self._data_lock:
            return dedupe_key in self._analyzing_stocks

    def get_analyzing_task_id(self, stock_code: str) -> Optional[str]:
        dedupe_key = _dedupe_stock_code_key(stock_code)
        with self._data_lock:
            return self._analyzing_stocks.get(dedupe_key)

    def submit_task(
        self,
        stock_code: str,
        stock_name: Optional[str] = None,
        original_query: Optional[str] = None,
        force_refresh: bool = False,
        user_id: Optional[int] = None,
    ) -> TaskInfo:
        stock_code = normalize_instrument(stock_code.strip())
        if not stock_code:
            raise ValueError("股票代码不能为空或仅包含空白字符")

        accepted, duplicates = self.submit_tasks_batch(
            [stock_code],
            stock_name=stock_name,
            original_query=original_query,
            force_refresh=force_refresh,
            user_id=user_id,
        )
        if duplicates:
            raise duplicates[0]
        return accepted[0]

    def submit_tasks_batch(
        self,
        stock_codes: List[str],
        stock_name: Optional[str] = None,
        original_query: Optional[str] = None,
        force_refresh: bool = False,
        notify: bool = True,
        user_id: Optional[int] = None,
        upsert: bool = False,
    ) -> Tuple[List[TaskInfo], List[DuplicateTaskError]]:
        accepted: List[TaskInfo] = []
        duplicates: List[DuplicateTaskError] = []
        created_task_ids: List[str] = []

        canonical_codes: List[str] = []
        for code in stock_codes:
            norm = normalize_instrument(code.strip())
            if norm:
                canonical_codes.append(norm)

        with self._data_lock:
            for stock_code in canonical_codes:
                dedupe_key = _dedupe_stock_code_key(stock_code)
                if dedupe_key in self._analyzing_stocks:
                    existing_task_id = self._analyzing_stocks[dedupe_key]
                    duplicates.append(DuplicateTaskError(stock_code, existing_task_id))
                    continue

                task_id = uuid.uuid4().hex
                task_info = TaskInfo(
                    task_id=task_id,
                    stock_code=stock_code,
                    stock_name=stock_name,
                    status=TaskStatus.PENDING,
                    message="任务已加入队列",
                    original_query=original_query,
                    user_id=user_id,
                )
                self._tasks[task_id] = task_info
                self._analyzing_stocks[dedupe_key] = task_id

                try:
                    future = self.executor.submit(
                        self._execute_task,
                        task_id,
                        stock_code,
                        force_refresh,
                        notify,
                        user_id,
                        upsert,
                    )
                except Exception:
                    self._rollback_submitted_tasks_locked(created_task_ids + [task_id])
                    raise

                self._futures[task_id] = future
                accepted.append(task_info)
                created_task_ids.append(task_id)
                logger.info(f"[TaskQueue] 任务已提交: {stock_code} -> {task_id}")

            for task_info in accepted:
                self._broadcast_event("task_created", task_info.to_dict())

        return accepted, duplicates

    def _rollback_submitted_tasks_locked(self, task_ids: List[str]) -> None:
        for task_id in task_ids:
            future = self._futures.pop(task_id, None)
            if future is not None:
                future.cancel()

            task = self._tasks.pop(task_id, None)
            if task:
                dedupe_key = _dedupe_stock_code_key(task.stock_code)
                if self._analyzing_stocks.get(dedupe_key) == task_id:
                    del self._analyzing_stocks[dedupe_key]

    def get_task(self, task_id: str) -> Optional[TaskInfo]:
        with self._data_lock:
            task = self._tasks.get(task_id)
            return task.copy() if task else None

    def list_pending_tasks(self) -> List[TaskInfo]:
        with self._data_lock:
            return [
                task.copy()
                for task in self._tasks.values()
                if task.status in (TaskStatus.PENDING, TaskStatus.PROCESSING)
            ]

    def list_all_tasks(self, limit: int = 50) -> List[TaskInfo]:
        with self._data_lock:
            tasks = sorted(self._tasks.values(), key=lambda t: t.created_at, reverse=True)
            return [t.copy() for t in tasks[:limit]]

    def get_task_stats(self) -> Dict[str, int]:
        with self._data_lock:
            stats = {
                "total": len(self._tasks),
                "pending": 0,
                "processing": 0,
                "completed": 0,
                "failed": 0,
            }
            for task in self._tasks.values():
                stats[task.status.value] = stats.get(task.status.value, 0) + 1
            return stats

    def update_task_progress(
        self,
        task_id: str,
        progress: int,
        message: Optional[str] = None,
        *,
        event_type: str = "task_progress",
    ) -> Optional[TaskInfo]:
        with self._data_lock:
            task = self._tasks.get(task_id)
            if not task or task.status not in (TaskStatus.PENDING, TaskStatus.PROCESSING):
                return None

            next_progress = max(task.progress, max(0, min(99, int(progress))))
            changed = False
            if next_progress != task.progress:
                task.progress = next_progress
                changed = True
            if message is not None and message != task.message:
                task.message = message
                changed = True

            if not changed:
                return task.copy()

            task_snapshot = task.copy()

        self._broadcast_event(event_type, task_snapshot.to_dict())
        return task_snapshot

    def _execute_task(
        self,
        task_id: str,
        stock_code: str,
        force_refresh: bool,
        notify: bool = True,
        user_id: Optional[int] = None,
        upsert: bool = False,
    ) -> Optional[Dict[str, Any]]:
        _ = force_refresh
        _ = notify

        with self._data_lock:
            task = self._tasks.get(task_id)
            if not task:
                return None
            task.status = TaskStatus.PROCESSING
            task.started_at = datetime.now()
            task.message = "正在分析中..."
            task.progress = 10

        self._broadcast_event("task_started", task.to_dict())

        try:
            from stopat30m.analysis.pipeline import analyze_stock

            def _on_progress(step: int, total: int, message: str) -> None:
                pct = 10 + int((step / max(total, 1)) * 85)
                self.update_task_progress(task_id, pct, message)

            analysis_result = analyze_stock(stock_code, on_progress=_on_progress, user_id=user_id, upsert=upsert)

            if analysis_result:
                with self._data_lock:
                    t = self._tasks.get(task_id)
                    if t:
                        t.status = TaskStatus.COMPLETED
                        t.progress = 100
                        t.completed_at = datetime.now()
                        t.result = analysis_result.model_dump()
                        t.message = "分析完成"
                        t.stock_name = t.stock_name or analysis_result.name
                        dedupe_key = _dedupe_stock_code_key(t.stock_code)
                        if dedupe_key in self._analyzing_stocks:
                            del self._analyzing_stocks[dedupe_key]
                        snap = t.to_dict()
                    else:
                        snap = {}

                if snap:
                    self._broadcast_event("task_completed", snap)
                logger.info(f"[TaskQueue] 任务完成: {task_id} ({stock_code})")
                self._cleanup_old_tasks()
                return analysis_result.model_dump()

            raise RuntimeError("分析返回空结果")

        except Exception as e:
            error_msg = str(e)
            logger.error(f"[TaskQueue] 任务失败: {task_id} ({stock_code}), 错误: {error_msg}")

            snap: Dict[str, Any] = {}
            with self._data_lock:
                t = self._tasks.get(task_id)
                if t:
                    t.status = TaskStatus.FAILED
                    t.completed_at = datetime.now()
                    t.error = error_msg[:200]
                    t.message = f"分析失败: {error_msg[:50]}"
                    dedupe_key = _dedupe_stock_code_key(t.stock_code)
                    if dedupe_key in self._analyzing_stocks:
                        del self._analyzing_stocks[dedupe_key]
                    snap = t.to_dict()

            if snap:
                self._broadcast_event("task_failed", snap)
            self._cleanup_old_tasks()
            return None

    def _cleanup_old_tasks(self) -> int:
        with self._data_lock:
            if len(self._tasks) <= self._max_history:
                return 0

            completed_tasks = sorted(
                [t for t in self._tasks.values() if t.status in (TaskStatus.COMPLETED, TaskStatus.FAILED)],
                key=lambda t: t.created_at,
            )

            to_remove = len(self._tasks) - self._max_history
            removed = 0

            for task in completed_tasks[:to_remove]:
                del self._tasks[task.task_id]
                if task.task_id in self._futures:
                    del self._futures[task.task_id]
                removed += 1

            if removed > 0:
                logger.debug(f"[TaskQueue] 清理了 {removed} 个过期任务")

            return removed

    def subscribe(self, queue: AsyncQueue) -> None:
        with self._subscribers_lock:
            self._subscribers.append(queue)
            try:
                self._main_loop = asyncio.get_running_loop()
            except RuntimeError:
                try:
                    self._main_loop = asyncio.get_event_loop()
                except RuntimeError:
                    pass
            logger.debug(f"[TaskQueue] 新订阅者加入，当前订阅者数: {len(self._subscribers)}")

    def unsubscribe(self, queue: AsyncQueue) -> None:
        with self._subscribers_lock:
            if queue in self._subscribers:
                self._subscribers.remove(queue)
                logger.debug(f"[TaskQueue] 订阅者离开，当前订阅者数: {len(self._subscribers)}")

    def _broadcast_event(self, event_type: str, data: Dict[str, Any]) -> None:
        event = {"type": event_type, "data": data}

        with self._subscribers_lock:
            subscribers = self._subscribers.copy()
            loop = self._main_loop

        if not subscribers:
            return

        if loop is None:
            logger.warning("[TaskQueue] 无法广播事件：主事件循环未设置")
            return

        for queue in subscribers:
            try:
                loop.call_soon_threadsafe(queue.put_nowait, event)
            except RuntimeError as e:
                logger.debug(f"[TaskQueue] 广播事件跳过（循环已关闭）: {e}")
            except Exception as e:
                logger.warning(f"[TaskQueue] 广播事件失败: {e}")

    def shutdown(self) -> None:
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None
            logger.info("[TaskQueue] 线程池已关闭")


def get_task_queue() -> AnalysisTaskQueue:
    queue = AnalysisTaskQueue()
    try:
        from stopat30m.config import get as cfg_get

        target_workers = max(1, int(cfg_get("analysis", "max_workers", queue.max_workers)))
        queue.sync_max_workers(target_workers, log=False)
    except Exception as exc:
        logger.debug("[TaskQueue] 读取 max_workers 失败，使用当前并发设置: {}", exc)

    return queue
