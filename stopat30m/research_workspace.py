"""Admin workspace over the existing immutable CLI research artifacts.

Only the registered, hash-locked protocols are executable. Background tasks
share a nonblocking OS lock and persist their actual outcome across page loads
and process restarts. This module never invents a successful research result.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import re
import threading
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
REGISTRY = {
    "eastmoney_etf": ("m2_etf_eastmoney_protocol.json", "m2_etf_eastmoney_protocol.universe.json", "固定 ETF 主协议"),
    "eastmoney_etf_cost10": ("m2_etf_eastmoney_cost_10bps_protocol.json", "m2_etf_eastmoney_protocol.universe.json", "成本敏感性：每边 10bp"),
    "eastmoney_etf_cost20": ("m2_etf_eastmoney_cost_20bps_protocol.json", "m2_etf_eastmoney_protocol.universe.json", "成本敏感性：每边 20bp"),
    "baostock_etf": ("m2_etf_protocol.json", "m2_etf_protocol.universe.json", "原 BaoStock 协议（只读）"),
}
_SNAPSHOT = re.compile(r"[0-9a-f]{64}\Z")
_RUN = re.compile(r"oos_[A-Za-z0-9_-]{1,100}\Z")
_JOB = re.compile(r"[0-9a-f]{32}\Z")
_OUTPUTS = {"returns.csv", "folds.csv", "positions.csv", "trades.csv", "signals.csv", "report.json", "protocol.json"}
_STRATEGIES = {"momentum", "equal_weight", "buy_hold"}
_SPLITS = {"train", "validation", "final_oos"}


class WorkspaceError(ValueError):
    def __init__(self, message, status_code=409):
        super().__init__(message)
        self.status_code = status_code


def clean_json(value):
    """JSON-safe nullable numbers, including NumPy/Pandas output values."""
    if isinstance(value, dict):
        return {str(key): clean_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean_json(item) for item in value]
    if isinstance(value, np.generic):
        return clean_json(value.item())
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if value is pd.NA or value is pd.NaT:
        return None
    return value


def _now():
    return datetime.now(timezone.utc).isoformat()


def _sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _read(path):
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise WorkspaceError(f"Expected JSON object: {path.name}")
    return value


def _atomic(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(json.dumps(clean_json(value), ensure_ascii=False, allow_nan=False, indent=2) + "\n", encoding="utf-8")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _guard(path: Path, base: Path):
    path, base = Path(os.path.abspath(path)), Path(os.path.abspath(base))
    if not path.is_relative_to(base):
        raise WorkspaceError("Research path escapes the configured workspace.")
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        if current.is_symlink():
            raise WorkspaceError(f"Symlinks are not accepted in research artifacts: {current.name}")
    return path


class ResearchWorkspace:
    def __init__(self, root=None, protocol_root=None, *, registry=None,
                 download=None, run=None, provider_factory=None):
        self.root = Path(root or PROJECT_ROOT / "output/research").expanduser().resolve()
        self.protocol_root = Path(protocol_root or PROJECT_ROOT / "docs/research").expanduser().resolve()
        self.registry = dict(REGISTRY if registry is None else registry)
        self.data_root = self.root / "data"
        self.snapshot_root = self.data_root / "snapshots"
        self.run_root = self.root / "runs"
        self.job_root = self.root / "jobs"
        self._download, self._run, self._provider_factory = download, run, provider_factory
        self._threads = {}
        self._thread_guard = threading.Lock()
        _guard(self.job_root, self.root).mkdir(parents=True, exist_ok=True)
        self._recover_interrupted()

    def _task_lock(self):
        path = _guard(self.root / ".task.lock", self.root)
        handle = path.open("a+b")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            handle.close()
            raise WorkspaceError("已有研究任务正在运行，请等待完成。", 409) from exc
        return handle

    @staticmethod
    def _unlock(handle):
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()

    def _recover_interrupted(self):
        try:
            handle = self._task_lock()
        except WorkspaceError:
            return  # Another live process still owns its running task.
        try:
            for path in self.job_root.glob("*.json"):
                try:
                    job = self._job(path.stem)
                except (WorkspaceError, OSError, ValueError):
                    # Preserve the unreadable evidence. A damaged old record
                    # must not block unrelated protocols, artifacts or jobs.
                    continue
                if job["status"] == "running":
                    job.update(status="interrupted", completed_at=_now(), progress="进程中断，任务未完成。",
                               error="Research worker exited before recording a final outcome; submit a new job to resume safely.")
                    _atomic(path, job)
        finally:
            self._unlock(handle)

    def _protocol(self, protocol_id):
        from stopat30m.backtest.walk_forward import load_protocol
        from stopat30m.data.historical_universe import HistoricalUniverse
        if protocol_id not in self.registry:
            raise WorkspaceError("Unknown registered research protocol.", 400)
        filename, universe_name, label = self.registry[protocol_id]
        path = _guard(self.protocol_root / filename, self.protocol_root)
        companion = _guard(self.protocol_root / universe_name, self.protocol_root)
        try:
            protocol = load_protocol(path)
            universe = HistoricalUniverse.load(companion)
            if universe.as_dict() != protocol["universe"]:
                raise WorkspaceError("Registered protocol differs from its bound universe.")
            if protocol_id.startswith("eastmoney_etf_cost"):
                main = load_protocol(_guard(self.protocol_root / self.registry["eastmoney_etf"][0], self.protocol_root))
                if protocol["universe_sha256"] != main["universe_sha256"]:
                    raise WorkspaceError("Cost sensitivity protocol must bind the main universe.")
            request = protocol["universe"]["metadata"]["research_data_request"]
            if request["source"] not in {"eastmoney", "baostock"}:
                raise WorkspaceError("Unsupported registered research source.")
            return path, protocol, universe, {"id": protocol_id, "label": label, "source": request["source"],
                "calendar_source": request.get("calendar_source", ""),
                "request": {key: request[key] for key in ("instruments", "start_date", "end_date", "adjustment")},
                "config": protocol["config"], "universe_kind": universe.kind,
                "protocol_sha256": protocol["protocol_sha256"],
                "limitations": list(dict.fromkeys([*protocol["universe"].get("limitations", []),
                    "历史样本外属于回看研究；规则于2026年锁定，不能视为前瞻实盘业绩。",
                    "复权价格仅用于分数敞口研究，不支持整数股现金账户撮合。",
                    "固定池有选池偏差；供应商历史复权版本未获点时验证。", "基准是价格指数，不含分红。"])),
            }
        except (OSError, KeyError, TypeError, ValueError) as exc:
            if isinstance(exc, WorkspaceError):
                raise
            raise WorkspaceError(f"Registered protocol is invalid: {protocol_id}: {exc}") from exc

    def protocols(self):
        return [self._protocol(key)[3] for key in self.registry
                if key != "baostock_etf" or (self.protocol_root / self.registry[key][0]).exists()]

    def _snapshot(self, snapshot_id):
        from stopat30m.data.research_prices import load_research_prices
        if not _SNAPSHOT.fullmatch(str(snapshot_id)):
            raise WorkspaceError("Invalid snapshot ID.", 400)
        path = _guard(self.snapshot_root / snapshot_id, self.root)
        if not path.exists():
            raise WorkspaceError("Research snapshot not found.", 404)
        for name in ("manifest.json", "prices.csv", "calendar.csv"):
            _guard(path / name, self.root)
        try:
            artifact = load_research_prices(path)
            if artifact.manifest["artifact_id"] != snapshot_id:
                raise WorkspaceError("Snapshot directory and content identity differ.")
            if artifact.manifest["provider"]["name"] not in {"eastmoney", "baostock"}:
                raise WorkspaceError("Snapshot has an unsupported price source.")
            if set(artifact.manifest["files"]) != {"prices.csv", "calendar.csv"}:
                raise WorkspaceError("Snapshot has an unexpected file checksum set.")
            for filename in artifact.manifest["files"]:
                if Path(filename).name != filename:
                    raise WorkspaceError("Invalid snapshot file reference.")
                _guard(path / filename, self.root)
            return artifact
        except (OSError, KeyError, TypeError, ValueError) as exc:
            if isinstance(exc, WorkspaceError):
                raise
            raise WorkspaceError(f"Snapshot validation failed: {snapshot_id}: {exc}") from exc

    def _compatible(self, artifact, protocol_id, *, coverage=False):
        _, protocol, _, _ = self._protocol(protocol_id)
        from stopat30m.backtest.walk_forward import ResearchConfig, _validate_source
        config = ResearchConfig(**protocol["config"])
        _validate_source(artifact, protocol["universe"], config)
        if coverage:
            days = artifact.calendar
            first = int(days.searchsorted(pd.Timestamp(config.train_start)))
            if first < config.lookback_sessions:
                raise WorkspaceError("Snapshot does not cover the locked training warmup.")
            required = days[max(0, first - config.lookback_sessions):]
            required = required[required <= config.oos_end]
            instruments = sorted(protocol["universe"]["metadata"]["research_data_request"]["instruments"])
            for label, frame in (("open", artifact.adjusted_open), ("close", artifact.adjusted_close)):
                values = frame.reindex(index=required, columns=instruments).to_numpy(dtype=float)
                if not np.isfinite(values).all() or (values <= 0).any():
                    raise WorkspaceError(f"Snapshot has missing/invalid required research {label} prices; no filling is allowed.")
        return protocol

    def snapshots(self):
        if not self.snapshot_root.exists():
            return []
        _guard(self.snapshot_root, self.root)
        protocols = [item["id"] for item in self.protocols()]
        result = []
        for path in sorted(self.snapshot_root.iterdir()):
            if path.name.startswith("."):
                continue
            item = {"id": path.name, "created_at": "", "source": "", "start_date": "", "end_date": "",
                "adjustment": "", "instruments": [], "session_count": 0, "compatible_protocols": [],
                "coverage": {}, "execution_ready": False, "validation_status": "invalid", "validation_error": None}
            try:
                artifact = self._snapshot(path.name)
                manifest, request = artifact.manifest, artifact.manifest["request"]
                compatible = []
                for protocol_id in protocols:
                    if protocol_id == "baostock_etf":
                        continue  # Preserved for audit/display only after the source-quality incident.
                    try:
                        self._compatible(artifact, protocol_id, coverage=True)
                        compatible.append(protocol_id)
                    except (ValueError, KeyError, OSError):
                        continue  # Incompatibility is represented by the explicit empty compatibility list.
                item.update(created_at=manifest["created_at"], source=manifest["provider"]["name"],
                    **{key: request[key] for key in ("start_date", "end_date", "adjustment", "instruments")},
                    session_count=len(artifact.calendar), compatible_protocols=compatible, coverage=manifest["coverage"],
                    validation_status="valid")
            except (ValueError, OSError, KeyError) as exc:
                item["validation_error"] = str(exc)
            result.append(item)
        return sorted(result, key=lambda item: (item["created_at"], item["id"]), reverse=True)

    def _verified_run(self, run_id):
        from stopat30m.backtest.walk_forward import load_protocol, _digest, _frame_bytes, _status_map
        if not _RUN.fullmatch(str(run_id)):
            raise WorkspaceError("Invalid research run ID.", 400)
        path = _guard(self.run_root / run_id, self.root)
        if not path.exists():
            raise WorkspaceError("Research run not found.", 404)
        try:
            manifest = _read(_guard(path / "manifest.json", self.root))
            checksums = manifest["output_checksums"]
            if set(checksums) != _OUTPUTS:
                raise WorkspaceError("Research run is missing its required output checksum set.")
            for filename, expected in checksums.items():
                file = _guard(path / filename, self.root)
                if _sha(file) != expected:
                    raise WorkspaceError(f"Research output checksum mismatch: {filename}")
            protocol = load_protocol(path / "protocol.json")
            report = _read(path / "report.json")
            if manifest["protocol_sha256"] != protocol["protocol_sha256"] or report["protocol_sha256"] != protocol["protocol_sha256"]:
                raise WorkspaceError("Run protocol references do not agree.")
            if manifest["config"] != protocol["config"] or manifest["universe_sha256"] != protocol["universe_sha256"]:
                raise WorkspaceError("Run rules differ from the locked protocol.")
            snapshot_id = manifest["source_manifest"]["artifact_id"]
            artifact = self._snapshot(snapshot_id)
            if (manifest["source_manifest"] != artifact.manifest or manifest["source_manifest_sha256"] != artifact.manifest_sha256
                    or Path(manifest["source_snapshot"]).absolute() != artifact.path):
                raise WorkspaceError("Run input snapshot/source path does not match its verified artifact.")
            from stopat30m.backtest.walk_forward import ResearchConfig, _validate_source
            _validate_source(artifact, protocol["universe"], ResearchConfig(**protocol["config"]))
            statuses = _status_map(artifact)
            inputs = {"adjusted_open": hashlib.sha256(_frame_bytes(artifact.adjusted_open)).hexdigest(),
                "adjusted_close": hashlib.sha256(_frame_bytes(artifact.adjusted_close)).hexdigest(),
                "calendar": _digest([day.date().isoformat() for day in artifact.calendar]),
                "statuses": _digest([[str(key[0].date()), key[1], None if pd.isna(value) else str(value)] for key, value in sorted(statuses.items())])}
            if inputs != manifest["input_checksums"]:
                raise WorkspaceError("Run input checksums do not match the saved price snapshot.")
            fingerprint = _digest({"inputs": inputs, "source_manifest": artifact.manifest,
                "protocol": protocol["protocol_sha256"], "code": manifest["code_checksums"], "environment": manifest["environment"]})
            if (manifest["deterministic_fingerprint"] != fingerprint or manifest["run_id"] != fingerprint[:24]
                    or report["deterministic_fingerprint"] != fingerprint or report.get("execution_ready") is not False):
                raise WorkspaceError("Run fingerprint or research-only declaration is invalid.")
            return path, manifest, report
        except (OSError, KeyError, TypeError, ValueError) as exc:
            if isinstance(exc, WorkspaceError):
                raise
            raise WorkspaceError(f"Research run validation failed: {run_id}: {exc}") from exc

    def _protocol_id(self, checksum):
        return next((item["id"] for item in self.protocols() if item["protocol_sha256"] == checksum), None)

    def runs(self):
        if not self.run_root.exists():
            return []
        _guard(self.run_root, self.root)
        result = []
        for path in sorted(self.run_root.iterdir()):
            if path.name.startswith("."):
                continue
            item = {"id": path.name, "generated_at": "", "protocol_id": None, "source": "", "snapshot_id": "",
                "metrics": {}, "historical_oos_is_retrospective": True, "execution_ready": False,
                "validation_status": "invalid", "validation_error": None}
            try:
                _, manifest, report = self._verified_run(path.name)
                item.update(generated_at=manifest["generated_at"], protocol_id=self._protocol_id(manifest["protocol_sha256"]),
                    source=manifest["source_manifest"]["provider"]["name"], snapshot_id=manifest["source_manifest"]["artifact_id"],
                    metrics=report["splits"]["final_oos"], validation_status="valid")
            except (ValueError, OSError, KeyError) as exc:
                item["validation_error"] = str(exc)
            result.append(item)
        return clean_json(sorted(result, key=lambda item: (item["generated_at"], item["id"]), reverse=True))

    def run_detail(self, run_id):
        path, manifest, report = self._verified_run(run_id)
        frame = pd.read_csv(path / "returns.csv")
        frame = frame.loc[frame["split"].eq("final_oos")]
        chart = pd.DataFrame()
        for strategy in ("momentum", "equal_weight", "buy_hold"):
            rows = frame.loc[frame["strategy"].eq(strategy)].sort_values("date").set_index("date")
            if rows.empty or rows.index.duplicated().any():
                raise WorkspaceError("Run final OOS curve is missing or duplicated.")
            anchor = str(rows["signal_date"].iloc[0])
            if anchor >= str(rows.index[0]):
                raise WorkspaceError("Final OOS initial signal date must precede execution.")
            nav = pd.concat([pd.Series([1.0], index=[anchor]), pd.to_numeric(rows["nav"], errors="coerce")])
            chart[strategy] = nav
            chart[f"{strategy}_drawdown"] = nav / nav.cummax().clip(lower=1.0) - 1
            if strategy == "momentum":
                benchmark = pd.concat([pd.Series([1.0], index=[anchor]), pd.to_numeric(rows["benchmark_nav"], errors="coerce")])
                chart["benchmark"] = benchmark
                chart["benchmark_drawdown"] = benchmark / benchmark.cummax().clip(lower=1.0) - 1
        chart.index.name = "date"
        return clean_json({"id": run_id, "manifest": manifest, "report": report, "config": manifest["config"],
            "chart": chart.sort_index().reset_index().to_dict(orient="records")})

    def table(self, run_id, name, *, offset=0, limit=50, split="final_oos", strategy="momentum"):
        if name not in {"trades", "positions", "signals", "folds"} or split not in _SPLITS or strategy not in _STRATEGIES:
            raise WorkspaceError("Invalid research table/filter.", 400)
        if not isinstance(offset, int) or offset < 0 or not isinstance(limit, int) or not 1 <= limit <= 200:
            raise WorkspaceError("Invalid research pagination.", 400)
        path, _, _ = self._verified_run(run_id)
        frame = pd.read_csv(path / f"{name}.csv")
        if "split" in frame:
            frame = frame.loc[frame["split"].eq(split)]
        elif name == "folds" and split != "final_oos":
            frame = frame.iloc[:0]
        if "strategy" in frame:
            frame = frame.loc[frame["strategy"].eq(strategy)]
        return clean_json({"rows": frame.iloc[offset:offset + limit].to_dict(orient="records"),
            "total": len(frame), "offset": offset, "limit": limit})

    def _job(self, job_id):
        if not _JOB.fullmatch(str(job_id)):
            raise WorkspaceError("Invalid research job ID.", 400)
        path = _guard(self.job_root / f"{job_id}.json", self.root)
        if not path.exists():
            raise WorkspaceError("Research job not found.", 404)
        try:
            job = _read(path)
            required = {"id", "kind", "status", "protocol_id", "snapshot_id", "run_id", "started_at", "completed_at", "progress", "error"}
            if (not required.issubset(job) or job.get("id") != job_id
                    or job.get("kind") not in {"download", "run"}
                    or job.get("status") not in {"running", "completed", "failed", "interrupted"}
                    or not all(isinstance(job.get(key), str) for key in ("protocol_id", "started_at", "progress"))
                    or not job["started_at"]
                    or any(job.get(key) is not None and not isinstance(job[key], str)
                           for key in ("snapshot_id", "run_id", "completed_at", "error"))):
                raise WorkspaceError("Invalid persisted research job.")
            return job
        except (OSError, ValueError) as exc:
            if isinstance(exc, WorkspaceError):
                raise
            raise WorkspaceError(f"Cannot read persisted research job: {job_id}: {type(exc).__name__}: {exc}") from exc

    def jobs(self):
        self._recover_interrupted()
        result = []
        for path in self.job_root.glob("*.json"):
            try:
                result.append(self._job(path.stem))
            except (WorkspaceError, OSError, ValueError) as exc:
                result.append({"id": path.stem, "kind": "unknown", "status": "invalid", "protocol_id": "",
                    "snapshot_id": None, "run_id": None, "started_at": "", "completed_at": None,
                    "progress": "任务记录无法读取，状态未知。", "error": str(exc)})
        return sorted(result, key=lambda job: (job["status"] == "invalid", job["started_at"], job["id"]), reverse=True)[:20]

    def job(self, job_id):
        self._recover_interrupted()
        return self._job(job_id)

    def _update(self, job_id, **changes):
        job = self._job(job_id)
        job.update(changes)
        _atomic(_guard(self.job_root / f"{job_id}.json", self.root), job)
        return job

    def start_download(self, protocol_id="eastmoney_etf", *, refresh=False, min_interval=3, max_attempts=4, request_timeout=15):
        if protocol_id != "eastmoney_etf":
            raise WorkspaceError("Downloads require the registered main Eastmoney protocol.", 400)
        self._protocol(protocol_id)
        if (not math.isfinite(min_interval) or not 2 <= min_interval <= 60 or not isinstance(max_attempts, int)
                or not 1 <= max_attempts <= 6 or not math.isfinite(request_timeout) or not 1 <= request_timeout <= 60):
            raise WorkspaceError("Invalid bounded research download policy.", 400)
        return self._start("download", protocol_id, None, {"refresh": refresh, "min_interval": min_interval,
            "max_attempts": max_attempts, "request_timeout": request_timeout})

    def start_run(self, protocol_id, snapshot_id):
        if protocol_id == "baostock_etf":
            raise WorkspaceError("The original BaoStock protocol is available for audit only; execute the amended Eastmoney protocol.", 400)
        artifact = self._snapshot(snapshot_id)
        try:
            self._compatible(artifact, protocol_id, coverage=True)
        except (ValueError, OSError, KeyError) as exc:
            raise WorkspaceError(f"Snapshot is incompatible with the registered protocol: {exc}") from exc
        return self._start("run", protocol_id, snapshot_id, {})

    def _start(self, kind, protocol_id, snapshot_id, options):
        handle = self._task_lock()
        job_id = uuid4().hex
        job = {"id": job_id, "kind": kind, "status": "running", "protocol_id": protocol_id,
            "snapshot_id": snapshot_id, "run_id": None, "started_at": _now(), "completed_at": None,
            "progress": "任务已启动。", "error": None}
        try:
            _atomic(_guard(self.job_root / f"{job_id}.json", self.root), job)
            thread = threading.Thread(target=self._worker, args=(job_id, handle, options), daemon=True,
                                      name=f"research-{job_id[:8]}")
            with self._thread_guard:
                self._threads[job_id] = thread
            thread.start()
        except Exception:
            self._unlock(handle)
            self._update(job_id, status="failed", completed_at=_now(), error="Unable to start research worker.")
            raise
        return job

    def _worker(self, job_id, handle, options):
        try:
            job = self._job(job_id)
            protocol_path, protocol, universe, _ = self._protocol(job["protocol_id"])
            if job["kind"] == "download":
                from stopat30m.data.research_prices import EastmoneyResearchProvider, download_research_prices
                request = protocol["universe"]["metadata"]["research_data_request"]
                provider = (self._provider_factory or EastmoneyResearchProvider)(cache_dir=self.data_root / "download-cache",
                    refresh=options["refresh"], min_interval=options["min_interval"], max_attempts=options["max_attempts"],
                    timeout=options["request_timeout"])
                try:
                    artifact = (self._download or download_research_prices)(request["instruments"], request["start_date"], request["end_date"],
                        self.data_root, adjustment=request["adjustment"], refresh=options["refresh"], provider=provider,
                        progress=lambda message: self._update(job_id, progress=str(message)[:1000]))
                finally:
                    provider.close()
                verified = self._snapshot(artifact.manifest["artifact_id"])
                self._compatible(verified, job["protocol_id"])
                self._update(job_id, snapshot_id=verified.manifest["artifact_id"])
            else:
                from stopat30m.backtest.walk_forward import run_walk_forward
                artifact = self._snapshot(job["snapshot_id"])
                self._compatible(artifact, job["protocol_id"], coverage=True)
                run_id = f"oos_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"
                output = _guard(self.run_root / run_id, self.root)
                self._update(job_id, progress="正在执行锁定规则的历史样本外研究。")
                (self._run or run_walk_forward)(artifact, universe, protocol_path, output)
                self._verified_run(run_id)
                self._update(job_id, run_id=run_id)
            self._update(job_id, status="completed", completed_at=_now(), progress="任务完成，产物校验通过。")
        except Exception as exc:
            try:
                self._update(job_id, status="failed", completed_at=_now(), progress="任务失败。", error=f"{type(exc).__name__}: {exc}"[:2000])
            except (WorkspaceError, OSError, ValueError):
                # Never replace a corrupted state record with a fabricated
                # outcome. The listing exposes it as invalid; unlock below.
                pass
        finally:
            self._unlock(handle)

    def wait(self, job_id, timeout=10):
        """Local tests/CLI embedding may join a worker; HTTP polling never waits."""
        with self._thread_guard:
            thread = self._threads.get(job_id)
        if thread:
            thread.join(timeout)
        return self.job(job_id)
