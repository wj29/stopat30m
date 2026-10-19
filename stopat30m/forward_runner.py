"""Daily source adapter for the separate, prospective strategy experiment.

No broker credentials, real account database or order API are used. A local
scheduler calls ``run_daily`` after close; each successful signal is frozen
before the next session whose completed bars can acknowledge paper exposure.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import fcntl
import hashlib
import json
import math
from pathlib import Path
import shutil
import tempfile
from zoneinfo import ZoneInfo

import pandas as pd

from stopat30m.backtest.model_comparison import load_frozen_model
from stopat30m.backtest.walk_forward import ResearchConfig
from stopat30m.data.research_calendar import Calendar, download_calendar
from stopat30m.data.research_prices import download_research_prices, load_research_prices
from stopat30m.trading.forward_paper import (
    ForwardPaperConfig, create_forward_paper, forward_paper_status,
    record_close_signal, settle_completed_session,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")


def _hash(payload):
    return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False,
                                    separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".daily-", delete=False) as stream:
        temporary = Path(stream.name)
        stream.write((json.dumps(value, sort_keys=True, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode())
    temporary.replace(path)


@contextmanager
def _daily_lock(root):
    with (root / ".daily.lock").open("a+b") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("A daily forward update is already running") from None
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class CachedCalendarProvider:
    """Reuse source-backed calendar and security identities, never weekday flags."""
    source = "baostock"
    calendar_source = "baostock.query_trade_dates"
    calendar_source_url = "https://www.baostock.com/"

    def __init__(self, calendar, instruments):
        self.calendar, self.instruments = calendar, instruments
        self.calendar_source, self.calendar_source_url = calendar.source, calendar.source_url

    def fetch_calendar(self, start, end):
        self.calendar._require_coverage(start, end)
        return pd.DataFrame([{"date": day, "is_trading_day": int(flag)}
                             for day, flag in self.calendar.days if start <= day <= end])

    def instrument_info(self, instrument):
        if instrument not in self.instruments:
            raise ValueError("Frozen source metadata does not identify this instrument")
        return json.loads(json.dumps(self.instruments[instrument]))


def initialize_forward(experiment_dir, paper_dir, calendar_path, *, initial_capital=100000, now=None,
                       model_artifact=None):
    """Copy the verified model and freeze a distinct forward period, without P&L."""
    instant = now or datetime.now(timezone.utc)
    target = Path(paper_dir).resolve()
    if (experiment_dir is None) == (model_artifact is None):
        raise ValueError("Select exactly one verified comparison experiment or forward-only model artifact")
    if experiment_dir is not None:
        experiment = Path(experiment_dir).resolve()
        experiment_manifest = json.loads((experiment / "experiment_manifest.json").read_text())
        for filename, expected in experiment_manifest["output_checksums"].items():
            if Path(filename).name != filename or hashlib.sha256((experiment / filename).read_bytes()).hexdigest() != expected:
                raise ValueError("Comparison experiment checksum mismatch")
        frozen = load_frozen_model(experiment / "frozen_model")
    else:
        frozen = load_frozen_model(model_artifact)
        if frozen.manifest.get("training_role") != "forward_only_no_historical_oos":
            raise ValueError("Direct model initialization requires a declared forward-only candidate")
    model_protocol = json.loads((frozen.path / "protocol.json").read_text())
    protocol = model_protocol["base_protocol"]
    cfg = ResearchConfig(**protocol["config"])
    config = ForwardPaperConfig(initial_capital=initial_capital, buy_fee_bps=cfg.buy_fee_bps,
        sell_fee_bps=cfg.sell_fee_bps, slippage_bps=cfg.slippage_bps,
        rebalance_sessions=cfg.rebalance_sessions)
    model_identity = {"model_sha256": frozen.model_sha256,
                      "trained_through": frozen.manifest["training"]["declared_cutoff"],
                      "model_protocol_sha256": frozen.manifest["protocol_sha256"],
                      "model_manifest_sha256": frozen.manifest["manifest_sha256"]}
    if frozen.manifest["source_manifest"].get("price_semantics", {}).get("adjustment_transform") == "affine_cash_accumulation":
        model_identity["return_basis"] = "provider_adjusted_price_exposure_not_investable_total_return"
        model_identity["execution_ready"] = False
    future = Calendar.load(calendar_path)
    original = load_research_prices(frozen.manifest["source_snapshot"])
    expected_source = frozen.manifest.get("source_manifest_sha256")
    if expected_source and original.manifest_sha256 != expected_source:
        raise ValueError("Training dataset changed after the forward model was frozen")
    original_rows = original.calendar_frame.to_dict("records") if hasattr(original, "calendar_frame") else pd.read_csv(original.path / "calendar.csv").to_dict("records")
    flags = {str(row["date"]): int(row.get("is_trading_day", row.get("is_open"))) for row in original_rows}
    if any(day in flags and flags[day] != int(flag) for day, flag in future.days):
        raise ValueError("The training and future exchange calendars disagree on an overlapping date")
    flags.update(dict(future.days))
    start, end = min(flags), max(flags)
    original_source = original.manifest["calendar"]["source"]
    source = future.source if original_source == future.source else f"{original_source}+{future.source}"
    source_url = future.source_url if original_source == future.source else original.manifest["calendar"].get("source_url", "https://www.baostock.com/") + " ; " + future.source_url
    calendar = Calendar.from_rows([{"date": day, "is_open": flag} for day, flag in sorted(flags.items())],
        start, end, source=source, source_url=source_url, fetched_at=future.fetched_at)
    manifest = create_forward_paper(target, config, protocol=protocol, model=model_identity,
                                    universe=frozen.manifest["instruments"], now=instant)
    copied_model = target / "frozen_model"
    if not copied_model.exists():
        with tempfile.TemporaryDirectory(prefix=".forward-model-", dir=target) as temporary:
            stage = Path(temporary) / "model"
            shutil.copytree(frozen.path, stage)
            load_frozen_model(stage)
            stage.rename(copied_model)
    if load_frozen_model(copied_model).model_sha256 != frozen.model_sha256:
        raise ValueError("Existing forward model differs from the frozen experiment")
    calendar.save(target / "calendar.json")
    adapter = {"schema_version": 1, "manifest_sha256": manifest["record_sha256"],
               "source": frozen.manifest["source_manifest"]["provider"]["name"],
               "adjustment": frozen.manifest["source_manifest"]["request"]["adjustment"],
               "warmup_natural_days": 400, "daily_attempt_limit": 3,
               "initial_capital": config.initial_capital,
               "model_path": "frozen_model", "calendar_path": "calendar.json"}
    if original.manifest.get("dataset_kind") == "append_only_multi_collection":
        adapter["dataset_seed"] = {"path": str(original.path), "manifest_sha256": original.manifest_sha256}
        adapter["dataset_catalog"] = "data/catalog"
    adapter["sha256"] = _hash(adapter)
    if (target / "adapter.json").exists() and json.loads((target / "adapter.json").read_text()) != adapter:
        raise ValueError("The forward adapter is already frozen with different settings")
    _write(target / "adapter.json", adapter)
    return forward_paper_status(target)


def _bar_pairs(artifact, instruments, day, previous):
    frame = artifact.prices.copy()
    frame["date"] = pd.to_datetime(frame.date).dt.strftime("%Y-%m-%d")
    result = {}
    for instrument in instruments:
        selected = frame.loc[frame.instrument.eq(instrument)].set_index("date")
        if day not in selected.index or previous not in selected.index:
            raise ValueError(f"Completed current/previous daily bar is missing for {instrument}")
        current, old = selected.loc[day], selected.loc[previous]
        result[instrument] = {"session_date": day, "previous_session": previous,
            "raw_open": float(current.raw_open), "raw_close": float(current.raw_close),
            "adjusted_open": float(current.adjusted_open), "adjusted_close": float(current.adjusted_close),
            "previous_raw_close": float(old.raw_close), "previous_adjusted_close": float(old.adjusted_close),
            "tradestatus": str(current.tradestatus), "complete": True}
    return result


def _completed_retrieval(artifact, day, instant):
    """A post-close wrapper cannot make a resumed intraday response complete."""
    public = artifact.manifest["provider"]["name"] in {"tencent", "eastmoney"}
    provenance = artifact.manifest.get("download_provenance", {})
    records = provenance.get("responses", [])
    if public and not records:
        raise ValueError("Completed daily public data requires original response retrieval times")
    values = [row.get("fetched_at") for row in records] if records else [artifact.manifest["created_at"]]
    try:
        stamps = [datetime.fromisoformat(value) for value in values]
        threshold = datetime.fromisoformat(day + "T15:05:00+08:00")
        if any(stamp.tzinfo is None or stamp.utcoffset() is None or stamp < threshold
               or stamp > instant or stamp.astimezone(SHANGHAI).date().isoformat() != day for stamp in stamps):
            raise ValueError
        first, last = min(stamps), max(stamps)
        if records and (provenance.get("as_of_date") != day
                        or datetime.fromisoformat(provenance.get("first_response_at", "")) != first
                        or datetime.fromisoformat(provenance.get("last_response_at", "")) != last):
            raise ValueError
    except (ValueError, TypeError):
        raise ValueError("Every original daily response must be today's post-close retrieval; refresh the entire collection") from None
    return {"fetched_at": first.isoformat(), "first_response_at": first.isoformat(),
            "last_response_at": last.isoformat(), "generation": provenance.get("generation")}


def _daily_targets(model, artifact, day, next_day, cfg):
    close = artifact.adjusted_close
    index = artifact.calendar.get_loc(pd.Timestamp(day))
    scores = {}
    for instrument in model.manifest["instruments"]:
        window = close.iloc[index - cfg.lookback_sessions:index + 1][instrument]
        if len(window) != cfg.lookback_sessions + 1 or window.isna().any():
            raise ValueError("Daily momentum history is incomplete")
        score = float(window.iloc[-1] / window.iloc[0] - 1)
        if not math.isfinite(score):
            raise ValueError("Daily momentum score is not finite")
        scores[instrument] = score
    selected = [instrument for instrument, score in sorted(scores.items(), key=lambda item: (-item[1], item[0]))
                if score > 0 or not cfg.positive_momentum_only][:cfg.top_k]
    momentum = {instrument: (1 - cfg.cash_reserve_pct) / len(selected) for instrument in selected}
    predictions = model.predict(artifact, signal_dates=pd.DatetimeIndex([day]))
    ml, rows = model.target_weights(predictions, day, next_day, model.manifest["instruments"], cfg.cash_reserve_pct)
    return {"momentum": momentum, "lightgbm": ml}, {"momentum_scores": scores, "lightgbm_scores": rows}


def run_daily(paper_dir, *, clock=None, download=None, provider_factory=None):
    """Idempotent post-close update; at most three download attempts per day.

    Providers may be injected for offline orchestration tests. Real execution
    uses the source frozen into the model, with bounded public HTTP requests.
    Successful days are skipped before network IO. Missed sessions halt the
    forward period rather than reconstructing yesterday's signals today.
    """
    clock = clock or (lambda: datetime.now(timezone.utc))
    instant = clock()
    if instant.tzinfo is None:
        raise ValueError("Daily runner requires a timezone-aware clock")
    root = Path(paper_dir).resolve()
    with _daily_lock(root):
        status = forward_paper_status(root)
        if status["halted"]:
            raise ValueError("Forward period is halted by a missing session; create a separately declared new period")
        adapter = json.loads((root / "adapter.json").read_text())
        if adapter.get("sha256") != _hash({key: value for key, value in adapter.items() if key != "sha256"}) or adapter["manifest_sha256"] != status["manifest"]["record_sha256"]:
            raise ValueError("Forward adapter integrity/binding check failed")
        day = instant.astimezone(SHANGHAI).date().isoformat()
        if status["latest_signal_date"] == day:
            return {"status": "already_complete", "session_date": day, "paper": status}
        calendar = Calendar.load(root / "calendar.json")
        try:
            calendar._require_coverage(day)
            next_day = calendar.next_session(day)
        except ValueError:
            local = instant.astimezone(SHANGHAI)
            calendar = download_calendar(calendar.coverage_start, f"{local.year + 1}-12-31", root / "calendar.json", refresh=True)
            next_day = calendar.next_session(day)
        if not calendar.is_session(day):
            return {"status": "market_closed", "session_date": day}
        local = instant.astimezone(SHANGHAI)
        if (local.hour, local.minute) < (15, 5):
            return {"status": "waiting_for_close", "session_date": day}
        receipts = root / "runner"
        receipts.mkdir(exist_ok=True)
        attempts = sorted(receipts.glob(f"{day}_attempt_*.json"))
        settled_today = status["latest_settlement_date"] == day
        if len(attempts) >= adapter["daily_attempt_limit"] and not settled_today:
            raise ValueError("Daily download attempt limit reached; inspect preserved failure receipts")
        receipt = receipts / f"{day}_attempt_{len(attempts) + 1}.json"
        started = clock().isoformat()
        try:
            model = load_frozen_model(root / adapter["model_path"])
            if model.model_sha256 != status["manifest"]["model"]["model_sha256"]:
                raise ValueError("Forward model no longer matches its frozen declaration")
            source = adapter["source"]
            if settled_today:
                # Resume the phase after a committed NAV without changing its source vintage.
                evidence = dict(status["latest_settlement"]["source"])
                artifact = load_research_prices(root / "data/snapshots" / evidence["source_snapshot_sha256"])
                if artifact.manifest_sha256 != evidence["source_manifest_sha256"]:
                    raise ValueError("Saved settlement source no longer matches its immutable snapshot")
            else:
                cached = CachedCalendarProvider(calendar, model.manifest["source_manifest"]["instruments"])
                if provider_factory is None:
                    from stopat30m.data.research_prices import EastmoneyResearchProvider, TencentResearchProvider
                    provider_class = {"tencent": TencentResearchProvider, "eastmoney": EastmoneyResearchProvider}.get(source)
                    if provider_class is None:
                        raise ValueError("No explicitly supported daily adapter for the frozen price source")
                    provider = provider_class(calendar_provider=cached, min_interval=3, max_attempts=3, timeout=15)
                else:
                    provider = provider_factory(cached)
                start = max(calendar.coverage_start, (local.date() - timedelta(days=adapter["warmup_natural_days"])).isoformat())
                try:
                    artifact = (download or download_research_prices)(model.manifest["instruments"], start, day,
                        root / "data", adjustment=adapter["adjustment"], refresh=bool(attempts), provider=provider)
                finally:
                    if hasattr(provider, "close"):
                        provider.close()
            previous = calendar.previous_session(day)
            verified = load_research_prices(artifact.path)
            if verified.manifest_sha256 != artifact.manifest_sha256:
                raise ValueError("Daily source snapshot changed after download")
            if verified.manifest["provider"]["name"] != source or verified.manifest["request"]["adjustment"] != adapter["adjustment"]:
                raise ValueError("Daily source provider/adjustment differs from the frozen model")
            if set(verified.manifest["request"]["instruments"]) != set(model.manifest["instruments"]) or verified.manifest["request"]["end_date"] != day:
                raise ValueError("Daily source request does not match today's frozen universe")
            artifact = verified
            retrieval = _completed_retrieval(artifact, day, clock())
            dataset_identity = None
            if adapter.get("dataset_seed"):
                from stopat30m.data.research_catalog import publish_daily_update
                seed = load_research_prices(adapter["dataset_seed"]["path"])
                if seed.manifest_sha256 != adapter["dataset_seed"]["manifest_sha256"]:
                    raise ValueError("The frozen master dataset seed changed")
                dataset = publish_daily_update(seed.path, artifact, root / "data", root / "calendar.json",
                                               root / adapter["dataset_catalog"])
                dataset_identity = {"path": str(dataset.path), "artifact_id": dataset.manifest["artifact_id"],
                                    "manifest_sha256": dataset.manifest_sha256,
                                    "verified_through": dataset.manifest["request"]["end_date"]}
            bars = _bar_pairs(artifact, model.manifest["instruments"], day, previous)
            cfg = ResearchConfig(**status["manifest"]["protocol"]["config"])
            evidence = evidence if settled_today else {"source": source, "source_snapshot_sha256": artifact.manifest["artifact_id"],
                "source_manifest_sha256": artifact.manifest_sha256, "model_sha256": model.model_sha256,
                "protocol_sha256": status["manifest"]["protocol"]["protocol_sha256"],
                **retrieval, "session_date": day,
                "role": "forward_completed_daily", "completed": True}
            if dataset_identity is not None:
                evidence = {**evidence, "verified_master_dataset": dataset_identity}
            if status["latest_signal_date"] and not settled_today:
                settle_completed_session(root, day, bars, calendar=calendar, evidence=evidence, now=clock())
            targets, diagnostics = _daily_targets(model, artifact, day, next_day, cfg)
            # Dates in diagnostic rows are converted to canonical strings before sealing.
            diagnostics = json.loads(json.dumps(diagnostics, default=lambda value: value.isoformat()))
            record_close_signal(root, day, targets, calendar=calendar,
                                evidence={**evidence, "bars": bars, "diagnostics": diagnostics}, now=clock())
            result = {"status": "complete", "session_date": day, "paper": forward_paper_status(root)}
            _write(receipt, {"status": "complete", "started_at": started, "finished_at": clock().isoformat(),
                             "snapshot": str(artifact.path), "snapshot_sha256": artifact.manifest_sha256,
                             "verified_master_dataset": dataset_identity})
            _write(receipts / "latest.json", result)
            return result
        except Exception as exc:
            _write(receipt, {"status": "failed", "started_at": started, "finished_at": clock().isoformat(),
                             "error_type": type(exc).__name__, "error": str(exc)[:2000],
                             "historical_backfill_performed": False})
            raise
