"""Offline web wiring tests; synthetic numbers are not evidence of performance."""
from dataclasses import replace
import json
import threading

from fastapi import FastAPI
from fastapi.testclient import TestClient
import numpy as np
import pandas as pd
import pytest

from stopat30m.backtest.walk_forward import ResearchConfig, lock_protocol, run_walk_forward
from stopat30m.data.historical_universe import HistoricalUniverse
from stopat30m.data.research_prices import download_research_prices
from stopat30m.research_workspace import REGISTRY, ResearchWorkspace, WorkspaceError, clean_json

A, B, INDEX = "SH510300", "SH510500", "SH000300"


@pytest.fixture
def workspace_fixture(tmp_path):
    days = pd.bdate_range("2019-11-01", periods=80)
    start, end = str(days[0].date()), str(days[-1].date())
    request = {"source": "eastmoney", "calendar_source": "baostock.query_trade_dates", "start_date": start,
               "end_date": end, "adjustment": "hfq", "instruments": [A, B, INDEX]}
    pool = HistoricalUniverse.fixed_pool("explicit synthetic test pool", [A, B], start, end,
                                        selection_date="2026-10-19", source="offline_test_fixture")
    payload = pool.as_dict()
    payload["metadata"]["research_data_request"] = request
    universe = HistoricalUniverse.from_dict(payload)
    config = ResearchConfig(train_start=str(days[10].date()), train_end=str(days[19].date()),
        validation_start=str(days[20].date()), validation_end=str(days[29].date()),
        oos_start=str(days[30].date()), oos_end=str(days[49].date()), lookback_sessions=3,
        rebalance_sessions=2, fold_sessions=5, positive_momentum_only=False)
    protocols = tmp_path / "protocols"
    protocols.mkdir()
    registry = {key: value for key, value in REGISTRY.items() if key != "baostock_etf"}
    universe.save(protocols / registry["eastmoney_etf"][1])
    for key, slip in (("eastmoney_etf", 2), ("eastmoney_etf_cost10", 7), ("eastmoney_etf_cost20", 17)):
        lock_protocol(replace(config, slippage_bps=slip), protocols / registry[key][0], universe=universe)
    calls = []
    class Provider:
        source, source_url, version = "eastmoney", "fixture://synthetic-no-network", "offline-test"
        volume_unit, amount_unit = "shares", "CNY"
        calendar_source, calendar_source_url = "baostock.query_trade_dates", "fixture://synthetic-calendar"
        def __init__(self, **kwargs):
            pass
        def close(self):
            pass
        def instrument_info(self, instrument):
            return {"asset_type": "index" if instrument == INDEX else "ETF", "name": "synthetic test"}
        def fetch_calendar(self, begin, finish):
            calls.append("calendar")
            dates = pd.date_range(begin, finish)
            return pd.DataFrame({"date": dates.strftime("%Y-%m-%d"), "is_trading_day": dates.isin(days).astype(int)})
        def fetch_raw(self, instrument, begin, finish):
            calls.append((instrument, "raw"))
            rate = .001 if instrument != B else .0005
            price = 10 * (1 + rate) ** np.arange(len(days))
            return pd.DataFrame({"date": days.strftime("%Y-%m-%d"), "code": instrument,
                "open": price, "high": price, "low": price, "close": price,
                "volume": 1000, "amount": price * 1000, "tradestatus": "1", "isST": "0"})
        def fetch_adjusted(self, instrument, begin, finish, adjustment):
            frame = self.fetch_raw(instrument, begin, finish)
            calls[-1] = (instrument, "adjusted")
            return frame
    root = tmp_path / "research"
    workspace = ResearchWorkspace(root=root, protocol_root=protocols, registry=registry, provider_factory=Provider)
    return workspace, Provider, calls, days, config, universe


def make_snapshot(fixture):
    workspace, Provider, *_ = fixture
    request = workspace.protocols()[0]["request"]
    return download_research_prices(request["instruments"], request["start_date"], request["end_date"],
                                    workspace.data_root, provider=Provider())


def test_registered_protocols_and_async_cached_download_restore_on_page_refresh(workspace_fixture):
    workspace, _, calls, *_ = workspace_fixture
    protocols = workspace.protocols()
    assert [item["id"] for item in protocols] == ["eastmoney_etf", "eastmoney_etf_cost10", "eastmoney_etf_cost20"]
    assert all(item["universe_kind"] == "fixed" for item in protocols)
    first = workspace.start_download()
    assert first["status"] == "running"
    completed = workspace.wait(first["id"])
    assert completed["status"] == "completed", completed
    assert completed["snapshot_id"]
    assert len(calls) == 7
    second = workspace.start_download()
    assert workspace.wait(second["id"])["status"] == "completed"
    assert len(calls) == 7  # Valid immutable request cache performs no source calls.
    restored = ResearchWorkspace(root=workspace.root, protocol_root=workspace.protocol_root, registry=workspace.registry)
    assert restored.job(first["id"])["status"] == "completed"
    snapshots = restored.snapshots()
    assert len(snapshots) == 1
    assert snapshots[0]["compatible_protocols"] == list(workspace.registry)


def test_cli_snapshot_and_multiple_same_fingerprint_runs_are_discovered(workspace_fixture):
    workspace, _, _, days, _, universe = workspace_fixture
    artifact = make_snapshot(workspace_fixture)
    protocol = workspace.protocol_root / workspace.registry["eastmoney_etf"][0]
    # Exact same CLI function and directory layout, no web job required.
    first = run_walk_forward(artifact, universe, protocol, workspace.run_root / "oos_cli_one")
    second = run_walk_forward(artifact, universe, protocol, workspace.run_root / "oos_cli_two")
    assert first.manifest["deterministic_fingerprint"] == second.manifest["deterministic_fingerprint"]
    rows = workspace.runs()
    assert {item["id"] for item in rows} == {"oos_cli_one", "oos_cli_two"}
    assert all(item["validation_status"] == "valid" for item in rows), rows
    detail = workspace.run_detail("oos_cli_one")
    assert detail["chart"][0]["date"] == str(days[29].date())
    assert detail["chart"][0]["momentum"] == 1
    assert detail["chart"][0]["momentum_drawdown"] == 0
    assert detail["chart"][1]["momentum_drawdown"] < 0  # First-day cost remains visible.
    assert set(detail["report"]["splits"]) == {"train", "validation", "final_oos"}
    table = workspace.table("oos_cli_one", "positions", offset=1, limit=2)
    assert len(table["rows"]) == 2 and table["total"] > 2
    assert all(row["split"] == "final_oos" and row["strategy"] == "momentum" for row in table["rows"])
    assert workspace.table("oos_cli_one", "trades", offset=1, limit=2)["total"] == 1
    assert workspace.table("oos_cli_one", "folds", split="train")["total"] == 0


def test_async_run_is_verified_before_completion_and_sources_cannot_be_swapped(workspace_fixture):
    workspace, Provider, *_ = workspace_fixture
    artifact = make_snapshot(workspace_fixture)
    job = workspace.start_run("eastmoney_etf_cost10", artifact.manifest["artifact_id"])
    final = workspace.wait(job["id"])
    assert final["status"] == "completed", final
    assert final["run_id"].startswith("oos_")
    assert workspace.runs()[0]["protocol_id"] == "eastmoney_etf_cost10"
    with pytest.raises(WorkspaceError, match="Unknown registered"):
        workspace.start_run("/tmp/arbitrary.json", artifact.manifest["artifact_id"])
    with pytest.raises(WorkspaceError, match="main Eastmoney"):
        workspace.start_download("eastmoney_etf_cost10")
    class OtherSource(Provider):
        source = "baostock"
    request = workspace.protocols()[0]["request"]
    other = download_research_prices(request["instruments"], request["start_date"], request["end_date"],
        workspace.data_root, provider=OtherSource())
    with pytest.raises(WorkspaceError, match="provider differs"):
        workspace.start_run("eastmoney_etf", other.manifest["artifact_id"])


def test_original_baostock_protocol_is_read_only(workspace_fixture):
    workspace, *_ = workspace_fixture
    with pytest.raises(WorkspaceError, match="audit only") as exc:
        workspace.start_run("baostock_etf", "a" * 64)
    assert exc.value.status_code == 400
    assert workspace.jobs() == []


def test_single_task_lock_real_failure_and_interruption_recovery(workspace_fixture):
    workspace, *_ = workspace_fixture
    entered, release = threading.Event(), threading.Event()
    def failed_download(*args, **kwargs):
        kwargs["progress"]("真实离线失败路径")
        entered.set()
        assert release.wait(3)
        raise ValueError("provider test disconnect, no usable snapshot")
    workspace._download = failed_download
    job = workspace.start_download()
    assert entered.wait(3)
    second_process = ResearchWorkspace(root=workspace.root, protocol_root=workspace.protocol_root, registry=workspace.registry)
    assert second_process.job(job["id"])["status"] == "running"  # Live owner is not marked interrupted.
    with pytest.raises(WorkspaceError, match="任务正在运行"):
        second_process.start_download()
    release.set()
    failed = workspace.wait(job["id"])
    assert failed["status"] == "failed" and "provider test disconnect" in failed["error"]
    assert workspace.snapshots() == []
    failed["status"], failed["completed_at"] = "running", None
    (workspace.job_root / f"{job['id']}.json").write_text(json.dumps(failed))
    assert second_process.job(job["id"])["status"] == "interrupted"  # Already-running reader also recovers a later crash.
    restarted = ResearchWorkspace(root=workspace.root, protocol_root=workspace.protocol_root, registry=workspace.registry)
    assert restarted.job(job["id"])["status"] == "interrupted"
    assert restarted.job(job["id"])["completed_at"]


def test_bad_artifacts_are_visible_and_safe_ids_and_symlinks_are_rejected(workspace_fixture, tmp_path):
    workspace, *_ = workspace_fixture
    artifact = make_snapshot(workspace_fixture)
    bad = workspace.snapshot_root / ("a" * 64)
    bad.mkdir()
    (bad / "manifest.json").write_text("{")
    rows = workspace.snapshots()
    assert len(rows) == 2
    invalid = next(item for item in rows if item["id"] == "a" * 64)
    assert invalid["validation_status"] == "invalid" and invalid["validation_error"]
    assert invalid["compatible_protocols"] == []
    with pytest.raises(WorkspaceError):
        workspace.start_run("eastmoney_etf", "a" * 64)
    outside = tmp_path / "outside"
    outside.mkdir()
    (workspace.run_root).mkdir()
    (workspace.run_root / "oos_symlink").symlink_to(outside, target_is_directory=True)
    assert workspace.runs()[0]["validation_status"] == "invalid"
    with pytest.raises(WorkspaceError, match="Symlinks"):
        workspace.run_detail("oos_symlink")
    for bad_id in ("../secrets", "/etc/passwd", "a" * 65):
        with pytest.raises(WorkspaceError, match="Invalid"):
            workspace.start_run("eastmoney_etf", bad_id)
    # A modified CSV cannot be reported as a successful input.
    (artifact.path / "prices.csv").write_text("date,bad\n")
    row = next(item for item in workspace.snapshots() if item["id"] == artifact.manifest["artifact_id"])
    assert row["validation_status"] == "invalid" and "checksum" in row["validation_error"]


def test_run_checksum_corruption_does_not_hide_other_valid_cli_runs(workspace_fixture):
    workspace, _, _, _, _, universe = workspace_fixture
    artifact = make_snapshot(workspace_fixture)
    protocol = workspace.protocol_root / workspace.registry["eastmoney_etf"][0]
    for run_id in ("oos_valid", "oos_bad"):
        run_walk_forward(artifact, universe, protocol, workspace.run_root / run_id)
    (workspace.run_root / "oos_bad" / "report.json").write_text("{}")
    rows = {item["id"]: item for item in workspace.runs()}
    assert rows["oos_valid"]["validation_status"] == "valid"
    assert rows["oos_bad"]["validation_status"] == "invalid"
    with pytest.raises(WorkspaceError, match="checksum"):
        workspace.run_detail("oos_bad")


def test_admin_api_auth_validation_async_jobs_and_nullable_numbers(workspace_fixture):
    from stopat30m.api.v1.endpoints import research
    from stopat30m.auth.deps import get_current_user
    from types import SimpleNamespace
    workspace, *_ = workspace_fixture
    app = FastAPI()
    app.include_router(research.router, prefix="/api/v1")
    app.dependency_overrides[research.get_workspace] = lambda: workspace
    # Avoid a database while preserving the real require_role dependency.
    def unauthorized():
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="Missing authentication")
    app.dependency_overrides[get_current_user] = unauthorized
    with TestClient(app) as client:
        assert client.get("/api/v1/research/protocols").status_code == 401
        app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(role="user")
        assert client.get("/api/v1/research/snapshots").status_code == 403
        app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(role="admin")
        assert client.get("/api/v1/research/protocols").status_code == 200
        assert client.post("/api/v1/research/download", json={"min_interval": 1}).status_code == 422
        assert client.post("/api/v1/research/download", json={"protocol_id": "/tmp/private"}).status_code == 422
        assert client.post("/api/v1/research/run", json={"protocol_id": "eastmoney_etf", "snapshot_id": "../"}).status_code == 422
        response = client.post("/api/v1/research/download", json={})
        assert response.status_code == 202
        job_id = response.json()["id"]
        assert workspace.wait(job_id)["status"] == "completed"
        assert client.get(f"/api/v1/research/jobs/{job_id}").json()["status"] == "completed"
        assert len(client.get("/api/v1/research/snapshots").json()) == 1
        assert client.get("/api/v1/research/jobs/not-a-job").status_code == 400
    assert clean_json({"x": np.inf, "y": [np.nan, np.float64(1), pd.NA]}) == {"x": None, "y": [None, 1, None]}


def test_dependency_initialization_failure_is_an_explicit_conflict(monkeypatch):
    from fastapi import HTTPException
    from stopat30m.api.v1.endpoints import research
    def broken_workspace():
        raise OSError("synthetic permissions failure")
    research.get_workspace.cache_clear()
    monkeypatch.setattr(research, "ResearchWorkspace", broken_workspace)
    try:
        with pytest.raises(HTTPException) as exc:
            research.get_workspace()
        assert exc.value.status_code == 409 and "permissions failure" in exc.value.detail
    finally:
        research.get_workspace.cache_clear()


def test_mixed_corrupt_job_records_remain_visible_without_blocking_workspace(workspace_fixture):
    workspace, *_ = workspace_fixture
    first = workspace.start_download()
    completed = workspace.wait(first["id"])
    assert completed["status"] == "completed"
    corrupt = {
        "b" * 32: "{broken json",
        "legacy bad name": json.dumps(completed),
        "c" * 32: json.dumps({**completed, "id": "c" * 32, "status": "queued"}),
        "d" * 32: json.dumps({"id": "d" * 32, "status": "completed"}),
    }
    for job_id, content in corrupt.items():
        (workspace.job_root / f"{job_id}.json").write_text(content)
    # More than one page of healthy records must not hide damaged old ones.
    for ordinal in range(21):
        job_id = f"{ordinal:032x}"
        (workspace.job_root / f"{job_id}.json").write_text(json.dumps({**completed, "id": job_id}))
    restored = ResearchWorkspace(root=workspace.root, protocol_root=workspace.protocol_root,
                                  registry=workspace.registry, provider_factory=workspace._provider_factory)
    assert restored.protocols()
    assert restored.snapshots()[0]["validation_status"] == "valid"
    listed = restored.jobs()
    assert len(listed) == 20
    invalid = [row for row in listed if row["status"] == "invalid"]
    assert {row["id"] for row in invalid} == set(corrupt)
    assert all(row["kind"] == "unknown" and row["error"] and row["run_id"] is None for row in invalid)
    assert listed[:4] == invalid
    assert restored.job(first["id"])["status"] == "completed"
    with pytest.raises(WorkspaceError) as exc:
        restored.job("b" * 32)
    assert exc.value.status_code == 409
    assert "JSONDecodeError" in str(exc.value)
    next_job = restored.start_download()
    assert restored.wait(next_job["id"])["status"] == "completed"
    for job_id, content in corrupt.items():
        assert (workspace.job_root / f"{job_id}.json").read_text() == content


def test_worker_with_corrupted_state_preserves_file_and_releases_task_lock(workspace_fixture):
    workspace, *_ = workspace_fixture
    entered, release = threading.Event(), threading.Event()
    def interrupted_download(*args, **kwargs):
        entered.set()
        assert release.wait(3)
        raise ValueError("offline worker failure after state corruption")
    workspace._download = interrupted_download
    job = workspace.start_download()
    assert entered.wait(3)
    path = workspace.job_root / f"{job['id']}.json"
    path.write_text("{damaged state")
    assert workspace.jobs()[0]["status"] == "invalid"
    with pytest.raises(WorkspaceError, match="任务正在运行"):
        workspace.start_download()
    release.set()
    workspace._threads[job["id"]].join(3)
    assert not workspace._threads[job["id"]].is_alive()
    assert path.read_text() == "{damaged state"
    workspace._download = None
    next_job = workspace.start_download()
    assert workspace.wait(next_job["id"])["status"] == "completed"
    assert path.read_text() == "{damaged state"
