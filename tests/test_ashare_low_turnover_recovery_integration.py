"""Same-account recovery through real runner/report gates, entirely offline.

Only inference and provider transports are synthetic.  Initialization, fills,
failed attempts, observation gaps, explicit migration, guarded empty dividend
evidence, close commitment, next-open gates and reporting use production code.
"""
from copy import deepcopy
from datetime import timezone
import ast
import json
from pathlib import Path
import shutil
from types import SimpleNamespace

import pytest

from stopat30m import ashare_forward as legacy
from stopat30m import ashare_low_turnover_forward as runner
from stopat30m import ashare_low_turnover_recovery as recovery
from stopat30m import ashare_low_turnover_recovery_close as recovery_close
from stopat30m import ashare_paper_report as reports
from stopat30m.data.ashare_actions import REQUIRED, SOURCE
from stopat30m.data.ashare_execution_data import EXECUTION_FIELDS
from test_ashare_low_turnover_forward import Provider, batch, clock, paper


def _save(path, value):
    legacy._write(path, value)
    return recovery._proof(path)


def _read_state(root):
    return json.loads((root / "state.json").read_bytes())


def _tree_bytes(root):
    return {str(path.relative_to(root)): path.read_bytes() for path in root.rglob("*") if path.is_file()}


def _held_rights_evidence(directory, held, day, stamp, *, missing_terminal_guard=False):
    """Real validated collection/response/terminal-guard shape for 20 holdings."""
    directory.mkdir(parents=True)
    proofs, responses = [], {}
    generation = "synthetic-recovery-close-" + day
    for inst in sorted(held):
        request = {"code": inst[:2].lower() + "." + inst[2:], "year": day[:4], "yearType": "operate"}
        payload = {"generation": generation, "request": request, "fetched_at": stamp,
            "fields": sorted(REQUIRED), "rows": []}
        filename = f"{inst}_{day[:4]}.json"
        original = _save(directory / filename, payload)
        proofs.append(original)
        responses[f"{inst}_{day[:4]}"] = {"file": filename, "sha256": original["sha256"]}
        # The actual validated dividend SDK guard has no kind/query markers.
        guard = {"request": request, "server_echo": request, "server_error_code": "0", "status": "verified",
            "complete_row_iteration_observed": True, "rows_verified": 0, "started_at": stamp, "finished_at": stamp}
        if not missing_terminal_guard or inst != sorted(held)[-1]:
            proofs.append(_save(directory / f"{inst}_guard.json", guard))
    proofs.append(_save(directory / "collection.json", {
        "schema_version": 1, "source": SOURCE, "generation": generation,
        "instruments": sorted(held), "years": [int(day[:4])], "year_type": "operate", "as_of_date": day,
        "responses": responses, "failures": {}, "status": "complete",
    }))
    return proofs


def _current_raw_evidence(directory, held, day, previous, stamp, *, values=("10", "10", "10"), problem=None):
    """A complete current two-mode capture, including each original daily guard."""
    directory.mkdir()
    previous_close, current_close, preclose = values
    fields = EXECUTION_FIELDS.split(",")
    generation = "synthetic-current-raw-" + day
    proofs, responses, response_hashes = [], {}, {}
    basic_fields = ["code", "code_name", "ipoDate", "outDate", "type", "status"]
    basic_request = {"code": "", "code_name": ""}
    basic = {"generation": generation, "fetched_at": stamp, "request": basic_request, "fields": basic_fields,
        "rows": [[inst[:2].lower()+"."+inst[2:], inst, "2010-01-01", "", "1", "1"] for inst in sorted(held)]}
    basic_proof = _save(directory / "stock_basic.json", basic)
    basic_guard = _save(directory / "stock_basic_guard.json", {
        "kind": "baostock_metadata_server_echo_guard", "query": "query_stock_basic",
        "request": basic_request, "server_echo": basic_request, "server_error_code": "0", "status": "verified",
        "complete_row_iteration_observed": True, "rows_verified": len(held), "started_at": stamp, "finished_at": stamp})
    proofs.extend([basic_proof, basic_guard])
    responses["stock_basic"] = {"file": "stock_basic.json", "sha256": basic_proof["sha256"],
        "fetched_at": stamp, "rows": len(held), "guard": basic_guard}
    for inst in sorted(held):
        code = inst[:2].lower() + "." + inst[2:]
        actual_preclose = "10.5" if problem == "mismatched_preclose" and inst == sorted(held)[-1] else preclose
        for mode, flag in (("raw", "3"), ("hfq", "1")):
            request = {"code": code, "fields": EXECUTION_FIELDS, "start_date": previous,
                "end_date": day, "frequency": "d", "adjustflag": flag}
            raw_rows = [
                {"date": previous, "code": code, "open": "10", "high": "12", "low": "9", "close": previous_close,
                    "preclose": "10", "volume": "100000", "amount": "1000000", "adjustflag": flag, "tradestatus": "1", "isST": "0"},
                {"date": day, "code": code, "open": "10", "high": "13", "low": "9", "close": current_close,
                    "preclose": actual_preclose, "volume": "100000", "amount": "1100000", "adjustflag": flag, "tradestatus": "1", "isST": "0"},
            ]
            payload = {"generation": generation, "request": request, "fields": fields,
                "rows": [[row[field] for field in fields] for row in raw_rows], "fetched_at": stamp}
            filename = f"{inst}_{mode}.json"
            source = _save(directory / filename, payload)
            guard = _save(directory / f"{inst}_{mode}_guard.json", {
                "kind": "baostock_daily_server_echo_guard", "request": request, "server_echo": request,
                "server_error_code": "0", "status": "verified", "complete_row_iteration_observed": True,
                "rows_verified": 2, "started_at": stamp, "finished_at": stamp,
                "row_mode_flags_repaired": False, "row_mode_anomalies_observed": []})
            proofs.append(source)
            if not (mode == "raw" and problem == "missing_previous_raw_guard" and inst == sorted(held)[-1]):
                proofs.append(guard)
            responses[f"{inst}_{mode}"] = {"file": filename, "sha256": source["sha256"],
                "fetched_at": stamp, "rows": 2, "guard": guard}
            if mode == "raw":
                response_hashes[inst] = source["sha256"]
    proofs.append(_save(directory / "capture.json", {
        "kind": "low_turnover_guarded_daily_originals", "generation": generation, "source": "baostock",
        "start_date": previous, "session": day, "instruments": sorted(held),
        "responses": responses, "status": "complete", "failures": {}, "finished_at": stamp,
    }))
    return proofs, response_hashes


class GuardedProvider(Provider):
    def __init__(self, paper, directory, *, problem=None):
        super().__init__(paper)
        self.directory, self.problem = directory, problem

    def fetch_close(self, day, *args):
        value = super().fetch_close(day, *args)
        state = _read_state(self.paper[0])
        held = set().union(*(legacy._account_instruments(account) for account in state["accounts"].values()))
        value["evidence"] += _held_rights_evidence(
            self.directory / f"{day}-{self.calls}", held, day, value["fetched_at"],
            missing_terminal_guard=self.problem == "missing_rights_guard",
        )
        if self.problem == "partial_close":
            value["quotes"].pop(sorted(held)[-1])
        if state.get(recovery.STATE_KEY, {}).get("activation") is None:
            calendar = runner.Calendar.load(self.paper[0] / "calendar.json")
            previous = calendar.previous_session(day)
            evidence, hashes = _current_raw_evidence(
                self.directory / f"{day}-{self.calls}" / "raw-daily", held, day, previous,
                value["fetched_at"], values=self.source_prices, problem=self.problem)
            value["evidence"] += evidence
            for inst in sorted(held):
                if inst not in value["quotes"]:
                    continue
                preclose = "10.5" if self.problem == "mismatched_preclose" and inst == sorted(held)[-1] else self.source_prices[2]
                value["quotes"][inst].update(source="baostock.daily_raw", close=self.source_prices[1], preclose=preclose,
                    source_response_sha256=hashes[inst])
                if self.problem == "wrong_previous_raw_hash" and inst == sorted(held)[-1]:
                    value["quotes"][inst]["source_response_sha256"] = "0" * 64
        return value

    source_prices = ("10", "10", "10")


class MovingCloseProvider(GuardedProvider):
    """Current evidence proves a missing close without posting it to the book."""
    source_prices = ("11", "12", "11")


@pytest.fixture
def recovery_case(paper, tmp_path, monkeypatch):
    root, names, source = paper
    # The shared transport fixture uses a model stub.  Bind that stub to real
    # immutable fixture bytes before its first close/open, as migration requires.
    candidate = SimpleNamespace(model_sha256=source["sha256"], manifest={"manifest_sha256": "b" * 64},
        protocol={"rebalance_sessions": 5, "original_fitted_at": "2026-10-20T07:00:00+00:00"},
        proofs=lambda: {"fixture": source})
    monkeypatch.setattr(runner, "_candidate", lambda manifest: candidate)
    # The recovery-only operation explicitly imports the original frozen
    # helpers.  Stub only the same two inference boundaries as the old fixture,
    # regardless of whether this module was imported before or after it.
    monkeypatch.setattr(recovery_close, "_signal_bar_eligibility", legacy._signal_bar_eligibility)
    monkeypatch.setattr(recovery_close, "_candidate_targets", legacy._candidate_targets)
    code = tmp_path / "synthetic-engineering-source.py"
    code.write_text("original frozen engineering source\n")
    manifest = json.loads((root / "manifest.json").read_bytes())
    manifest["model_sha256"] = candidate.model_sha256
    manifest["implementation_sha256"] = {str(code): recovery._sha(code)}
    manifest["manifest_sha256"] = legacy._digest({k: v for k, v in manifest.items() if k != "manifest_sha256"})
    _save(root / "manifest.json", manifest)
    state = _read_state(root)
    state["manifest_sha256"] = manifest["manifest_sha256"]
    legacy._save_state(root, state)

    provider = Provider(paper)
    assert runner.run_low_turnover(root, phase="close", provider=provider,
        now=clock("2026-10-20", "16:30"))["status"] == "completed"
    opened = runner.run_low_turnover(root, phase="open", provider=provider, now=clock("2026-10-21"))
    assert opened["status"] == "completed"
    assert len(_read_state(root)["accounts"]["model"]["positions"]) == 20

    class FailedSeam:
        def fetch_close(self, *args):
            raise ValueError("synthetic strict seam mismatch; do not publish")

    for _ in range(3):
        assert runner.run_low_turnover(root, phase="close", provider=FailedSeam(),
            now=clock("2026-10-21", "16:30"))["status"] == "failed"
    blocked = runner.run_low_turnover(root, phase="open", provider=provider, now=clock("2026-10-22"))
    assert blocked["status"] == "blocked_observation_gap_no_fill"

    before = tmp_path / "before-engineering-revision"
    shutil.copytree(root, before / "paper")
    archived_code = before / "sources" / code.name
    archived_code.parent.mkdir()
    shutil.copyfile(code, archived_code)
    old_manifest = json.loads((root / "manifest.json").read_bytes())
    old_state = _read_state(root)
    original_receipt_bytes = {item["path"]: Path(item["path"]).read_bytes() for item in old_state["receipts"]}
    evidence = _save(tmp_path / "offline-source-evidence.json", {"kind": "isolated_synthetic_source_fixture"})
    audit = _save(tmp_path / "offline-seam-audit.json", {
        "kind": "a15_offline_full_pool_seam_audit", "status": "passed", "instrument_count": 1300,
        "instruments_sha256": legacy._digest(old_manifest["initial_universe"]), "session": "2026-10-22",
        "strict_source_validation": True, "seam_tolerance_relaxed": False, "source_proofs": [evidence],
    })
    code.write_text("new explicitly audited engineering source\n")
    added_implementations = {
        str(Path(recovery.__file__).resolve()): recovery._sha(recovery.__file__),
        str(Path(recovery_close.__file__).resolve()): recovery._sha(recovery_close.__file__),
    }
    tested_implementations = {str(code): recovery._sha(code),
        **added_implementations}
    tests = _save(tmp_path / "isolated-offline-tests.json", {
        "kind": "a15_offline_recovery_tests", "status": "passed", "test_count": 1,
        "network_called": False, "evidence": [evidence], "implementation_sha256": tested_implementations,
    })
    monkeypatch.setattr(recovery, "_UPDATABLE", {code})
    declaration = {
        "kind": recovery.DECLARATION_KIND, "schema_version": 1,
        "declared_at": clock("2026-10-22", "15:59").astimezone(timezone.utc).isoformat(),
        "paper_dir": str(root), "gap_sessions": ["2026-10-21"], "resume_not_before_session": "2026-10-22",
        "archived_sources": {str(code): recovery._proof(archived_code)},
        "authorized_code_updates": {str(code): {
            "before_sha256": old_manifest["implementation_sha256"][str(code)], "after_sha256": recovery._sha(code)}},
        "added_implementation_proofs": added_implementations,
        "offline_audit": audit, "test_report": tests,
    }
    declaration_path = tmp_path / "explicit-recovery-declaration.json"
    _save(declaration_path, declaration)
    return {"paper": paper, "root": root, "before": before, "declaration": declaration_path,
        "old_manifest": old_manifest, "old_state": old_state, "old_receipt_bytes": original_receipt_bytes,
        "source_bytes": Path(source["path"]).read_bytes(), "evidence_dir": tmp_path / "new-close-evidence"}


def _migrate(case):
    return recovery.migrate_recovery(case["root"], before_directory=case["before"],
        declaration_path=case["declaration"], now=clock("2026-10-22", "16:00"))


def _activate(case, *, problem=None):
    _migrate(case)
    provider = GuardedProvider(case["paper"], case["evidence_dir"], problem=problem)
    receipt = runner.run_low_turnover(case["root"], phase="close", provider=provider,
        now=clock("2026-10-22", "16:30"))
    return provider, receipt


def test_recovery_close_changes_only_declared_gap_and_verified_comparison_reference_logic():
    old_tree = ast.parse(Path(legacy.__file__).read_text())
    new_tree = ast.parse(Path(recovery_close.__file__).read_text())
    old = next(node for node in old_tree.body if isinstance(node, ast.FunctionDef) and node.name == "_close_operation")
    new = next(node for node in new_tree.body if isinstance(node, ast.FunctionDef)
        and node.name == "close_after_explicit_recovery")
    declared = new.body.pop(2)
    assert ast.unparse(declared) == "unresolved_gaps = pending_gaps(state, close_session=session)"
    assert sum(isinstance(node, ast.Name) and node.id == "unresolved_gaps" for node in ast.walk(new)) == 2
    audit_initialization = next(node for node in new.body if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "reference_audit" for target in node.targets))
    assert ast.unparse(audit_initialization) == "reference_audit = {}"
    new.body.remove(audit_initialization)
    held_loop = next(node for node in ast.walk(new) if isinstance(node, ast.For)
        and ast.unparse(node.target) == "(instrument, position)")
    comparison_assignment = held_loop.body.pop(4)
    assert ast.unparse(comparison_assignment) == "comparison_mark = position.last_mark"
    comparison_gate = held_loop.body.pop(4)
    expected_gate = ast.parse('''
if (position.last_mark_date == previous and position.last_mark_basis == "live_observed_quote"
        and state["engineering_recovery"]["activation"] is None
        and previous in state.get("observation_gaps", {}) and not reference_verified):
    comparison_mark = previous_observed_close(state, batch, instrument, previous)
    reference_audit[instrument] = {"previous_session": previous,
        "saved_mark": str(position.last_mark), "saved_mark_basis": position.last_mark_basis,
        "verified_raw_close": str(comparison_mark),
        "source_response_sha256": quote["source_response_sha256"],
        "usage": "comparison_only_no_historical_valuation_or_posting"}
''').body[0]
    assert ast.dump(comparison_gate, include_attributes=False) == ast.dump(expected_gate, include_attributes=False)
    assert sum(isinstance(node, ast.Name) and node.id == "comparison_mark" for node in ast.walk(new)) == 3
    returned = new.body[-1]
    assert isinstance(returned, ast.Return) and isinstance(returned.value, ast.Dict)
    extras = [number for number, key in enumerate(returned.value.keys)
        if isinstance(key, ast.Constant) and key.value == "recovery_previous_reference_audit"]
    assert len(extras) == 1
    audit_number = extras[0]
    assert ast.unparse(returned.value.values[audit_number]) == "reference_audit"
    returned.value.keys.pop(audit_number)
    returned.value.values.pop(audit_number)

    class RestoreOriginalGapGates(ast.NodeTransformer):
        def visit_Name(self, node):
            if node.id == "unresolved_gaps":
                return ast.parse("state.get('observation_gaps')", mode="eval").body
            if node.id == "comparison_mark":
                return ast.parse("position.last_mark", mode="eval").body
            return node

    new = RestoreOriginalGapGates().visit(new)
    new.name = old.name
    assert ast.dump(new, include_attributes=False) == ast.dump(old, include_attributes=False)


def test_explicit_migration_preserves_twenty_positions_cash_clock_phase_and_all_history(recovery_case):
    case = recovery_case
    with pytest.raises(ValueError, match="Explicit new period"):
        runner.low_turnover_status(case["root"])
    assert _migrate(case)["status"] == "migrated_planned_awaiting_new_audited_close"
    status = runner.low_turnover_status(case["root"])
    state = status["state"]
    for key in ("accounts", "holding_clock", "rebalance_anchor", "signals", "attempts", "completed", "receipts", "valuations", "observation_gaps"):
        assert state[key] == case["old_state"][key]
    for key in ("model_sha256", "candidate_manifest_sha256", "initial_capital_per_strategy", "strategy", "execution_policy", "created_at", "first_possible_execution"):
        assert status["manifest"][key] == case["old_manifest"][key]
    assert len(state["accounts"]["model"]["positions"]) == 20
    assert state["attempts"]["2026-10-21_close"] == 3
    assert state["observation_gaps"]["2026-10-21"] == case["old_state"]["observation_gaps"]["2026-10-21"]
    assert state[recovery.STATE_KEY]["activation"] is None
    assert all(Path(path).read_bytes() == raw for path, raw in case["old_receipt_bytes"].items())


def test_planned_recovery_does_not_reopen_blocked_phase_or_execute_without_new_close(recovery_case):
    case = recovery_case
    _migrate(case)
    provider = GuardedProvider(case["paper"], case["evidence_dir"])
    original = deepcopy(_read_state(case["root"])["accounts"])
    replay = runner.run_low_turnover(case["root"], phase="open", provider=provider, now=clock("2026-10-22", "09:40"))
    assert replay["replayed"] and replay["status"] == "blocked_observation_gap_no_fill"
    before_close = runner.run_low_turnover(case["root"], phase="close", provider=provider,
        now=clock("2026-10-22", "14:59"))
    assert before_close["status"] == "not_completed_session"
    failed_old = runner.run_low_turnover(case["root"], phase="close", provider=provider,
        now=clock("2026-10-21", "16:30"))
    assert failed_old["status"] == "attempt_limit"
    missing = runner.run_low_turnover(case["root"], phase="open", provider=provider, now=clock("2026-10-23"))
    assert missing["status"] == "blocked_observation_gap_no_fill"
    assert provider.calls == 0
    state = _read_state(case["root"])
    assert state["accounts"] == original
    assert state[recovery.STATE_KEY]["activation"] is None
    assert "2026-10-22" not in state["signals"]


def test_real_new_complete_close_commits_guarded_rights_then_future_open_gate_clears(recovery_case, monkeypatch):
    case = recovery_case
    provider, closed = _activate(case)
    assert closed["status"] == "completed", closed
    state = runner.low_turnover_status(case["root"])["state"]
    activation = state[recovery.STATE_KEY]["activation"]
    assert activation["session"] == "2026-10-22"
    assert activation["historical_gap_unchanged"] is True
    assert activation["historical_fills_or_NAV_added"] is False
    assert recovery.pending_gaps(state) == {}
    assert state["completed"]["2026-10-22_close"]["receipt_sha256"]
    assert state["completed"]["2026-10-22_open"] == case["old_state"]["completed"]["2026-10-22_open"]
    assert state["observation_gaps"] == case["old_state"]["observation_gaps"]
    assert state["holding_clock"]["holding_started"] == case["old_state"]["holding_clock"]["holding_started"]
    assert state["accounts"]["model"]["cash"] == case["old_state"]["accounts"]["model"]["cash"]
    assert set(state["accounts"]["model"]["positions"]) == set(case["old_state"]["accounts"]["model"]["positions"])
    # First future open is correctly non-rebalance, rather than blocked by the
    # retained historical gap; the original 5-session anchor has not reset.
    result = runner.run_low_turnover(case["root"], phase="open", provider=provider, now=clock("2026-10-23"))
    assert result["status"] == "skipped_non_rebalance"
    assert provider.calls == 1
    # Advance only actual future phases.  A genuine D5 open reaches the real
    # execution path, with the original D10 holding lock still in force.
    for day in ("2026-10-23", "2026-10-26", "2026-10-27"):
        receipt = runner.run_low_turnover(case["root"], phase="close", provider=provider, now=clock(day, "16:30"))
        assert receipt["status"] == "completed", receipt
    monkeypatch.setattr(reports, "_now", lambda: clock("2026-10-27", "16:35"))
    report = reports.ashare_paper_summary(case["root"])
    assert report["next_possible_execution_session"] == "2026-10-28"
    assert report["next_execution_is_conditional"] is True
    assert report["new_orders_frozen_by_observation_gap"] is False
    assert report["observation_gaps"] == case["old_state"]["observation_gaps"]
    opened = runner.run_low_turnover(case["root"], phase="open", provider=provider, now=clock("2026-10-28"))
    assert opened["status"] == "completed", opened
    assert opened["network_called"] is True
    assert opened["accounts"]["model"]["turnover_audit"]["minimum_hold_sessions"] == 10
    assert all(Path(path).read_bytes() == raw for path, raw in case["old_receipt_bytes"].items())


def test_actual_runner_refuses_an_interrupted_migration_before_any_provider_call(recovery_case):
    case = recovery_case
    _migrate(case)
    _save(case["root"] / recovery.TRANSACTION, {"kind": "synthetic_interrupted_two_file_commit"})
    provider = GuardedProvider(case["paper"], case["evidence_dir"])
    with pytest.raises(ValueError, match="Interrupted engineering transaction"):
        runner.low_turnover_status(case["root"])
    with pytest.raises(ValueError, match="Interrupted engineering transaction"):
        runner.run_low_turnover(case["root"], phase="close", provider=provider, now=clock("2026-10-22", "16:30"))
    assert provider.calls == 0
    state = _read_state(case["root"])
    assert state["accounts"] == case["old_state"]["accounts"]
    assert state[recovery.STATE_KEY]["activation"] is None


@pytest.mark.parametrize("problem", ["partial_close", "missing_rights_guard", "unknown_held_right"])
def test_partial_or_unknown_held_evidence_never_activates_or_commits_recovery(recovery_case, problem):
    case = recovery_case
    _migrate(case)
    if problem == "unknown_held_right":
        state = _read_state(case["root"])
        held = sorted(state["accounts"]["model"]["positions"])[0]
        state["unresolved_actions"]["model"][held] = {"reason": "unknown_current_right"}
        legacy._save_state(case["root"], state)
    before = _read_state(case["root"])
    provider = GuardedProvider(case["paper"], case["evidence_dir"], problem=problem)
    receipt = runner.run_low_turnover(case["root"], phase="close", provider=provider,
        now=clock("2026-10-22", "16:30"))
    assert receipt["status"] == "failed", receipt
    state = runner.low_turnover_status(case["root"])["state"]
    assert state[recovery.STATE_KEY]["activation"] is None
    assert state["accounts"] == before["accounts"]
    assert state["valuations"] == before["valuations"]
    assert state["signals"] == before["signals"]
    assert "2026-10-22_close" not in state["completed"]
    assert recovery.pending_gaps(state) == before["observation_gaps"]
    assert state["attempts"]["2026-10-22_close"] == 1


def test_new_gap_after_activation_immediately_blocks_future_orders_and_report(recovery_case, monkeypatch):
    case = recovery_case
    provider, closed = _activate(case)
    assert closed["status"] == "completed", closed
    before = _read_state(case["root"])
    calls = provider.calls
    blocked = runner.run_low_turnover(case["root"], phase="open", provider=provider, now=clock("2026-10-26"))
    assert blocked["status"] == "blocked_observation_gap_no_fill"
    assert provider.calls == calls
    state = runner.low_turnover_status(case["root"])["state"]
    assert state["accounts"] == before["accounts"]
    assert set(state["observation_gaps"]) == {"2026-10-21", "2026-10-23"}
    assert set(recovery.pending_gaps(state)) == {"2026-10-23"}
    monkeypatch.setattr(reports, "_now", lambda: clock("2026-10-26"))
    report = reports.ashare_paper_summary(case["root"])
    assert report["new_orders_frozen_by_observation_gap"] is True
    assert set(report["unresolved_operational_gaps"]) == {"2026-10-23"}
    assert report["next_possible_execution_session"] is None


def test_report_retains_gap_and_costs_without_inventing_daily_or_stability_returns(recovery_case, monkeypatch):
    case = recovery_case
    _migrate(case)
    monkeypatch.setattr(reports, "_now", lambda: clock("2026-10-22", "16:00"))
    planned = reports.ashare_paper_summary(case["root"])
    assert planned["new_orders_frozen_by_observation_gap"] is True
    assert set(planned["unresolved_operational_gaps"]) == {"2026-10-21"}
    assert planned["next_possible_execution_session"] is None
    old_fees = deepcopy(planned["accounts"]["model"]["fees"])
    provider = GuardedProvider(case["paper"], case["evidence_dir"])
    closed = runner.run_low_turnover(case["root"], phase="close", provider=provider,
        now=clock("2026-10-22", "16:30"))
    assert closed["status"] == "completed", closed
    monkeypatch.setattr(reports, "_now", lambda: clock("2026-10-22", "16:35"))
    before = _tree_bytes(case["root"])
    report, curves = reports._build_report(case["root"])
    assert _tree_bytes(case["root"]) == before
    assert report["new_orders_frozen_by_observation_gap"] is False
    assert report["unresolved_operational_gaps"] == {}
    assert report["observation_gaps"] == case["old_state"]["observation_gaps"]
    assert report["next_possible_execution_session"] is None  # D2 is not a rebalance.
    assert report["accounts"]["model"]["fees"] == old_fees
    assert report["accounts"]["model"]["trade_count"] == 20
    rows = {row["session"]: row for row in report["daily_summary_rows"]}
    assert rows["2026-10-21"]["nav_lower"] is None
    assert rows["2026-10-22"]["daily_net_return"] is None
    assert rows["2026-10-22"]["diagnostic_status"] == "diagnostic_incomplete_or_noncontinuous_interval"
    assert rows["2026-10-22"]["previous_close_session"] == "2026-10-20"
    assert not any(row.get("session") == "2026-10-21" and row.get("nav_lower") is not None for row in curves)
    monitoring = report["daily_monitoring"]["accounts"]["model"]
    assert "historical_observation_gap_retained_audited_future_recovery" in monitoring["diagnostic_reasons"]
    for length in ("20", "60"):
        window = monitoring["stability"][length]
        assert window["status"] == "insufficient_continuous_complete_observations"
        assert window["observed_complete_sessions"] == 1
        assert window["net_return_lower"] is None
        assert window["max_drawdown_conservative"] is None
    assert monitoring["annualized_return"] is None
    assert monitoring["performance_qualification_claimed"] is False


def test_recovery_uses_missing_days_verified_raw_close_only_as_reference_without_backfilling(recovery_case, monkeypatch):
    case = recovery_case
    _migrate(case)
    before = _read_state(case["root"])
    old_positions = before["accounts"]["model"]["positions"]
    assert len(old_positions) == 20
    assert all(position["last_mark"] == "10" and position["last_mark_date"] == "2026-10-21"
        and position["last_mark_basis"] == "live_observed_quote" for position in old_positions.values())
    provider = MovingCloseProvider(case["paper"], case["evidence_dir"])
    receipt = runner.run_low_turnover(case["root"], phase="close", provider=provider,
        now=clock("2026-10-22", "16:30"))
    assert receipt["status"] == "completed", receipt
    assert len(receipt["recovery_previous_reference_audit"]) == 20
    for row in receipt["recovery_previous_reference_audit"].values():
        assert row["saved_mark"] == "10"
        assert row["saved_mark_basis"] == "live_observed_quote"
        assert row["previous_session"] == "2026-10-21"
        assert row["verified_raw_close"] == "11"
        assert row["usage"] == "comparison_only_no_historical_valuation_or_posting"
    state = runner.low_turnover_status(case["root"])["state"]
    assert recovery.pending_gaps(state) == {}
    assert state["observation_gaps"] == before["observation_gaps"]
    assert state["accounts"]["model"]["cash"] == before["accounts"]["model"]["cash"]
    assert state["holding_clock"]["holding_started"] == before["holding_clock"]["holding_started"]
    assert state["rebalance_anchor"] == before["rebalance_anchor"]
    for inst, position in state["accounts"]["model"]["positions"].items():
        assert position["quantity"] == old_positions[inst]["quantity"]
        assert position["last_mark"] == "12"
        assert position["last_mark_date"] == "2026-10-22"
        assert position["last_mark_basis"] == "observed_raw_close"
    assert [row["session"] for row in state["valuations"]] == ["2026-10-20", "2026-10-22"]
    assert "2026-10-21" not in state["signals"]
    assert all(Path(path).read_bytes() == raw for path, raw in case["old_receipt_bytes"].items())
    archived = _read_state(case["before"] / "paper")
    assert archived["accounts"]["model"]["positions"] == old_positions
    monkeypatch.setattr(reports, "_now", lambda: clock("2026-10-22", "16:35"))
    report = reports.ashare_paper_summary(case["root"])
    rows = {row["session"]: row for row in report["daily_summary_rows"]}
    assert rows["2026-10-21"]["nav_lower"] is None
    assert rows["2026-10-22"]["daily_net_return"] is None
    assert report["daily_monitoring"]["accounts"]["model"]["stability"]["20"]["net_return_lower"] is None


@pytest.mark.parametrize("problem", ["mismatched_preclose", "missing_previous_raw_guard", "wrong_previous_raw_hash", "new_gap"])
def test_reference_recovery_rejects_unknown_action_missing_or_mismatched_source_and_new_gap(recovery_case, monkeypatch, problem):
    case = recovery_case
    _migrate(case)
    before = _read_state(case["root"])
    captured = []
    real_close = recovery_close.close_after_explicit_recovery

    def observe_actual_close(*args, **kwargs):
        receipt = real_close(*args, **kwargs)
        captured.append((deepcopy(receipt), deepcopy(args[2])))
        return receipt

    monkeypatch.setattr(recovery_close, "close_after_explicit_recovery", observe_actual_close)
    provider = MovingCloseProvider(case["paper"], case["evidence_dir"], problem=problem)
    day = "2026-10-23" if problem == "new_gap" else "2026-10-22"
    receipt = runner.run_low_turnover(case["root"], phase="close", provider=provider, now=clock(day, "16:30"))
    assert receipt["status"] == "failed", receipt
    state = runner.low_turnover_status(case["root"])["state"]
    assert state[recovery.STATE_KEY]["activation"] is None
    assert state["accounts"] == before["accounts"]
    assert state["holding_clock"] == before["holding_clock"]
    assert state["valuations"] == before["valuations"]
    assert state["signals"] == before["signals"]
    assert day + "_close" not in state["completed"]
    assert "2026-10-21" in recovery.pending_gaps(state)
    assert all(Path(path).read_bytes() == raw for path, raw in case["old_receipt_bytes"].items())
    if problem == "mismatched_preclose":
        partial, working = captured[-1]
        assert partial["status"] == "partial"
        assert partial["valuations"]["model"]["valuation_complete"] is False
        bad_inst = sorted(before["accounts"]["model"]["positions"])[-1]
        unresolved = working["unresolved_actions"]["model"][bad_inst]
        assert unresolved["previous_raw_close"] == "11"
        assert unresolved["source_preclose"] == "10.5"
        assert partial["recovery_previous_reference_audit"][bad_inst]["verified_raw_close"] == "11"
    elif problem == "new_gap":
        assert set(recovery.pending_gaps(state)) == {"2026-10-21", "2026-10-22"}
        assert captured[-1][0]["status"] == "partial"
    else:
        assert not captured  # Source proof rejection precedes any close result.
