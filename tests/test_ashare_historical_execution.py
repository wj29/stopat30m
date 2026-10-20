"""Historical rule boundaries and frozen-ledger parity; no strategy fitting."""
import ast
from dataclasses import replace
from decimal import Decimal as D
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from stopat30m.backtest import ashare_execution as original
from stopat30m.backtest import ashare_historical_execution as ex
from stopat30m import ashare_historical_optimization_execution as opt


A = "SH600000"
CAL = pd.bdate_range("2022-12-01", "2025-01-03").strftime("%Y-%m-%d").tolist()
DAY = "2023-01-03"


def info(board="sse_main", *, session=DAY, **changes):
    row = ex.SecurityInfo(A, board, "2000-01-03", False,
        "2022-01-01T00:00:00+08:00", "synthetic prior-public status",
        status_session=session)
    return replace(row, **changes)


def quote(session=DAY, price="10", preclose="10", tradestatus="trading"):
    return ex.OpeningQuote(session, D(price) if price is not None else None,
        D(preclose), tradestatus, "synthetic historical source")


def execute(state=None, *, session=DAY, targets=None, security=None, opening=None, policy=None):
    return ex.execute_target_weights(state or ex.PortfolioState.initial(),
        session=session, signal_date=CAL[CAL.index(session) - 1],
        targets={A: D(".5")} if targets is None else targets,
        quotes={A: opening or quote(session)}, metadata={A: security or info(session=session)},
        calendar=CAL, policy=policy)


@pytest.mark.parametrize("session,rate", [
    ("2023-01-03", ".001"), ("2023-08-25", ".001"),
    ("2023-08-28", ".0005"), ("2024-12-31", ".0005"),
])
def test_stamp_effective_boundary_and_bilateral_transfer(session, rate):
    policy = ex.historical_policy(session)
    assert policy.sell_stamp_rate == D(rate)
    assert policy.transfer_rate == D(".00001")
    buy = ex.fees_for_order(10000, 10, "BUY", policy)
    sell = ex.fees_for_order(10000, 10, "SELL", policy)
    assert buy["stamp_tax"] == 0
    assert sell["stamp_tax"] == D("100000") * D(rate)
    assert buy["transfer_fee"] == sell["transfer_fee"] == 1


def test_execute_does_not_silently_use_post_2023_default_stamp_before_cut():
    state = ex.PortfolioState(D(100), {A: ex.Position(1000,
        last_mark=D(10), last_mark_date="2022-12-30")})
    sold = execute(state, targets={}, policy=original.ExecutionPolicy(slippage_bps=0))
    assert sold.trades[0]["stamp_tax"] == "10.00"
    assert sold.trades[0]["commission"] == "5.00"
    assert sold.trades[0]["transfer_fee"] == "0.10"
    assert sold.state.cash == D("10084.90")


@pytest.mark.parametrize("session", ["2022-12-30", "2025-01-02"])
def test_scope_rejects_unverified_earlier_and_forward_rules(session):
    with pytest.raises(ValueError, match="2023--2024"):
        ex.historical_policy(session)
    with pytest.raises(ValueError, match="2023--2024"):
        ex.price_limits(info(session=session), quote(session), session=session, calendar=CAL)


@pytest.mark.parametrize("board,is_st,rate", [
    ("sse_main", False, ".10"), ("sse_main", True, ".05"),
    ("szse_main", False, ".10"), ("szse_main", True, ".05"),
    ("chinext", False, ".20"), ("chinext", True, ".20"),
    ("star", False, ".20"), ("star", True, ".20"),
])
def test_day_specific_st_and_board_limits(board, is_st, rate):
    limits = ex.price_limits(info(board, is_st=is_st), quote(), session=DAY, calendar=CAL)
    assert limits["rate"] == D(rate)


@pytest.mark.parametrize("board", ["sse_main", "szse_main"])
def test_pre_registration_ipo_not_mislabelled_as_unrestricted(board):
    listing = "2023-03-27"
    security = info(board, session=listing, listing_date=listing)
    for session in CAL[CAL.index(listing):CAL.index(listing) + 5]:
        row = replace(security, status_session=session)
        with pytest.raises(ValueError, match="144%/64%"):
            ex.price_limits(row, quote(session), session=session, calendar=CAL)
        assert not execute(session=session, security=row).trades
    sixth = CAL[CAL.index(listing) + 5]
    assert ex.price_limits(replace(security, status_session=sixth), quote(sixth),
        session=sixth, calendar=CAL)["rate"] == D(".10")


@pytest.mark.parametrize("board,listing", [
    ("sse_main", "2023-04-10"), ("szse_main", "2023-04-10"),
    ("star", "2023-01-03"), ("chinext", "2023-01-03"),
])
def test_registration_and_growth_board_initial_five_sessions_are_excluded(board, listing):
    security = info(board, session=listing, listing_date=listing)
    for session in CAL[CAL.index(listing):CAL.index(listing) + 5]:
        row = replace(security, status_session=session)
        limits = ex.price_limits(row, quote(session), session=session, calendar=CAL)
        assert limits["no_limit_ipo"] and limits["upper"] is limits["lower"] is None
        assert not execute(session=session, security=row).trades
    sixth = CAL[CAL.index(listing) + 5]
    assert not ex.price_limits(replace(security, status_session=sixth), quote(sixth),
        session=sixth, calendar=CAL)["no_limit_ipo"]


@pytest.mark.parametrize("board,weight,quantity", [
    ("sse_main", ".0151", 100), ("szse_main", ".0151", 100),
    ("chinext", ".0151", 100), ("star", ".0199", 0), ("star", ".0201", 201),
])
def test_main_hundred_share_and_star_two_hundred_plus_one_lots(board, weight, quantity):
    result = execute(targets={A: D(weight)}, security=info(board))
    assert sum(row["quantity"] for row in result.trades) == quantity


def test_t1_same_day_lock_and_next_real_session_unlock():
    bought = execute()
    blocked = execute(bought.state, targets={})
    assert not blocked.trades and blocked.state.positions[A].quantity == 5000
    assert "T+1" in blocked.orders[0]["reason"]
    sold = execute(blocked.state, session="2023-01-04", targets={})
    assert sold.trades[0]["quantity"] == 5000 and not sold.state.positions


def test_unknown_status_preclose_and_adverse_limits_remain_blocked():
    assert not execute(security=info(is_st=None)).trades
    assert not execute(security=info(status_session="2023-01-04")).trades
    assert not execute(opening=quote(tradestatus="unknown")).trades
    assert not execute(opening=quote(price="11")).trades
    with pytest.raises(ValueError, match="preclose"):
        ex.price_limits(info(), replace(quote(), preclose=None), session=DAY, calendar=CAL)
    with pytest.raises(ValueError, match="Unsupported board"):
        ex.price_limits(info("bse"), quote(), session=DAY, calendar=CAL)


def test_verified_suspension_only_waives_staleness_and_keeps_missing_mark_incomplete():
    state = ex.PortfolioState(D(10), {A: ex.Position(100,
        last_mark=D(10), last_mark_date="2023-01-03")}, session=DAY)
    with pytest.raises(ValueError, match="Unknown held raw-price gap"):
        ex.mark_to_close(state, session="2023-03-01", prices={}, calendar=CAL)
    marked = ex.mark_to_close(state, session="2023-03-01", prices={}, calendar=CAL,
        verified_suspended=(A,))
    assert marked.equity_lower_bound == D(1010)
    assert marked.valuation_complete is False
    assert marked.positions[0]["valuation_source"] == "carried_observed_mark"


def test_historical_adapter_cannot_accept_live_clock():
    with pytest.raises(ValueError, match="research-only"):
        ex.execute_target_weights(ex.PortfolioState.initial(), session=DAY,
            signal_date="2022-12-30", targets={}, quotes={}, metadata={}, calendar=CAL,
            now="2023-01-03T09:35:00+08:00")


def test_cash_action_valuation_and_rounding_algorithms_are_same_ast_as_frozen_source():
    old = Path(original.__file__).read_bytes()
    assert hashlib.sha256(old).hexdigest() == ex.BASE_ENGINE_SHA256
    def definitions(source):
        return {node.name: ast.dump(node, include_attributes=False) for node in ast.parse(source).body
            if isinstance(node, (ast.FunctionDef, ast.ClassDef))}
    before, after = definitions(old), definitions(Path(ex.__file__).read_bytes())
    changed = {name for name in before if before[name] != after[name]}
    assert changed == {"price_limits", "execute_target_weights"}
    # Independent namespaces: the immutable original still refuses 2023 dates.
    with pytest.raises(ValueError, match="modern"):
        original.price_limits(info(), quote(), session=DAY, calendar=CAL)


def test_historical_rank_buffer_copy_changes_only_import_docstring_and_audit_version():
    from stopat30m import ashare_optimization_execution as previous
    before = ast.parse(Path(previous.__file__).read_text())
    after = ast.parse(Path(opt.__file__).read_text())
    def normalized(tree):
        tree.body = [node for node in tree.body if not isinstance(node, ast.ImportFrom)
            and not (isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant)
                and isinstance(node.value.value, str))
            and not (isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "POLICY" for t in node.targets))]
        return ast.dump(tree, include_attributes=False)
    assert normalized(before) == normalized(after)


def test_historical_rank_buffer_keeps_existing_exact_odd_quantity():
    state = ex.PortfolioState(D(5000), {A: ex.Position(399, last_mark=D(10),
        last_mark_date="2023-01-03")}, session="2023-01-03")
    plan = opt.build_rank_buffer_plan(state, signal_date="2023-01-03", ranked_instruments=[A],
        raw_signal_prices={A: D(10)}, boards={A: "sse_main"}, signal_equity=D(8990),
        policy=opt.RankBufferPolicy(top_k=1, keep_rank=2),
        execution_policy=ex.historical_policy("2023-01-04"))
    result = opt.execute_rank_buffer_plan(state, plan=plan, session="2023-01-04",
        quotes={A: quote("2023-01-04")}, metadata={A: info(session="2023-01-04")},
        calendar=CAL, policy=ex.historical_policy("2023-01-04"))
    assert result.state.positions[A].quantity == 399 and result.state.cash == 5000
    assert result.trades == []


def test_reviewed_rules_declaration_checks_original_source_bytes_and_real_ledger(tmp_path):
    original_path = Path(__file__).resolve().parents[1] / "docs/research/a4_historical_execution_rules.json"
    value = json.loads(original_path.read_text())
    # The test is portable; official downloaded byte evidence is an integration
    # prerequisite, not an implicit network dependency of this unit test.
    evidence = tmp_path / "source.bin"
    evidence.write_bytes(b"explicit source fixture")
    for name, proof in value["bound_inputs"].items():
        if name not in {"frozen_a2_engine", "historical_engine"}:
            proof.update(path=str(evidence), sha256=hashlib.sha256(evidence.read_bytes()).hexdigest())
        else:
            proof["path"] = str((original_path.parent / proof["path"]).resolve())
    manifest = tmp_path / "rules.json"
    manifest.write_text(json.dumps(value))
    assert ex.verify_historical_rules(manifest)["status"] == "verified"
    evidence.write_bytes(b"changed")
    with pytest.raises(ValueError, match="checksum"):
        ex.verify_historical_rules(manifest)
    value["bound_inputs"]["historical_engine"]["sha256"] = "0" * 64
    manifest.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="does not bind"):
        ex.verify_historical_rules(manifest)


def test_reviewed_rules_declaration_refuses_missing_core_sources(tmp_path):
    path = tmp_path / "incomplete.json"
    path.write_text(json.dumps({"schema_version": 1, "status": "verified",
        "rule_version": ex.RULE_VERSION, "supported_sessions": ["2023-01-01", "2024-12-31"],
        "bound_inputs": {}}))
    with pytest.raises(ValueError, match="incomplete"):
        ex.verify_historical_rules(path)
