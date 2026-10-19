"""Offline forward-only candidate and persistent A-share orchestration tests."""
from datetime import datetime, timezone
import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from stopat30m import ashare_forward as af

NOW = datetime(2026, 10, 19, 9, tzinfo=timezone.utc)


def test_mature_five_session_open_label_excludes_six_future_signal_sessions():
    # Explicit synthetic sessions exercise index arithmetic, not a calendar feed.
    days = pd.to_datetime(["2026-09-29", "2026-09-30", "2026-10-08", "2026-10-09",
        "2026-10-12", "2026-10-13", "2026-10-14", "2026-10-15", "2026-10-16", "2026-10-19"])
    assert af.mature_signal_end(days, "2026-10-19") == "2026-10-09"
    with pytest.raises(ValueError, match="observed"):
        af.mature_signal_end(days, "2026-10-17")
    with pytest.raises(ValueError, match="Insufficient"):
        af.mature_signal_end(days[:5], "2026-10-12")


@pytest.fixture
def candidate(tmp_path, monkeypatch):
    days = pd.bdate_range("2026-09-01", "2026-10-19")
    mature = af.mature_signal_end(days, "2026-10-19")
    rng = np.random.default_rng(71)
    index = pd.MultiIndex.from_product([days[days <= mature], [f"SH{600000 + n:06d}" for n in range(12)]], names=["datetime", "instrument"])
    names = [f"alpha{n}" for n in range(158)]
    raw = pd.DataFrame(rng.normal(size=(len(index), 158)), index=index, columns=names)
    center = raw.median().to_numpy()
    scale = (raw.sub(center).abs().median().to_numpy() + 1e-12) * 1.4826
    features = raw.sub(center).div(scale).clip(-3, 3).fillna(0)
    labels = pd.DataFrame({"label": raw.iloc[:, 0] * .3 + raw.iloc[:, 1] * .1}, index=index)
    learned = pd.concat({"feature": features, "label": labels}, axis=1)
    class Dataset:
        def prepare(self, segment, col_set=None, data_key=None):
            assert segment == "train" and data_key == "learn"
            return learned
    Robust = type("RobustZScoreNorm", (), {})
    norm = Robust()
    norm.fit_start_time = "2026-09-01"
    norm.fit_end_time = mature
    norm.fields_group = "feature"
    norm.clip_outlier = True
    norm.mean_train, norm.std_train = center, scale
    fillna = type("Fillna", (), {})()
    handler = SimpleNamespace(infer_processors=[norm, fillna])
    source = tmp_path / "source"
    source.mkdir()
    (source / "bound-input.bin").write_bytes(b"immutable synthetic source")
    snapshot = SimpleNamespace(path=source, dataset_dir=source, manifest_sha256="a" * 64,
        manifest={"created_at": "2026-10-19T08:00:00+00:00", "source": {"kind": "synthetic_fixture"}, "price_semantics": {"factor": "qprice/rawprice"}})
    context = {"snapshot": snapshot, "dataset": source, "calendar": days,
        "mature_end": mature, "used_files": {"bound-input.bin": af._sha(source / "bound-input.bin")}}
    monkeypatch.setattr(af, "_snapshot_context", lambda *args: context)
    monkeypatch.setattr(af, "_training_dataset", lambda *args: (Dataset(), handler))
    bundle = af.fit_ashare_forward_candidate(source, tmp_path / "candidate", train_start="2026-09-01", cutoff="2026-10-19", now=NOW)
    return bundle, raw, context


def test_forward_candidate_is_native_mature_train_only_and_has_no_oos(candidate):
    bundle, raw, _ = candidate
    assert bundle.manifest["training_rows"] == len(raw)
    assert bundle.protocol["train_signal_end"] == "2026-10-09"
    assert bundle.protocol["last_label_execution_end"] == "2026-10-19"
    assert bundle.protocol["historical_oos_claim"] is False
    assert bundle.protocol["training_role"] == "forward_only_no_historical_oos"
    assert len(bundle.manifest["feature_names"]) == 158
    assert not list(bundle.path.rglob("*.pkl"))
    assert bundle.model.spec.num_boost_round == 100
    assert bundle.model.metadata["training_role"] == "forward_only_no_historical_oos"


def test_frozen_preprocessing_predictions_ignore_future_feature_rows(candidate):
    bundle, raw, _ = candidate
    past = raw.iloc[:24].copy()
    expected = bundle.predict_features(past)
    future = raw.iloc[-12:].copy()
    future.index = pd.MultiIndex.from_product([[pd.Timestamp("2026-10-20")], future.index.get_level_values("instrument")], names=raw.index.names)
    future *= 1000000
    combined = pd.concat([past, future])
    actual = bundle.predict_features(combined).loc[past.index]
    pd.testing.assert_series_equal(actual, expected)
    loaded = af.load_ashare_forward_candidate(bundle.path)
    pd.testing.assert_series_equal(loaded.predict_features(past), expected)


@pytest.mark.parametrize("name", ["processors.json", "model/model.txt", "protocol.json"])
def test_native_model_preprocessor_and_protocol_tampering_is_rejected(candidate, name):
    bundle, _, _ = candidate
    path = bundle.path / name
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ValueError, match="checksum"):
        af.load_ashare_forward_candidate(bundle.path)


def test_inference_feature_order_infinity_and_duplicates_are_rejected(candidate):
    bundle, raw, _ = candidate
    with pytest.raises(ValueError, match="order"):
        bundle.predict_features(raw.iloc[:2, ::-1])
    bad = raw.iloc[:2].copy()
    bad.iloc[0, 0] = np.inf
    with pytest.raises(ValueError, match="Infinite"):
        bundle.predict_features(bad)
    with pytest.raises(ValueError, match="index"):
        bundle.predict_features(pd.concat([raw.iloc[:1], raw.iloc[:1]]))


def test_refit_requires_postclose_cutoff_and_preserves_frozen_bundle(tmp_path, candidate):
    bundle, _, _ = candidate
    with pytest.raises(FileExistsError):
        af.fit_ashare_forward_candidate(tmp_path, bundle.path, train_start="2026-09-01", cutoff="2026-10-19", now=NOW)
    with pytest.raises(ValueError, match="completed"):
        af.fit_ashare_forward_candidate(tmp_path, tmp_path / "future", cutoff="2026-10-20", now=NOW)
    with pytest.raises(ValueError, match="completed"):
        af.fit_ashare_forward_candidate(tmp_path, tmp_path / "intraday", cutoff="2026-10-19", now=datetime(2026, 10, 19, 2, tzinfo=timezone.utc))
    with pytest.raises(ValueError, match="timezone"):
        af.fit_ashare_forward_candidate(tmp_path, tmp_path / "naive", cutoff="2026-10-19", now=datetime(2026, 10, 19, 17))


@pytest.fixture
def paper(tmp_path, monkeypatch, candidate):
    bundle, _, _ = candidate
    days = pd.date_range("2026-10-16", "2026-11-02").strftime("%Y-%m-%d").tolist()
    sessions = set(pd.bdate_range("2026-10-16", "2026-11-02").strftime("%Y-%m-%d"))
    # Explicit fixture flags, never passed as a production weekday-calendar adapter.
    calendar = af.Calendar.from_rows([{"date": day, "is_open": day in sessions} for day in days],
        days[0], days[-1], source="synthetic_fixture", source_url="fixture://calendar")
    calendar_path = tmp_path / "calendar.json"
    calendar.save(calendar_path)
    evidence = tmp_path / "current-universe-receipt.json"
    evidence.write_text('{"synthetic_fixture":true}')
    universe = [f"SH{600000 + n:06d}" for n in range(20)]
    references = [{"path": str(evidence), "sha256": af._sha(evidence), "url": "fixture://public-source"}]
    monkeypatch.setattr(af, "_signal_bar_eligibility", lambda candidate, batch, instruments, session: (instruments, {}, "b" * 64))
    def targets(candidate, snapshot, day, eligible):
        weights = {key: 1 / len(eligible) for key in eligible}
        return {"model": weights, "momentum": weights.copy()}, {"synthetic_fixture": True}
    monkeypatch.setattr(af, "_candidate_targets", targets)
    folder = tmp_path / "ashare-paper"
    af.initialize_ashare_forward(bundle.path, folder, calendar_path, instruments=universe,
        universe_evidence=references, now=datetime(2026, 10, 19, 10, tzinfo=timezone.utc))
    class Provider:
        def __init__(self):
            self.calls = []
            self.fail = False
            self.stale = False
            self.missing_close = set()
            self.preclose = "10.00"
            self.corporate_actions = []
        def make(self, phase, session, instruments, now):
            self.calls.append((phase, session, tuple(instruments)))
            if self.fail:
                raise ValueError("synthetic source unavailable")
            stamp = session + ("T15:00:00+08:00" if phase == "close" else "T09:35:00+08:00")
            if self.stale and phase == "open":
                stamp = session + "T09:30:00+08:00"
            quote_source = "synthetic_daily_raw" if phase == "close" else "tencent.public_level1_observation"
            quotes = {key: {"session": session, "open": "10.00", "preclose": self.preclose,
                "close": "10.70" if session > "2026-10-19" else "10.00", "factor": "1.0",
                "execution_price": "10.50", "execution_price_basis": "live_observed_quote",
                "tradestatus": "trading", "source": quote_source, "asof": stamp,
                "fetched_at": now.isoformat()} for key in instruments if key not in self.missing_close or phase != "close"}
            metadata = {key: {"instrument": key, "board": "sse_main", "listing_date": "2000-01-01",
                "is_st": False, "known_at": session + "T09:00:00+08:00", "source": "synthetic_metadata",
                "status_session": session, "listing_session_number": 5000} for key in instruments}
            return {"phase": phase, "session": session, "source": quote_source, "fetched_at": now.isoformat(),
                "quotes": quotes, "metadata": metadata, "evidence": references,
                "corporate_actions": self.corporate_actions,
                "instruments": universe, "universe_evidence": references, "snapshot_path": "synthetic_current_snapshot"}
        def fetch_close(self, session, instruments, now):
            return self.make("close", session, instruments, now)
        def fetch_open(self, session, instruments, now):
            return self.make("open", session, instruments, now)
    return folder, Provider(), universe


def close_paper(paper, day="2026-10-19"):
    folder, provider, _ = paper
    clock = datetime.fromisoformat(day + "T18:00:00+08:00")
    return af.run_ashare_forward(folder, phase="close", provider=provider, now=clock)


def open_paper(paper, day="2026-10-20"):
    folder, provider, _ = paper
    return af.run_ashare_forward(folder, phase="open", provider=provider,
        now=datetime.fromisoformat(day + "T09:35:00+08:00"))


def test_two_isolated_integer_accounts_execute_observed_price_not_past_open_and_replay(paper):
    from decimal import Decimal
    folder, provider, _ = paper
    assert close_paper(paper)["eligible_count"] == 20
    result = open_paper(paper)
    assert result["status"] == "completed"
    for name, account in result["accounts"].items():
        assert account["trades"] and {row["reference_price"] for row in account["trades"]} == {"10.50"}
        assert all(Decimal(row["execution_price"]) >= Decimal("10.50") for row in account["trades"])
        assert all(row["quantity"] % 100 == 0 and row["live_order"] is False for row in account["trades"])
        state = af.ashare_forward_status(folder)["state"]["accounts"][name]
        spent = sum(Decimal(row["notional"]) + Decimal(row["total_fee"]) for row in account["trades"])
        assert Decimal(state["cash"]) == Decimal("100000") - spent
    calls = len(provider.calls)
    before = (folder / "state.json").read_bytes()
    replay = open_paper(paper)
    assert replay["replayed"] and len(provider.calls) == calls
    assert (folder / "state.json").read_bytes() == before
    status = af.ashare_forward_status(folder)
    assert status["real_account_accessed"] is False and status["live_order_count"] == 0


def test_closed_session_and_nonrebalance_open_make_no_provider_request(paper):
    folder, provider, _ = paper
    assert close_paper(paper)["status"] == "completed"
    assert open_paper(paper)["status"] == "completed"
    assert close_paper(paper, "2026-10-20")["rebalance"] is False
    calls = len(provider.calls)
    assert open_paper(paper, "2026-10-21")["status"] == "skipped_non_rebalance"
    assert len(provider.calls) == calls
    result = af.run_ashare_forward(folder, phase="open", provider=provider,
        now=datetime.fromisoformat("2026-10-24T09:35:00+08:00"))
    assert result["status"] == "skipped_non_session" and len(provider.calls) == calls


def test_source_failure_retry_limit_preserves_accounts_and_survives_restart(paper):
    folder, provider, _ = paper
    provider.fail = True
    initial = af.ashare_forward_status(folder)["state"]["accounts"]
    for expected in (1, 2, 3):
        result = close_paper(paper)
        assert result["status"] == "failed" and result["attempt"] == expected
        assert af.ashare_forward_status(folder)["state"]["accounts"] == initial
    assert close_paper(paper)["status"] == "attempt_limit"
    assert len(provider.calls) == 3
    assert len(af.ashare_forward_status(folder)["state"]["receipts"]) == 3


def test_stale_open_cannot_fill_or_replace_selected_names(paper):
    folder, provider, _ = paper
    close_paper(paper)
    selected = af.ashare_forward_status(folder)["state"]["signals"]["2026-10-19"]["targets"]
    provider.stale = True
    result = open_paper(paper)
    assert result["status"] == "failed" and "current verified live" in result["error"]
    state = af.ashare_forward_status(folder)["state"]
    assert state["signals"]["2026-10-19"]["targets"] == selected
    assert all(not account["positions"] and account["cash"] == "100000.00" for account in state["accounts"].values())


def test_expired_open_does_not_backfill_and_next_close_uses_actual_cash(paper):
    folder, provider, _ = paper
    close_paper(paper)
    before_calls = len(provider.calls)
    result = af.run_ashare_forward(folder, phase="open", provider=provider,
        now=datetime.fromisoformat("2026-10-20T16:00:00+08:00"))
    assert result["status"] == "expired_no_fill" and len(provider.calls) == before_calls
    close = close_paper(paper, "2026-10-20")
    assert close["status"] == "completed"
    assert all(value["equity_lower_bound"] == "100000.00" for value in close["valuations"].values())
    assert all(not value["positions"] for value in af.ashare_forward_status(folder)["state"]["accounts"].values())


def test_missing_held_close_is_partial_and_freezes_orders_without_fake_nav(paper):
    folder, provider, universe = paper
    close_paper(paper); open_paper(paper)
    provider.missing_close.add(universe[0])
    result = close_paper(paper, "2026-10-20")
    assert result["status"] == "partial"
    assert all(not row["valuation_complete"] for row in result["valuations"].values())
    signal = af.ashare_forward_status(folder)["state"]["signals"]["2026-10-20"]
    assert signal["allowed"] == {"model": False, "momentum": False}
    assert universe[0] in signal["exclusions"]


def test_changed_exright_reference_blocks_complete_nav_until_verified_entitlement(paper):
    folder, provider, _ = paper
    close_paper(paper); open_paper(paper); close_paper(paper, "2026-10-20")
    provider.preclose = "10.00"  # Yesterday's observed close is 10.70.
    result = close_paper(paper, "2026-10-21")
    assert result["status"] == "partial"
    assert all(not row["valuation_complete"] for row in result["valuations"].values())
    assert af.ashare_forward_status(folder)["state"]["unresolved_actions"]
    assert any("unresolved_corporate_action" in warning for warning in result["warnings"])


def test_lock_and_committed_receipt_tampering_fail_before_provider(paper):
    folder, provider, _ = paper
    with af._lock(folder):
        with pytest.raises(ValueError, match="already running"):
            close_paper(paper)
    assert not provider.calls
    close_paper(paper)
    state = af.ashare_forward_status(folder)["state"]
    from pathlib import Path
    receipt = Path(state["receipts"][0]["path"])
    receipt.write_bytes(receipt.read_bytes() + b" ")
    with pytest.raises(ValueError, match="receipt changed"):
        af.ashare_forward_status(folder)


def test_actual_official_alpha158_refit_and_frozen_future_inference_are_causal(tmp_path):
    import hashlib
    import tarfile
    from stopat30m.data.qlib_snapshot import prepare_qlib_snapshot
    symbols = ["SH600000", "SZ000001"]
    dates = pd.bdate_range("2020-01-02", periods=330)
    source = tmp_path / "synthetic-qlib"
    (source / "calendars").mkdir(parents=True)
    (source / "calendars/day.txt").write_text("\n".join(dates.strftime("%Y-%m-%d")) + "\n")
    (source / "instruments").mkdir()
    members = "".join(f"{key}\t{dates[0].date()}\t{dates[-1].date()}\n" for key in symbols)
    (source / "instruments/all.txt").write_text(members)
    (source / "instruments/csi300.txt").write_text(members)
    for n, instrument in enumerate(symbols):
        folder = source / "features" / instrument.lower()
        folder.mkdir(parents=True)
        t = np.arange(len(dates))
        close = 10 + t * .005 + .04 * np.sin(t / (n + 2))
        values = {"open": close * .999, "high": close * 1.004, "low": close * .996,
            "close": close, "volume": 100000 + t * 37 + 1000 * np.sin(t / 7),
            "vwap": close * 1.0005, "factor": np.ones(len(t))}
        for field, array in values.items():
            (folder / (field + ".day.bin")).write_bytes(np.r_[0, array].astype("<f4").tobytes())
    archive = tmp_path / "synthetic.tar.gz"
    with tarfile.open(archive, "w:gz") as stream:
        stream.add(source, arcname="cn_data")
    snapshot = prepare_qlib_snapshot(archive, tmp_path / "snapshots",
        source={"kind": "community", "synthetic_fixture_only": True},
        expected_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(), max_extract_bytes=10 * 2 ** 20)
    bundle = af.fit_ashare_forward_candidate(snapshot.path, tmp_path / "official-candidate",
        train_start=str(dates[70].date()), cutoff=str(dates[-1].date()))
    assert bundle.manifest["training_rows"] > 400
    assert len(bundle.model.feature_names) == 158
    from stopat30m.data.qlib_incremental import read_qlib_security
    last_date = str(dates[-1].date())
    quote = read_qlib_security(snapshot.dataset_dir, symbols[0]).loc[last_date]
    batch = {"snapshot_path": str(snapshot.path), "fetched_at": datetime.now(timezone.utc).isoformat(),
        "quotes": {symbols[0]: {"close": float(quote["close"]), "factor": 1},
                   symbols[1]: {"close": 999999, "factor": 1}}}
    valid, exclusions, source_sha = af._signal_bar_eligibility(bundle, batch, symbols, last_date)
    assert valid == [symbols[0]] and symbols[1] in exclusions and source_sha == snapshot.manifest_sha256
    earlier = str(dates[250].date())
    before = af._prediction_features(SimpleNamespace(dataset_dir=source), earlier, symbols)
    expected = bundle.predict_features(before)
    # Modify only later fixture bytes, never the immutable snapshot used to fit.
    for path in (source / "features").rglob("*.day.bin"):
        data = np.frombuffer(path.read_bytes(), dtype="<f4").copy()
        data[1 + 251:] *= 100
        path.write_bytes(data.tobytes())
    after = af._prediction_features(SimpleNamespace(dataset_dir=source), earlier, symbols)
    pd.testing.assert_frame_equal(before, after)
    pd.testing.assert_series_equal(bundle.predict_features(after), expected)


def test_second_account_failure_cannot_partially_commit_first_account(paper, monkeypatch):
    from stopat30m.backtest import ashare_execution as execution
    folder, _, _ = paper
    close_paper(paper); open_paper(paper)
    before = af.ashare_forward_status(folder)["state"]["accounts"]
    original = execution.mark_to_close
    calls = []
    def interrupted(*args, **kwargs):
        calls.append(1)
        if len(calls) == 2:
            raise ValueError("synthetic crash after calculating the first account")
        return original(*args, **kwargs)
    monkeypatch.setattr(execution, "mark_to_close", interrupted)
    assert close_paper(paper, "2026-10-20")["status"] == "failed"
    assert af.ashare_forward_status(folder)["state"]["accounts"] == before


def test_partial_close_can_retry_without_duplicate_daily_nav_or_orders(paper):
    folder, provider, universe = paper
    close_paper(paper); open_paper(paper)
    provider.missing_close.add(universe[0])
    assert close_paper(paper, "2026-10-20")["status"] == "partial"
    provider.missing_close.clear()
    result = close_paper(paper, "2026-10-20")
    assert result["status"] == "completed" and result["attempt"] == 2
    state = af.ashare_forward_status(folder)["state"]
    assert [row["session"] for row in state["valuations"]].count("2026-10-20") == 1
    assert state["signals"]["2026-10-20"]["allowed"] == {"model": True, "momentum": True}


def test_equal_weight_decimal_rounding_never_exceeds_the_available_equity():
    from decimal import Decimal
    weights = af._equal_weights([f"SH{600000 + n}" for n in range(13)])
    assert sum(map(Decimal, weights.values())) <= Decimal(1)


def dividend_fixture(paper, monkeypatch, *, pay="2026-10-22", listing="2026-10-23"):
    """Public-source-shaped evidence, with no request or real account access."""
    folder, provider, universe = paper
    instrument = universe[0]
    original = provider.make
    def with_action(phase, session, instruments, now):
        batch = original(phase, session, instruments, now)
        row = batch["quotes"].get(instrument)
        if row and session >= "2026-10-21":
            row.update(preclose="9.55" if session == "2026-10-21" else "9.60",
                close="9.60", open="9.55", execution_price="9.60")
        return batch
    monkeypatch.setattr(provider, "make", with_action)
    proof = original("close", "2026-10-21", universe, datetime.fromisoformat("2026-10-21T18:00:00+08:00"))["evidence"][0]
    event = {"event_id": "fixture-dividend-1", "instrument": instrument,
        "record_date": "2026-10-20", "ex_date": "2026-10-21", "pay_date": pay,
        "listing_date": listing, "cash_per_share": "0.20", "bonus_ratio": "0.10",
        "stock_dividend_ratio": "0", "capital_reserve_ratio": "0.10",
        "known_at": "2026-10-19T23:59:59+08:00", "source_response_sha256": proof["sha256"]}
    return event


def test_verified_ex_date_rights_then_cash_payment_and_bonus_listing_are_idempotent(paper, monkeypatch):
    from decimal import Decimal
    folder, provider, universe = paper
    event = dividend_fixture(paper, monkeypatch)
    close_paper(paper); open_paper(paper); close_paper(paper, "2026-10-20")
    before = af.ashare_forward_status(folder)["state"]["accounts"]["model"]
    record_shares = before["positions"][universe[0]]["quantity"]
    provider.preclose = "10.70"
    provider.corporate_actions = [event]
    result = close_paper(paper, "2026-10-21")
    assert result["status"] == "completed"
    assert [row["operation"] for row in result["corporate_actions"]["model"]] == ["register_cash", "register_bonus"]
    state = af.ashare_forward_status(folder)["state"]["accounts"]["model"]
    assert state["cash"] == before["cash"]  # Receivable is not spendable.
    assert state["positions"][universe[0]]["quantity"] == record_shares
    assert all(row["record_date_shares"] == record_shares for row in state["entitlements"].values())
    assert all(row["status"] == "pending" for row in state["entitlements"].values())
    assert all(row["valuation_complete"] for row in result["valuations"].values())
    assert not af.ashare_forward_status(folder)["state"]["unresolved_actions"]["model"]
    # Events survive omission from a later source batch, including after sale.
    provider.corporate_actions = []
    payment = close_paper(paper, "2026-10-22")
    assert payment["status"] == "completed"
    paid = af.ashare_forward_status(folder)["state"]["accounts"]["model"]
    expected = Decimal(before["cash"]) + Decimal(record_shares) * Decimal("0.20") * Decimal("0.80")
    assert Decimal(paid["cash"]) == expected
    assert paid["entitlements"][event["event_id"] + ":cash"]["status"] == "settled"
    assert paid["entitlements"][event["event_id"] + ":bonus"]["status"] == "pending"
    assert close_paper(paper, "2026-10-22")["replayed"]
    listed = close_paper(paper, "2026-10-23")
    assert listed["status"] == "completed"
    final = af.ashare_forward_status(folder)["state"]["accounts"]["model"]
    assert final["positions"][universe[0]]["quantity"] == record_shares * 11 // 10
    assert final["entitlements"][event["event_id"] + ":bonus"]["status"] == "settled"
    assert Decimal(final["cash"]) == expected
    assert close_paper(paper, "2026-10-23")["replayed"]


def test_same_day_ex_payment_listing_happen_before_open_execution(paper, monkeypatch):
    folder, provider, universe = paper
    event = dividend_fixture(paper, monkeypatch, pay="2026-10-21", listing="2026-10-21")
    close_paper(paper); open_paper(paper); close_paper(paper, "2026-10-20")
    before = af.ashare_forward_status(folder)["state"]["accounts"]["model"]
    provider.preclose, provider.corporate_actions = "10.70", [event]
    # Exercise the same entry operation as a scheduled rebalance; do not alter
    # production cadence or retrospectively inject another live job.
    root, manifest, state, _, calendar = af._read_paper(folder)
    signal = state["signals"]["2026-10-20"]
    signal["targets"] = {name: {} for name in manifest["strategies"]}
    signal["signal_sha256"] = af._digest({key: value for key, value in signal.items() if key != "signal_sha256"})
    clock = datetime.fromisoformat("2026-10-21T09:35:00+08:00")
    batch = provider.fetch_open("2026-10-21", universe, clock)
    af._batch(batch, "open", "2026-10-21", clock, manifest)
    result = af._open_operation(manifest, state, calendar, batch, clock)
    actions = result["accounts"]["model"]["corporate_actions"]
    assert [row["operation"] for row in actions] == ["register_cash", "settle_cash", "register_bonus", "settle_bonus"]
    sale = next(row for row in result["accounts"]["model"]["trades"] if row["instrument"] == universe[0])
    original_quantity = before["positions"][universe[0]]["quantity"]
    assert sale["side"] == "SELL" and sale["quantity"] == original_quantity * 11 // 10
    assert not state["accounts"]["model"]["positions"]
    assert all(row["status"] == "settled" for row in state["accounts"]["model"]["entitlements"].values())


def test_event_evidence_and_conflicting_terms_fail_without_ledger_mutation(paper, monkeypatch):
    folder, provider, _ = paper
    event = dividend_fixture(paper, monkeypatch)
    close_paper(paper); open_paper(paper); close_paper(paper, "2026-10-20")
    provider.preclose = "10.70"
    initial = af.ashare_forward_status(folder)["state"]["accounts"]
    provider.corporate_actions = [{**event, "source_response_sha256": "0" * 64}]
    assert close_paper(paper, "2026-10-21")["status"] == "failed"
    assert af.ashare_forward_status(folder)["state"]["accounts"] == initial
    provider.corporate_actions = [event]
    assert close_paper(paper, "2026-10-21")["status"] == "completed"
    registered = af.ashare_forward_status(folder)["state"]["accounts"]
    provider.corporate_actions = [{**event, "cash_per_share": "0.30"}]
    result = close_paper(paper, "2026-10-22")
    assert result["status"] == "failed" and "conflicting" in result["error"]
    assert af.ashare_forward_status(folder)["state"]["accounts"] == registered


def test_verified_late_same_session_evidence_can_resolve_partial_reference(paper, monkeypatch):
    folder, provider, _ = paper
    event = dividend_fixture(paper, monkeypatch)
    close_paper(paper); open_paper(paper); close_paper(paper, "2026-10-20")
    provider.preclose = "10.70"
    assert close_paper(paper, "2026-10-21")["status"] == "partial"
    provider.corporate_actions = [event]
    assert close_paper(paper, "2026-10-21")["status"] == "completed"
    assert not af.ashare_forward_status(folder)["state"]["unresolved_actions"]["model"]


def test_record_date_ownership_survives_sale_and_missed_dates_are_not_backfilled(paper, monkeypatch):
    from stopat30m.backtest import ashare_execution as execution
    folder, provider, universe = paper
    event = dividend_fixture(paper, monkeypatch)
    close_paper(paper); open_paper(paper); close_paper(paper, "2026-10-20")
    _, manifest, state, _, calendar = af._read_paper(folder)
    account = execution.PortfolioState.from_dict(state["accounts"]["model"])
    record_quantity = account.positions.pop(universe[0]).quantity  # Fixture sale after record date.
    clock = datetime.fromisoformat("2026-10-21T18:00:00+08:00")
    batch = provider.fetch_close("2026-10-21", universe, clock)
    batch["corporate_actions"] = [event]
    events = af._action_book(state, batch, clock)
    sessions = calendar.sessions(calendar.coverage_start, calendar.coverage_end)
    registered, _ = af._apply_actions(account, events, manifest=manifest, session="2026-10-21", calendar=sessions, clock=clock)
    assert registered.entitlements[event["event_id"] + ":cash"]["record_date_shares"] == record_quantity
    assert registered.entitlements[event["event_id"] + ":bonus"]["record_date_shares"] == record_quantity
    assert universe[0] in af._account_instruments(registered.to_dict())
    missing = execution.PortfolioState.from_dict(state["accounts"]["model"])
    missing.holdings_history.pop("2026-10-20")
    with pytest.raises(ValueError, match="record-date"):
        af._apply_actions(missing, events, manifest=manifest, session="2026-10-21", calendar=sessions, clock=clock)
    with pytest.raises(ValueError, match="retrospective"):
        af._apply_actions(account, events, manifest=manifest, session="2026-10-22", calendar=sessions,
            clock=datetime.fromisoformat("2026-10-22T18:00:00+08:00"))


def test_events_before_isolated_cash_initialization_create_no_entitlement(paper, monkeypatch):
    folder, provider, universe = paper
    event = dividend_fixture(paper, monkeypatch)
    event.update(record_date="2026-10-16", ex_date="2026-10-19", known_at="2026-10-15T23:59:59+08:00")
    provider.corporate_actions = [event]
    result = close_paper(paper)
    assert result["status"] == "completed"
    assert all(not account["entitlements"] for account in af.ashare_forward_status(folder)["state"]["accounts"].values())
    assert result["corporate_actions"]["model"][0]["status"] == "no_preinitialization_entitlement"


def test_verified_cash_plus_stock_dividend_tax_override_is_evidenced_and_consistent(paper, monkeypatch, tmp_path):
    from decimal import Decimal
    folder, provider, universe = paper
    event = dividend_fixture(paper, monkeypatch, pay="2026-10-21")
    tax_proof = tmp_path / "official-tax-fixture.json"
    tax_proof.write_text('{"fixture_only":true,"taxable_bonus_face_value":1}')
    event.update(cash_dividend_tax_rate="0.24", tax_evidence_sha256=af._sha(tax_proof),
        stock_dividend_ratio="0.10", capital_reserve_ratio="0")
    original = provider.make
    def with_tax(*args):
        batch = original(*args)
        batch["evidence"] = [*batch["evidence"], {"path": str(tax_proof), "sha256": af._sha(tax_proof)}]
        return batch
    monkeypatch.setattr(provider, "make", with_tax)
    close_paper(paper); open_paper(paper); close_paper(paper, "2026-10-20")
    before = af.ashare_forward_status(folder)["state"]["accounts"]["model"]
    provider.corporate_actions, provider.preclose = [event], "10.70"
    assert close_paper(paper, "2026-10-21")["status"] == "completed"
    account = af.ashare_forward_status(folder)["state"]["accounts"]["model"]
    quantity = before["positions"][universe[0]]["quantity"]
    assert Decimal(account["cash"]) == Decimal(before["cash"]) + quantity * Decimal("0.20") * Decimal("0.76")
    entitlement = account["entitlements"][event["event_id"] + ":cash"]
    assert entitlement["tax_rate"] == "0.24" and entitlement["evidence_sha256"] == af._sha(tax_proof)
    event["cash_dividend_tax_rate"] = "1.01"
    result = close_paper(paper, "2026-10-22")
    assert result["status"] == "failed" and "Cash cannot cover" in result["error"]


def test_ashare_forward_cli_help_is_registered_and_lazy():
    from click.testing import CliRunner
    from stopat30m.research_cli import research_group
    runner = CliRunner()
    for command in ("ashare-forward-fit", "ashare-forward-status", "ashare-paper-run"):
        result = runner.invoke(research_group, [command, "--help"])
        assert result.exit_code == 0 and "--help" in result.output
    assert "--now" not in runner.invoke(research_group, ["ashare-paper-run", "--help"]).output


def test_evidenced_virtual_ex_reference_does_not_change_actual_cash_entitlement(paper, monkeypatch, tmp_path):
    from decimal import Decimal
    folder, provider, universe = paper
    event = dividend_fixture(paper, monkeypatch)
    reference_proof = tmp_path / "virtual-reference-fixture.json"
    reference_proof.write_text('{"fixture_only":true,"virtual_cash_per_share":"0.10"}')
    event.update(reference_cash_per_share="0.10", reference_evidence_sha256=af._sha(reference_proof))
    original = provider.make
    def with_reference(*args):
        batch = original(*args)
        if args[1] == "2026-10-21":
            batch["quotes"][universe[0]]["preclose"] = "9.64"  # (10.70 - virtual .10) / 1.10.
        batch["evidence"] = [*batch["evidence"], {"path": str(reference_proof), "sha256": af._sha(reference_proof)}]
        return batch
    monkeypatch.setattr(provider, "make", with_reference)
    close_paper(paper); open_paper(paper); close_paper(paper, "2026-10-20")
    provider.preclose, provider.corporate_actions = "10.70", [event]
    result = close_paper(paper, "2026-10-21")
    assert result["status"] == "completed"
    account = af.ashare_forward_status(folder)["state"]["accounts"]["model"]
    right = account["entitlements"][event["event_id"] + ":cash"]
    assert right["cash_per_share"] == "0.20"
    assert Decimal(right["gross_cash"]) == right["record_date_shares"] * Decimal("0.20")
    assert not af.ashare_forward_status(folder)["state"]["unresolved_actions"]["model"]


def test_taxable_stock_dividend_without_nominal_tax_evidence_is_refused(paper, monkeypatch):
    folder, provider, _ = paper
    event = dividend_fixture(paper, monkeypatch)
    event.update(stock_dividend_ratio="0.10", capital_reserve_ratio="0")
    close_paper(paper); open_paper(paper); close_paper(paper, "2026-10-20")
    before = af.ashare_forward_status(folder)["state"]["accounts"]
    provider.corporate_actions, provider.preclose = [event], "10.70"
    result = close_paper(paper, "2026-10-21")
    assert result["status"] == "failed" and "nominal-value tax" in result["error"]
    assert af.ashare_forward_status(folder)["state"]["accounts"] == before


def test_missed_whole_close_permanently_freezes_new_orders_but_records_current_nav(paper):
    folder, provider, _ = paper
    close_paper(paper); open_paper(paper); close_paper(paper, "2026-10-20")
    before = af.ashare_forward_status(folder)["state"]["accounts"]
    provider.preclose = "10.70"
    # No 10/21 close: the last mark is older than yesterday, so the reference
    # check alone cannot establish that intervening corporate actions were seen.
    result = close_paper(paper, "2026-10-22")
    assert result["status"] == "partial"
    state = af.ashare_forward_status(folder)["state"]
    assert set(state["observation_gaps"]) == {"2026-10-21"}
    assert state["signals"]["2026-10-22"]["allowed"] == {"model": False, "momentum": False}
    assert all(row["valuation_complete"] for row in result["valuations"].values())
    assert all(state["accounts"][name]["positions"].keys() == before[name]["positions"].keys() for name in before)
    calls = len(provider.calls)
    blocked = open_paper(paper, "2026-10-23")
    assert blocked["status"] == "blocked_observation_gap_no_fill" and not blocked["network_called"]
    assert len(provider.calls) == calls
    later = close_paper(paper, "2026-10-23")
    assert later["status"] == "partial"
    final = af.ashare_forward_status(folder)["state"]
    assert set(final["observation_gaps"]) == {"2026-10-21"}
    assert not any(final["signals"]["2026-10-23"]["allowed"].values())


def test_failed_close_is_a_gap_next_session_without_provider_retry(paper):
    folder, provider, universe = paper
    close_paper(paper); open_paper(paper)
    provider.fail = True
    assert close_paper(paper, "2026-10-20")["status"] == "failed"
    provider.fail = False
    calls = len(provider.calls)
    assert open_paper(paper, "2026-10-21")["status"] == "blocked_observation_gap_no_fill"
    assert len(provider.calls) == calls
