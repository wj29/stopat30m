"""Temporal seams cannot hide behind a matching per-day numerator/factor."""
import json

import numpy as np
import pandas as pd
import pytest

from stopat30m.data import a5_adjusted_prices as audit


A = "SZ000620"


def run(monkeypatch, normalized, raw_close, preclose, observed=None):
    days = ["2023-10-13", "2023-10-16", "2023-10-17"][:len(normalized)]
    raw = pd.DataFrame({"instrument": A, "date": days,
        "close": [str(value) for value in raw_close], "preclose": [str(value) for value in preclose],
        "source_complete_quote": observed if observed is not None else True,
        "source_response_sha256": "synthetic-original"})
    stored = pd.DataFrame({"close": np.asarray(normalized, dtype=np.float32)}, index=days)
    monkeypatch.setattr(audit, "read_qlib_security", lambda *args: stored)
    return audit.audit_temporal_adjustments("fixture", raw, [A], start=days[0], end=days[-1])


def test_actual_zero_return_cannot_become_a_38_percent_model_return_via_factor_seam(monkeypatch):
    result = run(monkeypatch, [.16698291897773743, .23086653649806976], [1.6, 1.6], [1.6, 1.6])
    assert result["status"] == "blocked"
    assert result["failures"][0]["reason"] == "unexplained_normalized_price_reference_seam"
    assert float(result["failures"][0]["normalized_observed_return"]) > .38
    assert result["failures"][0]["source_preclose_return"] == "0"


def test_actual_bonus_exchange_reference_is_consistent_with_unchanged_adjusted_close(monkeypatch):
    result = run(monkeypatch, [1., 1.], [10., 5.], [10., 5.])
    assert result["status"] == "passed"
    assert result["checked_adjacent_observed_quotes"] == 1


def test_one_cent_published_reference_margin_is_not_reestimated_from_anomaly_counts(monkeypatch):
    assert run(monkeypatch, [1., 1.], [10., 10.], [10., 10.01])["status"] == "passed"
    assert run(monkeypatch, [1., 1.], [10., 10.], [10., 10.02])["status"] == "blocked"


def test_missing_actual_model_bar_is_not_skipped_when_comparing_the_next_quote(monkeypatch):
    result = run(monkeypatch, [1., np.nan, 1.], [10., 10., 10.], [10., 10., 10.])
    failures = [row for row in result["failures"] if row["reason"] == "missing_or_non_float32_actual_model_close"]
    assert {row["session"] for row in failures} == {"2023-10-16", "2023-10-17"}
    assert failures[-1]["previous_observed_session"] == "2023-10-16"


def test_confirmed_source_halt_has_no_fabricated_normalized_close(monkeypatch):
    result = run(monkeypatch, [1., np.nan, 1.01], [10., 10., 10.1], [10., 10., 10.],
                 observed=[True, False, True])
    assert result["status"] == "passed"
    assert result["checked_adjacent_observed_quotes"] == 1


def test_independently_observed_partial_session_is_still_checked(monkeypatch):
    days = ["2023-10-13", "2023-10-16"]
    raw = pd.DataFrame({"instrument": A, "date": days, "close": "10", "preclose": "10",
        "source_complete_quote": [True, False], "source_observed_market_quote": [True, True],
        "source_response_sha256": "original"})
    stored = pd.DataFrame({"close": np.asarray([1., 1.4], dtype=np.float32)}, index=days)
    monkeypatch.setattr(audit, "read_qlib_security", lambda *args: stored)
    result = audit.audit_temporal_adjustments("fixture", raw, [A], start=days[0], end=days[-1])
    assert result["status"] == "blocked" and result["checked_adjacent_observed_quotes"] == 1


def test_float32_quantization_bounds_are_positive_outward_and_contain_the_stored_value():
    from decimal import Decimal
    value = float(np.float32(.1))
    low, high = audit.float32_close_interval(value)
    assert Decimal.from_float(value) > low > 0
    assert Decimal.from_float(value) < high
    implied_low, implied_high = audit.implied_reference_interval("10", value, value)
    assert implied_low < 10 < implied_high


@pytest.mark.parametrize("value", [0, -1, np.nan, np.inf, .123456789])
def test_nonfinite_negative_and_unstored_float64_values_cannot_claim_float32_error_bounds(value):
    with pytest.raises(ValueError):
        audit.float32_close_interval(value)


def test_wider_source_error_margin_is_rejected_even_if_document_calls_it_the_same_rule(tmp_path):
    source = json.loads(audit.DEFAULT_SPEC.read_text())
    source["source_reference_margin_cny"] = "0.02"
    path = tmp_path / "wider.json"
    path.write_text(json.dumps(source))
    with pytest.raises(ValueError, match="declared fixed source rule"):
        audit.load_adjustment_bridge_spec(path)


def test_single_source_observation_is_a_reported_boundary_not_a_temporal_pass(monkeypatch):
    result = run(monkeypatch, [1.], [10.], [10.])
    assert result["status"] == "blocked"
    assert result["initial_source_boundaries"][0]["session"] == "2023-10-13"
    assert result["failures"][0]["reason"] == "no_verified_adjacent_original_model_quote_pair"
