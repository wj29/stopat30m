import json

import numpy as np
import pandas as pd
import pytest

from scripts.audit_ashare_data_readiness import change_preclose_counterexamples, verified_change_semantics


def test_change_cannot_recover_ex_right_reference(tmp_path):
    """The raw close ratio retains a dividend gap; it is not pctChg/preclose."""
    feature = tmp_path / "features/sh600000"
    feature.mkdir(parents=True)
    calendar = tmp_path / "calendars"
    calendar.mkdir()
    (calendar / "day.txt").write_text("2023-01-03\n2023-01-04\n")
    for name, values in {"close": [10, 8.5], "factor": [1, 1], "change": [np.nan, -.15]}.items():
        np.array([0, *values], dtype="<f4").tofile(feature / (name + ".day.bin"))
    raw = pd.DataFrame([{"instrument": "SH600000", "date": "2023-01-04", "preclose": "8.00",
        "source_complete_quote": True, "source_response_sha256": "proof"}])
    rows, aggregate = change_preclose_counterexamples(tmp_path, raw)
    assert len(rows) == 1
    assert rows[0]["inferred_previous_raw_close"] == pytest.approx(10)
    assert rows[0]["actual_source_preclose"] == 8
    assert aggregate[0]["counterexample_sessions"] == 1
    assert "preclose" not in rows[0]  # Diagnostic output cannot masquerade as adapter input.


def test_missing_change_is_reported_without_guessing(tmp_path):
    raw = pd.DataFrame([{"instrument": "SH600000", "date": "2023-01-04", "preclose": "8.00",
        "source_complete_quote": True, "source_response_sha256": "proof"}])
    rows, aggregate = change_preclose_counterexamples(tmp_path, raw)
    assert rows == []
    assert aggregate == [{"instrument": "SH600000", "status": "change_unavailable"}]


def test_normalizer_interpretation_requires_original_pinned_bytes(tmp_path):
    declaration = tmp_path / "source.json"
    declaration.write_text(json.dumps({"evidence_files": {"pinned_official_yahoo_collector.py": {"sha256": "changed"}}}))
    with pytest.raises(ValueError, match="fresh code review"):
        verified_change_semantics(declaration, tmp_path)
