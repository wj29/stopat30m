"""Small source receipts exercise derivation, not strategy returns or network."""
from datetime import datetime, timezone
import json

import numpy as np
import pytest

from stopat30m.data.ashare_daily import FIELDS, _save, _sha, SHANGHAI, load_collection_frames
from stopat30m.data.ashare_repair import FACTOR_DOCUMENT, derive_ipo_hfq, compose_rebuild_sources

A, B, C = "SH688223", "SH601727", "SZ000001"
DAYS = ["2022-01-26", "2022-01-27", "2022-01-28"]


def collection(root, instruments, *, mixed=False, generation="original"):
    root.mkdir()
    instant = datetime.now(timezone.utc).isoformat()
    manifest = {"schema_version": 1, "source": "baostock", "generation": generation,
        "request_start": "2000-01-04", "request_end": DAYS[-1],
        "as_of_date": datetime.now(SHANGHAI).date().isoformat(), "instruments": sorted(instruments),
        "target_selection": {"kind": "explicit_download_targets"}, "responses": {}, "failures": {}, "status": "complete"}
    for symbol in instruments:
        for mode, flag in (("raw", "3"), ("hfq", "1")):
            code = symbol[:2].lower() + "." + symbol[2:]
            rows = []
            for i, day in enumerate(DAYS):
                factor = 2 if mode == "hfq" and i == 2 else 1
                values = [str((10 + i) * factor)] * 4
                returned = "3" if mixed and symbol == A and i == 0 else flag
                rows.append([day, code, *values, "1000", str((10 + i) * 1000), returned, "1", "0"])
            payload = {"request": {"code": code, "fields": FIELDS, "start_date": "2000-01-04",
                "end_date": DAYS[-1], "frequency": "d", "adjustflag": flag}, "fields": FIELDS.split(","),
                "rows": rows, "generation": generation, "fetched_at": instant}
            key = symbol + "_" + mode
            filename = key + ".json"
            digest = _save(root / filename, payload)
            manifest["responses"][key] = {"file": filename, "sha256": digest, "fetched_at": instant, "rows": 3}
    _save(root / "collection.json", manifest)
    return root


@pytest.fixture
def repair(tmp_path):
    original = collection(tmp_path / "original", [A, B, C], mixed=True)
    evidence = tmp_path / "factor-evidence"
    evidence.mkdir()
    generation, instant = "factor-source-generation", datetime.now(timezone.utc).isoformat()
    receipt = {"kind": "baostock_official_factor_identity_evidence", "source": "baostock", "instrument": A,
        "generation": generation, "request_start": "2000-01-04", "request_end": DAYS[-1],
        "responses": {}, "status": "complete", "method_source_url": FACTOR_DOCUMENT}
    data = {
        "identity": {"query": "query_stock_basic", "request": {"code": "sh.688223"},
            "fields": ["code", "ipoDate", "outDate", "type", "status"],
            "rows": [["sh.688223", DAYS[0], "", "1", "1"]]},
        "factors": {"query": "query_adjust_factor", "request": {"code": "sh.688223",
            "start_date": "2000-01-04", "end_date": DAYS[-1]},
            "fields": ["code", "dividOperateDate", "backAdjustFactor"],
            "rows": [["sh.688223", DAYS[1], "1"], ["sh.688223", DAYS[2], "2"]]}}
    for key, payload in data.items():
        payload.update(generation=generation, fetched_at=instant)
        digest = _save(evidence / (key + ".json"), payload)
        receipt["responses"][key] = {"file": key + ".json", "sha256": digest,
            "fetched_at": instant, "rows": len(payload["rows"])}
    _save(evidence / "factor_evidence.json", receipt)
    return original, evidence


def test_ipo_repair_reconstructs_all_ohlc_and_preserves_original_flags_bytes_and_dates(repair):
    original, evidence = repair
    before = {path.name: path.read_bytes() for path in original.glob("*.json")}
    with pytest.raises(ValueError, match="adjustment mode"):
        load_collection_frames(original)
    raw, adjusted, proof = derive_ipo_hfq(original, A, evidence)
    assert raw.date.tolist() == adjusted.date.tolist() == DAYS
    assert raw.adjustflag.eq("3").all() and "adjustflag" not in adjusted
    np.testing.assert_allclose(adjusted.close, [10, 11, 24])
    assert proof["source_rows_removed"] is False and proof["original_flags_modified"] is False
    assert proof["original_anomaly"] == {"date": DAYS[0], "returned_flag": "3", "requested_flag": "1"}
    assert max(proof["max_absolute_OHLC_validation_errors"].values()) == 0
    assert {path.name: path.read_bytes() for path in original.glob("*.json")} == before


def mutate_evidence(evidence, key, mutation):
    payload = json.loads((evidence / (key + ".json")).read_bytes())
    mutation(payload)
    digest = _save(evidence / (key + ".json"), payload)
    receipt = json.loads((evidence / "factor_evidence.json").read_bytes())
    receipt["responses"][key]["sha256"] = digest
    _save(evidence / "factor_evidence.json", receipt)


@pytest.mark.parametrize("defect", ["ipo", "first_factor", "factor_date", "factor_value", "request", "checksum"])
def test_ipo_derivation_rejects_unproven_identity_coverage_values_and_tampering(repair, defect):
    original, evidence = repair
    if defect == "ipo":
        mutate_evidence(evidence, "identity", lambda x: x["rows"][0].__setitem__(1, "2022-01-25"))
    elif defect == "first_factor":
        mutate_evidence(evidence, "factors", lambda x: x["rows"][0].__setitem__(2, "1.1"))
    elif defect == "factor_date":
        mutate_evidence(evidence, "factors", lambda x: x["rows"][0].__setitem__(1, "2022-01-28"))
    elif defect == "factor_value":
        mutate_evidence(evidence, "factors", lambda x: x["rows"][1].__setitem__(2, "3"))
    elif defect == "request":
        mutate_evidence(evidence, "factors", lambda x: x["request"].__setitem__("start_date", DAYS[1]))
    else:
        with (evidence / "identity.json").open("ab") as stream:
            stream.write(b" ")
    with pytest.raises(ValueError):
        derive_ipo_hfq(original, A, evidence)


def test_composition_keeps_each_original_generation_time_and_full_selection(repair, tmp_path):
    original, evidence = repair
    replacement = collection(tmp_path / "replacement", [B], generation="new-complete-generation")
    raw, adjusted, receipt = compose_rebuild_sources(original,
        replacements={B: replacement}, factor_repairs={A: evidence}, receipt_dir=tmp_path / "receipt")
    assert set(raw.instrument) == set(adjusted.instrument) == {A, B, C}
    assert len(raw) == len(adjusted) == 9
    assert receipt["supplier_generation_claimed"] is False and receipt["atomic_supplier_vintage_verified"] is False
    assert receipt["securities"][C]["generation"] == "original"
    assert receipt["securities"][B]["generation"] == "new-complete-generation"
    assert receipt["securities"][A]["factor_generation"] == "factor-source-generation"
    old = json.loads((original / "collection.json").read_bytes())
    assert receipt["securities"][C]["responses"]["raw"]["fetched_at"] == old["responses"][C + "_raw"]["fetched_at"]
    assert receipt["receipt_sha256"] == _sha((tmp_path / "receipt/rebuild_composite.json").read_bytes())
    with pytest.raises(ValueError, match="new directory"):
        compose_rebuild_sources(original, replacements={B: replacement}, factor_repairs={A: evidence}, receipt_dir=tmp_path / "receipt")


def test_ipo_proof_does_not_waive_any_later_mixed_mode_even_when_numbers_match(repair):
    original, evidence = repair
    path = original / (A + "_hfq.json")
    payload = json.loads(path.read_bytes())
    payload["rows"][1][8] = "3"
    digest = _save(path, payload)
    manifest = json.loads((original / "collection.json").read_bytes())
    manifest["responses"][A + "_hfq"]["sha256"] = digest
    _save(original / "collection.json", manifest)
    with pytest.raises(ValueError, match="narrow verified IPO"):
        derive_ipo_hfq(original, A, evidence)
