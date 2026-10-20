import json
from pathlib import Path

import pandas as pd
import pytest

from stopat30m.data import a5_ipo_prefix as ipo
from stopat30m.data.ashare_daily import FIELDS
from stopat30m.data.ashare_repair import FACTOR_DOCUMENT


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n")


def fixture(tmp_path):
    root = tmp_path / "prices"; root.mkdir()
    factors = tmp_path / "factors"; factors.mkdir()
    guards = tmp_path / "guards"; guards.mkdir()
    dates = ["2023-07-26", "2023-07-27", "2023-07-28", "2024-06-03"]
    calendar = tmp_path / "calendar.txt"; calendar.write_text("\n".join(dates) + "\n")
    manifest = {"source": "baostock", "instruments": ["SH688612"], "generation": "price-generation", "as_of_date": "2026-10-20",
        "request_start": "2017-01-01", "request_end": "2026-10-19", "responses": {}, "status": "partial_not_publishable"}
    pg = {}; originals = {}
    def guarded(path, payload, request, *, daily=False):
        echo = {**request, **({"adjustflag": ""} if daily else {})}
        anomalies = [{"date": day, "requested_mode": "1", "returned_row_mode": "3"} for day in dates[:2]] if request.get("adjustflag") == "1" else []
        save(path, {"status": "verified", "server_error_code": "0", "request": request, "server_echo": echo,
            "rows_verified": len(payload["rows"]), "complete_row_iteration_observed": True,
            "started_at": "2026-10-20T06:00:00+00:00", "finished_at": "2026-10-20T06:00:01+00:00",
            "row_mode_anomalies_observed": anomalies})
    for mode, flag in [("raw", "3"), ("hfq", "1")]:
        request = {"code": "sh.688612", "fields": FIELDS, "start_date": "2017-01-01", "end_date": "2026-10-19", "frequency": "d", "adjustflag": flag}
        rows = []
        for n, day in enumerate(dates):
            price = 10 + n; adjusted = price * (1.1 if n == 3 else 1) if mode == "hfq" else price
            rows.append([day, "sh.688612", *[str(adjusted)] * 4, "100", str(price * 100),
                         "3" if mode == "raw" or n < 2 else "1", "1", "0"])
        payload = {"request": request, "fields": FIELDS.split(","), "rows": rows,
            "generation": "price-generation", "fetched_at": "2026-10-20T06:00:02+00:00"}
        path = root / ("SH688612_" + mode + ".json"); save(path, payload); originals[mode] = ipo._proof(path)
        manifest["responses"]["SH688612_" + mode] = {"file": path.name, "sha256": ipo._proof(path)["sha256"],
            "rows": len(rows), "fetched_at": payload["fetched_at"]}
        gp = guards / (mode + ".json"); guarded(gp, payload, request, daily=True); pg[mode] = ipo._proof(gp)
    save(root / "collection.json", manifest)
    method_pdf = tmp_path / "method.pdf"; method_pdf.write_bytes(b"%PDF-synthetic-official-method-fixture")
    method = tmp_path / "method.json"; save(method, {"file": method_pdf.name, "sha256": ipo._proof(method_pdf)["sha256"], "source_url": FACTOR_DOCUMENT})
    fm = {"kind": "baostock_official_factor_identity_evidence", "source": "baostock", "status": "complete", "instrument": "SH688612",
        "generation": "factor-generation", "request_start": "2017-01-01", "request_end": "2026-10-19", "responses": {},
        "method_evidence": {"receipt_path": str(method), "receipt_sha256": ipo._proof(method)["sha256"]}}
    fg = {}
    for key in ["identity", "factors"]:
        request = {"code": "sh.688612"} if key == "identity" else {"code": "sh.688612", "start_date": "2017-01-01", "end_date": "2026-10-19"}
        fields = ["code", "code_name", "ipoDate", "outDate", "type", "status"] if key == "identity" else ["code", "dividOperateDate", "foreAdjustFactor", "backAdjustFactor", "adjustFactor"]
        rows = [["sh.688612", "fixture", "2023-07-26", "", "1", "1"]] if key == "identity" else [
            ["sh.688612", "2023-07-28", "1", "1.000000", "1"], ["sh.688612", "2024-06-03", "1", "1.1", "1.1"]]
        payload = {"query": "query_stock_basic" if key == "identity" else "query_adjust_factor", "request": request,
            "fields": fields, "rows": rows, "generation": fm["generation"], "fetched_at": "2026-10-20T06:00:02+00:00"}
        path = factors / (key + ".json"); save(path, payload)
        fm["responses"][key] = {"file": path.name, "sha256": ipo._proof(path)["sha256"], "fetched_at": payload["fetched_at"], "rows": len(rows)}
        gp = guards / (key + ".json"); guarded(gp, payload, {**request, "code_name": ""} if key == "identity" else request); fg[key] = ipo._proof(gp)
    save(factors / "factor_evidence.json", fm)
    contract = {"kind": "a5_verified_two_session_ipo_factor_derivation_contract", "status": "source_only_frozen_before_full_ohlc_derivation",
        "instrument": "SH688612", "spec": ipo._proof(ipo.SPEC_PATH), "implementation": ipo._proof(ipo.__file__),
        "original_collection_manifest": ipo._proof(root / "collection.json"), "factor_evidence_manifest": ipo._proof(factors / "factor_evidence.json"),
        "original_raw_response": originals["raw"], "original_hfq_response": originals["hfq"], "price_guard_proofs": pg,
        "factor_guard_proofs": fg, "calendar": ipo._proof(calendar), "prefix_sessions": dates[:2]}
    path = tmp_path / "contract.json"; save(path, contract)
    return root, factors, path


def test_two_day_ipo_derives_entire_history_from_independent_factors_retaining_flags(tmp_path):
    root, factors, contract = fixture(tmp_path); before = (root / "SH688612_hfq.json").read_bytes()
    raw, hfq, proof = ipo.derive_two_session_ipo_hfq(root, "SH688612", factors, contract_path=contract)
    assert (root / "SH688612_hfq.json").read_bytes() == before
    assert proof["status"] == "passed" and len(proof["original_mode_anomalies"]) == 2
    assert raw.close.iloc[-1] == "13" and hfq.close.iloc[-1] == pytest.approx(14.3)
    assert hfq.adjustflag.tolist() == ["3", "3", "1", "1"]
    assert proof["original_flags_modified"] is False and proof["source_rows_removed"] is False


@pytest.mark.parametrize("mutation", ["original_bytes", "contract", "factor_bytes", "guard_bytes", "calendar_bytes", "wrong_instrument"])
def test_two_day_contract_never_waives_wrong_identity_changed_sources_or_capture_guard(tmp_path, mutation):
    root, factors, contract = fixture(tmp_path)
    path = {"original_bytes": root / "SH688612_hfq.json", "factor_bytes": factors / "factors.json",
        "guard_bytes": tmp_path / "guards/hfq.json", "calendar_bytes": tmp_path / "calendar.txt"}.get(mutation)
    if path is not None:path.write_bytes(path.read_bytes() + b" ")
    if mutation == "contract":
        document = json.loads(contract.read_text()); document["prefix_sessions"] = ["2023-07-26", "2023-07-28"]; save(contract, document)
    with pytest.raises(ValueError):ipo.derive_two_session_ipo_hfq(root, "SH688613" if mutation == "wrong_instrument" else "SH688612", factors, contract_path=contract)


@pytest.mark.parametrize("mutation", ["first_factor_day", "nonunit_first_factor", "wrong_ipo", "extra_mode", "ohlc", "nonprice"])
def test_independently_valid_original_hashes_do_not_waive_prefix_or_full_history_math(tmp_path, mutation):
    root, factors, contract_path = fixture(tmp_path)
    contract = json.loads(contract_path.read_text())
    if mutation in {"first_factor_day", "nonunit_first_factor", "wrong_ipo"}:
        key = "identity" if mutation == "wrong_ipo" else "factors"
        path = factors / (key + ".json"); payload = json.loads(path.read_text())
        if mutation == "wrong_ipo":payload["rows"][0][2] = "2023-07-25"
        if mutation == "first_factor_day":payload["rows"][0][1] = "2023-07-27"
        if mutation == "nonunit_first_factor":payload["rows"][0][3] = "1.000001"
        save(path, payload); fm = json.loads((factors / "factor_evidence.json").read_text())
        fm["responses"][key]["sha256"] = ipo._proof(path)["sha256"]; save(factors / "factor_evidence.json", fm)
        contract["factor_evidence_manifest"] = ipo._proof(factors / "factor_evidence.json")
    else:
        path = root / "SH688612_hfq.json"; payload = json.loads(path.read_text())
        if mutation == "extra_mode":
            payload["rows"][2][8] = "3"
            gp = tmp_path / "guards/hfq.json"; guard = json.loads(gp.read_text())
            guard["row_mode_anomalies_observed"].append({"date": "2023-07-28", "requested_mode": "1", "returned_row_mode": "3"})
            save(gp, guard); contract["price_guard_proofs"]["hfq"] = ipo._proof(gp)
        if mutation == "ohlc":payload["rows"][-1][5] = "14.4"
        if mutation == "nonprice":payload["rows"][-1][6] = "101"
        save(path, payload); manifest = json.loads((root / "collection.json").read_text())
        manifest["responses"]["SH688612_hfq"]["sha256"] = ipo._proof(path)["sha256"]; save(root / "collection.json", manifest)
        contract["original_collection_manifest"] = ipo._proof(root / "collection.json")
        contract["original_hfq_response"] = ipo._proof(path)
    save(contract_path, contract)
    with pytest.raises(ValueError):ipo.derive_two_session_ipo_hfq(root, "SH688612", factors, contract_path=contract_path)
