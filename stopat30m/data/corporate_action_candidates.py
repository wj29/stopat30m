"""Offline extraction of public F10 candidates, never an authoritative ledger.

This module performs no HTTP requests and does not modify prices or returns.
Vendor cash amounts are per record-date unit. Rounded conversion ratios and
missing announcement dates must be verified before an economic ledger uses them.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
import hashlib
from io import StringIO
import json
from pathlib import Path
import re

import pandas as pd


def _day(value):
    text = str(value).strip()
    if text in {"", "-", "--", "nan", "None"}:
        return None
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError:
        raise ValueError("F10 event contains an unrecognized date; not converted to zero events") from None


def parse_f10_page(raw: bytes, *, instrument: str, source_url: str, fetched_at: str, expected_sha256: str):
    """Parse every identified table once, preserving unknown and placeholder states."""
    digest = hashlib.sha256(raw).hexdigest()
    if digest != expected_sha256:
        raise ValueError("F10 source bytes differ from the discovery manifest checksum")
    if not re.fullmatch(r"(?:SH|SZ)\d{6}", instrument) or source_url != f"https://fundf10.eastmoney.com/fhsp_{instrument[2:]}.html":
        raise ValueError("F10 source does not match the declared instrument")
    text = raw.decode("utf-8")
    if instrument[2:] not in text:
        raise ValueError("F10 page has no matching instrument identity")
    tables = pd.read_html(StringIO(text))
    definitions = {"cash_distribution": {"权益登记日", "除息日", "分红发放日"},
                   "split_conversion": {"拆分折算日", "拆分类型", "拆分折算比例"}}
    events, table_audit = [], {}
    for kind, required in definitions.items():
        matches = [frame for frame in tables if required.issubset(set(map(str, frame.columns)))]
        if len(matches) != 1:
            raise ValueError("Missing or ambiguous F10 action table; completeness remains unknown")
        frame = matches[0]
        placeholders = 0
        for ordinal, row in enumerate(frame.to_dict(orient="records")):
            if all("暂无" in str(value) for value in row.values()):
                placeholders += 1
                continue
            event = {"instrument": instrument, "action_type": kind, "status": "candidate",
                "ex_date": None, "effective_date": None, "record_date": None, "payment_date": None,
                "cash_per_original_unit": None, "split_ratio": None, "announcement_date": None,
                "announcement_url": None, "source_url": source_url, "source_sha256": digest,
                "source_fetched_at": fetched_at, "source_row_index": ordinal, "raw_fields": row,
                "issuer_verified": False, "ready_for_economic_ledger": False}
            if kind == "cash_distribution":
                amount_columns = [column for column in row if re.fullmatch(r"每(?:\d+)?份分红", str(column))]
                if len(amount_columns) != 1:
                    raise ValueError("Unknown F10 cash unit; amount is not inferred")
                match = re.fullmatch(r"每(\d+)?份派现金([0-9]+(?:\.[0-9]+)?)元", str(row[amount_columns[0]]).strip())
                if not match:
                    raise ValueError("Unknown F10 cash amount syntax")
                denominator = Decimal(match.group(1) or "1")
                header_units = re.fullmatch(r"每(\d+)?份分红", str(amount_columns[0]))
                if denominator <= 0 or denominator != Decimal(header_units.group(1) or "1"):
                    raise ValueError("Invalid F10 cash unit denominator")
                event.update(record_date=_day(row["权益登记日"]), ex_date=_day(row["除息日"]),
                    payment_date=_day(row["分红发放日"]),
                    cash_per_original_unit=str(Decimal(match.group(2)) / denominator),
                    cash_unit_basis="one fund unit entitled at record_date, before this event; not an inception/2013-original cohort unit")
                ordered = [event[key] for key in ("record_date", "ex_date", "payment_date") if event[key]]
                if ordered != sorted(ordered):
                    raise ValueError("F10 record/ex/payment dates have inconsistent ordering")
            else:
                match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?):([0-9]+(?:\.[0-9]+)?)", str(row["拆分折算比例"]).strip())
                if not match or min(Decimal(match.group(1)), Decimal(match.group(2))) <= 0:
                    raise ValueError("Unknown F10 split-ratio syntax")
                event.update(effective_date=_day(row["拆分折算日"]),
                    split_ratio=str(Decimal(match.group(2)) / Decimal(match.group(1))),
                    split_ratio_basis="vendor display ratio, candidate new units per prior unit; exact issuer ratio and effective/ex date unverified",
                    split_ratio_exact=False)
            event["unknown_fields"] = [key for key in ("ex_date", "record_date", "payment_date", "announcement_date", "announcement_url") if event[key] is None]
            identity = {key: event[key] for key in ("instrument", "action_type", "ex_date", "effective_date", "record_date", "cash_per_original_unit", "split_ratio")}
            event["event_id"] = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()[:24]
            events.append(event)
        table_audit[kind] = {"raw_rows": len(frame), "placeholder_rows": placeholders,
            "candidate_events": sum(row["action_type"] == kind for row in events),
            "page_statement": "vendor_reports_no_records_completeness_unverified" if placeholders else "vendor_records_parsed",
            "issuer_verified_complete": False}
    if len({event["event_id"] for event in events}) != len(events):
        raise ValueError("Duplicate F10 event identities require manual review")
    return {"instrument": instrument, "source_url": source_url, "source_sha256": digest,
            "source_fetched_at": fetched_at, "source_bytes_verified": True,
            "status": "candidate_only", "issuer_verified_complete": False,
            "history_coverage_status": "unknown_not_proven_by_vendor_page", "table_audit": table_audit,
            "events": sorted(events, key=lambda row: (row["ex_date"] or row["effective_date"] or "", row["action_type"]))}


def audit_f10_directory(root, *, start_date="2013-01-01", end_date="2026-10-19"):
    """Audit only already-downloaded files; missing bytes/HTTP failures are errors."""
    start, end = date.fromisoformat(start_date), date.fromisoformat(end_date)
    root = Path(root)
    manifest = json.loads((root / "discovery_manifest.json").read_text(encoding="utf-8"))
    items = []
    for source in manifest["instruments"]:
        if source.get("http_status") != 200:
            raise ValueError("A missing/refused source cannot be treated as no corporate actions")
        item = parse_f10_page((root / (source["instrument"] + ".html")).read_bytes(),
            instrument=source["instrument"], source_url=source["url"],
            fetched_at=manifest["fetched_at"], expected_sha256=source["source_sha256"])
        counts = {"cash_distribution": 0, "split_conversion": 0}
        for event in item["events"]:
            day = event["ex_date"] or event["effective_date"]
            event["within_requested_window"] = start.isoformat() <= day <= end.isoformat() if day else None
            if event["within_requested_window"]:
                counts[event["action_type"]] += 1
        item["candidate_counts_in_window"] = counts
        item["evidence_file"] = str(root / (source["instrument"] + ".html"))
        items.append(item)
    return {"schema_version": 1, "status": "candidate_only_not_an_economic_ledger",
        "requested_window": {"start_date": start.isoformat(), "end_date": end.isoformat()},
        "source_fetched_at": manifest["fetched_at"], "discovery_manifest": str(root / "discovery_manifest.json"),
        "discovery_manifest_sha256": hashlib.sha256((root / "discovery_manifest.json").read_bytes()).hexdigest(),
        "adapter_reference": manifest.get("adapter_reference"), "issuer_verified_complete": False,
        "used_to_change_prices_or_returns": False, "ready_for_economic_ledger": False,
        "limitations": [
            "All rows are vendor F10 candidates; parsed source bytes and fields are verified, issuer announcements and historical completeness are not.",
            "No-record placeholders are not zero-event proofs. Unlisted years and absent records remain coverage unknown.",
            "Split ratios are displayed with limited precision; verify exact issuer ratio, direction, effective date and share/price basis before use.",
            "Cash amounts are per record-date unit, not an inception/2013 unit after cumulative conversions; no automatic cohort conversion is performed.",
            "Vendor conversion dates are not silently equated to exchange ex-dates; missing record/payment/announcement fields remain null.",
            "Fetched-at is observation time, never a historical announcement/known-at time; these candidates cannot supply PIT features.",
            "No raw cash/share ledger, reinvested total return, correction to existing research, or return-based source choice was performed.",
        ], "instruments": items}
