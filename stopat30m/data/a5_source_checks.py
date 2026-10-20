"""Run unchanged source checks on indexed per-security views.

These views avoid repeatedly scanning an entire market table. The reviewed A4
checks still evaluate every selected row/session with their original rules.
"""
from __future__ import annotations

from . import ashare_history_audit as original


def _groups(raw):
    return {name: frame for name, frame in raw.groupby("instrument", sort=False)}


def audit_daily_coverage(raw, basic, calendar, instruments, *, start, end):
    if raw.duplicated(["instrument", "date"]).any():
        raise ValueError("Duplicate original daily execution keys")
    groups, results = _groups(raw), []
    # Retain the original global calendar validation, including empty targets.
    template = original.audit_daily_coverage(raw.iloc[:0], basic, calendar, [], start=start, end=end)
    for inst in instruments:
        results.append(original.audit_daily_coverage(groups.get(inst, raw.iloc[:0]), basic,
            calendar, [inst], start=start, end=end))
    for field in ("missing_listed_security_sessions", "invalid_trading_sessions", "source_confirmed_suspensions"):
        template[field] = [row for result in results for row in result[field]]
    template["instruments"] = len(instruments)
    template["status"] = "passed" if all(result["status"] == "passed" for result in results) else "blocked"
    template["historical_st_row_count"] = int(raw.loc[raw.date.between(start, end), "isST"].eq("1").sum())
    return template


def audit_qlib_availability(dataset, raw, instruments, *, start, end):
    groups, mismatches, classified = _groups(raw), [], []
    for inst in instruments:
        result = original.audit_qlib_availability(dataset, groups.get(inst, raw.iloc[:0]), [inst], start=start, end=end)
        mismatches.extend(result["mismatches"])
        classified.extend(result["classified_missing_model_sessions"])
    return {"status": "passed" if not mismatches else "blocked", "mismatches": mismatches,
            "classified_missing_model_sessions": classified}


def audit_reference_prices(raw, events, calendar, *, start, end, rights=()):
    groups, records, failures = _groups(raw), [], []
    if any(row["instrument"] not in groups for row in rights):
        raise ValueError("Every rights issue requires an actual observed audited ex-date reference")
    for inst in sorted(groups):
        frame = groups[inst]
        # The original loop would reject these calendar entries in its filter.
        # Keeping precisely its iterated sessions preserves all halt/ex dates.
        first = str(frame.date.min())
        sessions = [day for day in calendar if first <= day <= end]
        result = original.audit_reference_prices(frame,
            [row for row in events if row["instrument"] == inst], sessions,
            start=start, end=end, rights=[row for row in rights if row["instrument"] == inst])
        records.extend(result["matched_adjustments"])
        failures.extend(result["unexplained"])
    return {"status": "passed" if not failures else "blocked_unexplained_ex_reference",
        "checked_stocks": int(raw.instrument.nunique()), "matched_adjustments": records,
        "unexplained": failures, "price_tick_tolerance_cny": "0.011"}
