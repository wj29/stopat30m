"""Offline F10 extraction; no source requests and no economic return claims."""
import hashlib

import pytest

from stopat30m.data.corporate_action_candidates import parse_f10_page


def page(cash=None, split=None):
    cash = cash or '<tr><td>2024年</td><td>2024-01-17</td><td>2024-01-18</td><td>每10份派现金0.6900元</td><td>2024-01-23</td></tr>'
    split = split or '<tr><td>2012年</td><td>2012-05-11</td><td>份额折算</td><td>1:0.3709</td></tr>'
    return (f'<title>510300</title><table><tr><th>年份</th><th>权益登记日</th><th>除息日</th><th>每10份分红</th><th>分红发放日</th></tr>{cash}</table>'
            f'<table><tr><th>年份</th><th>拆分折算日</th><th>拆分类型</th><th>拆分折算比例</th></tr>{split}</table>').encode()


def parse(raw):
    return parse_f10_page(raw, instrument="SH510300", source_url="https://fundf10.eastmoney.com/fhsp_510300.html",
                          fetched_at="2026-10-19T08:46:43+00:00", expected_sha256=hashlib.sha256(raw).hexdigest())


def test_vendor_cash_units_are_exact_and_not_claimed_as_issuer_complete():
    result = parse(page())
    cash = next(row for row in result["events"] if row["action_type"] == "cash_distribution")
    assert cash["cash_per_original_unit"] == "0.0690"
    assert cash["record_date"] == "2024-01-17" and cash["payment_date"] == "2024-01-23"
    assert cash["announcement_date"] is None and cash["announcement_url"] is None
    assert cash["issuer_verified"] is False and cash["ready_for_economic_ledger"] is False
    assert result["issuer_verified_complete"] is False and result["source_bytes_verified"] is True


def test_no_records_placeholder_is_not_an_event_or_proof_of_zero_dividends():
    result = parse(page(cash='<tr><td colspan="5">暂无分红信息!</td></tr>'))
    audit = result["table_audit"]["cash_distribution"]
    assert audit["raw_rows"] == audit["placeholder_rows"] == 1
    assert audit["candidate_events"] == 0 and audit["issuer_verified_complete"] is False
    assert audit["page_statement"] == "vendor_reports_no_records_completeness_unverified"
    assert result["history_coverage_status"] == "unknown_not_proven_by_vendor_page"


def test_split_display_precision_and_conversion_date_are_not_upgraded_to_verified_fields():
    split = next(row for row in parse(page())["events"] if row["action_type"] == "split_conversion")
    assert split["split_ratio"] == "0.3709" and split["split_ratio_exact"] is False
    assert split["effective_date"] == "2012-05-11"
    assert split["ex_date"] is None and split["record_date"] is None and split["payment_date"] is None
    assert split["cash_per_original_unit"] is None


def test_missing_payment_remains_null_without_losing_cash_candidate():
    result = parse(page().replace(b"2024-01-23", b"--"))
    cash = next(row for row in result["events"] if row["action_type"] == "cash_distribution")
    assert cash["payment_date"] is None and "payment_date" in cash["unknown_fields"]
    assert cash["source_fetched_at"] != cash["announcement_date"]


def test_wrong_source_checksum_cannot_be_parsed_as_an_empty_history():
    with pytest.raises(ValueError, match="checksum"):
        parse_f10_page(page(), instrument="SH510300", source_url="https://fundf10.eastmoney.com/fhsp_510300.html",
                       fetched_at="2026-10-19", expected_sha256="0" * 64)


def test_missing_tables_conflicting_cash_units_and_duplicate_events_fail_closed():
    for raw in [b"<title>510300</title><p>No data</p>", page().replace("每10份派现金".encode(), "每100份派现金".encode()),
                page(cash='<tr><td>2024年</td><td>2024-01-17</td><td>2024-01-18</td><td>每10份派现金0.6900元</td><td>2024-01-23</td></tr>' * 2)]:
        with pytest.raises(ValueError):
            parse(raw)
