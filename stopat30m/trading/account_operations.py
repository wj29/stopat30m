"""Observed account valuations and read-only standard-statement comparison.

The caller commits writes. Saving starts with a clean/read-only Session: its
initial read transaction is released before market IO, then the ledger hash is
checked under the same SQLite write reservation as all personal cash/trades.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

from sqlalchemy.orm import Session

from stopat30m.storage.models import (
    PortfolioCashFlow, PortfolioExecution, PortfolioNav, PortfolioNavReceipt,
)
from . import portfolio

LIMITATIONS = [
    "仅保存用户主动操作时的当日估值；不是历史收盘净值，缺失日期不补造。",
    "同一上海日期保留最近一次保存的估值，历史请求重试仍返回原始回执。",
    "累计损益为总资产减累计净投入，入出金不是利润；未计算TWR、年化或收益率。",
    "账本尚不自动处理分红、送转、配股等公司行为；对账相等不证明价格或成本正确。",
]
MAX_CSV_BYTES, MAX_STATEMENT_ROWS = 100_000, 2000
_NAV_FIELDS = ("cash", "market_value", "equity", "net_deposits", "realized_pnl",
               "unrealized_pnl", "total_fees", "total_pnl")


def now():
    return datetime.now(portfolio.SHANGHAI)


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False, default=str)


def _begin_read(db):
    if db.new or db.dirty or db.deleted:
        raise portfolio.PortfolioError("账户操作必须在已提交账本后单独执行")
    connection = db.connection()
    # sqlite3 legacy transaction control does not BEGIN on SELECT. Explicitly
    # BEGIN a read transaction so account/flows/fills refer to one snapshot.
    if connection.dialect.name == "sqlite" and not connection.connection.driver_connection.in_transaction:
        connection.exec_driver_sql("BEGIN")


def _ledger(db, account):
    executions = db.query(PortfolioExecution).filter_by(account_id=account.id).order_by(PortfolioExecution.id).all()
    flows = db.query(PortfolioCashFlow).filter_by(account_id=account.id).order_by(PortfolioCashFlow.id).all()
    facts = {
        "account_id": account.id, "user_id": account.user_id,
        "opened_date": account.opened_date.isoformat(), "last_event_date": account.last_event_date.isoformat(),
        "cash": str(account.cash), "net_deposits": str(account.net_deposits),
        "cash_flows": [{key: getattr(row, key) for key in
            ("id", "direction", "amount", "effective_date", "cash_after")} for row in flows],
        "executions": [{key: getattr(row, key) for key in
            ("id", "trade_date", "instrument", "direction", "quantity", "price", "amount",
             "commission", "stamp_tax", "transfer_fee", "net_amount", "realized_pnl", "cash_after")}
            for row in executions],
    }
    version = hashlib.sha256(_json(facts).encode()).hexdigest()
    # Financial facts remain in their immutable ledger tables. Store a digest
    # plus their exact append boundaries, rather than duplicating the entire
    # growing trade history into every daily observation/API response.
    return version, {"version": version, "execution_count": len(executions), "cash_flow_count": len(flows),
        "last_event_date": account.last_event_date.isoformat(), "facts": {
            "account_id": account.id, "last_execution_id": executions[-1].id if executions else None,
            "last_cash_flow_id": flows[-1].id if flows else None, "cash": str(account.cash),
            "net_deposits": str(account.net_deposits)}}


def _receipt(db, account_id, key):
    row = db.query(PortfolioNavReceipt).filter_by(account_id=account_id, idempotency_key=key).first()
    return json.loads(row.receipt) if row else None


def _timestamp(value, label, instant):
    try:
        parsed = datetime.fromisoformat(value) if isinstance(value, str) else None
        if parsed is None or parsed.tzinfo is None:
            raise ValueError("timezone required")
        age = (instant - parsed).total_seconds()
        if not 0 <= age <= 120 or parsed.astimezone(portfolio.SHANGHAI).date() != instant.date():
            raise ValueError("not current Shanghai date/fresh")
        return parsed
    except (TypeError, ValueError, OverflowError):
        raise portfolio.PortfolioError(f"{label}缺失、过期或无有效时区，不能保存完整估值") from None


def _quotes(instruments, received, instant):
    validated = {}
    for inst in instruments:
        quote = received.get(inst) if isinstance(received, dict) else None
        if not isinstance(quote, dict):
            raise portfolio.PortfolioError(f"{inst} 缺少报价，未保存估值快照")
        try:
            price = Decimal(str(quote.get("price")))
            if not price.is_finite() or not 0 < price <= Decimal("1000000"):
                raise ValueError("price")
        except (InvalidOperation, ValueError, TypeError):
            raise portfolio.PortfolioError(f"{inst} 报价无效，未保存估值快照") from None
        if quote.get("status") not in {"available", "cached"} or quote.get("is_realtime") is not True:
            raise portfolio.PortfolioError(f"{inst} 没有可信的新鲜报价，未保存估值快照")
        if not isinstance(quote.get("source"), str) or not quote["source"].strip() or len(quote["source"]) > 120:
            raise portfolio.PortfolioError(f"{inst} 报价来源未知，未保存估值快照")
        source_time = _timestamp(quote.get("as_of"), f"{inst} 行情时间", instant)
        fetched_time = _timestamp(quote.get("fetched_at"), f"{inst} 获取时间", instant)
        if source_time > fetched_time:
            raise portfolio.PortfolioError(f"{inst} 行情时间晚于获取时间，未保存估值快照")
        validated[inst] = {"price": float(price), "source": quote["source"],
            "as_of": quote["as_of"], "fetched_at": quote["fetched_at"],
            "status": quote["status"], "is_realtime": True,
            "name": str(quote.get("name") or "")[:100]}
    return validated


def nav_dict(row):
    return {"id": row.id, "date": row.snapshot_date.isoformat(), "recorded_at": row.recorded_at,
            "revision": row.revision, "ledger_version": row.ledger_version,
            **{key: float(getattr(row, key)) for key in _NAV_FIELDS},
            "valuation_complete": True, "kind": "saved_observation", "facts": json.loads(row.facts)}


def save_nav(db: Session, user_id: int, idempotency_key: str, *, quote_loader=None):
    key, day = portfolio._key(idempotency_key), now().date()
    _begin_read(db)
    account = portfolio.get_account(db, user_id)
    replay = _receipt(db, account.id, key)
    if replay is not None:
        return replay, True
    expected_version, _ = _ledger(db, account)
    instruments = sorted(inst for inst, pos in portfolio.holdings(db, account.id).items() if pos.quantity > 0)
    db.rollback()  # Never keep the ledger read transaction/lock during IO.
    received = {}
    if instruments:
        if quote_loader is None:
            from stopat30m.data.realtime import fetch_spot_quotes
            quote_loader = lambda symbols: fetch_spot_quotes(symbols, ttl=0)
        try:
            received = quote_loader(instruments)
        except Exception as exc:
            raise portfolio.PortfolioError(f"获取当日报价失败，未保存估值：{type(exc).__name__}") from exc
    portfolio._write_lock(db, user_id)
    account = portfolio.get_account(db, user_id)
    replay = _receipt(db, account.id, key)
    if replay is not None:
        return replay, True
    version, ledger = _ledger(db, account)
    if version != expected_version:
        raise portfolio.PortfolioError("取价期间账户账本已变化，请核对最新成交/资金后重新保存")
    instant = now()
    if instant.date() != day or instant.date() != portfolio.today():
        raise portfolio.PortfolioError("保存过程跨越上海日期，请重新获取当日报价")
    quotes = _quotes(instruments, received, instant)
    summary, positions = portfolio.valuation(db, account, quotes)
    if summary["valuation_complete"] is not True or summary["equity"] is None:
        raise portfolio.PortfolioError("持仓估值不完整，未保存快照")
    row = db.query(PortfolioNav).filter_by(account_id=account.id, snapshot_date=day).first()
    if row is None:
        row = PortfolioNav(account_id=account.id, snapshot_date=day, revision=1)
        db.add(row)
    else:
        row.revision += 1
    row.recorded_at, row.ledger_version = instant.isoformat(), version
    for field in _NAV_FIELDS:
        # All summary values derive from portfolio's Decimal arithmetic.
        setattr(row, field, Decimal(str(summary[field])).quantize(portfolio.CENT))
    row.facts = _json({"ledger": ledger, "quotes": quotes, "positions": positions,
        "valuation": summary, "limitations": LIMITATIONS})
    db.flush()
    snapshot = nav_dict(row)
    db.add(PortfolioNavReceipt(account_id=account.id, nav_id=row.id, idempotency_key=key, receipt=_json(snapshot)))
    db.flush()
    return snapshot, False


def list_nav(db: Session, user_id: int, limit=90, offset=0):
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 366 or not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
        raise portfolio.PortfolioError("估值分页参数无效", 400)
    account = portfolio.get_account(db, user_id, required=False)
    query = db.query(PortfolioNav).filter_by(account_id=account.id) if account else None
    return {"items": [nav_dict(row) for row in query.order_by(PortfolioNav.snapshot_date.desc()).offset(offset).limit(limit)] if query is not None else [],
            "total": query.count() if query is not None else 0, "limit": limit, "offset": offset, "limitations": LIMITATIONS}


def _statement(csv_text):
    try:
        valid_size = isinstance(csv_text, str) and bool(csv_text) and len(csv_text.encode("utf-8")) <= MAX_CSV_BYTES
    except UnicodeError:
        valid_size = False
    if not valid_size or "\x00" in csv_text:
        raise portfolio.PortfolioError("CSV须为不超过100KB的非空UTF-8文本", 400)
    try:
        reader = csv.reader(io.StringIO(csv_text.removeprefix("\ufeff")), strict=True)
        header = next(reader, [])
        if [field.strip().lower() for field in header] != ["instrument", "quantity"]:
            raise portfolio.PortfolioError("CSV表头必须为 instrument,quantity，且只包含这两列", 400)
        positions = {}
        count = 0
        for row in reader:
            if not row:  # Empty physical lines do not imply an empty position.
                continue
            count += 1
            if count > MAX_STATEMENT_ROWS or len(row) != 2:
                raise portfolio.PortfolioError("CSV最多2000行持仓，每行必须正好两列", 400)
            instrument = portfolio._instrument(row[0])
            if instrument in positions:
                raise portfolio.PortfolioError(f"CSV证券重复（归一后）：{instrument}", 400)
            raw_quantity = row[1].strip()
            if not re.fullmatch(r"[0-9]+", raw_quantity) or len(raw_quantity) > 10 or int(raw_quantity) > 1_000_000_000:
                raise portfolio.PortfolioError(f"{instrument} 数量须为0至10亿的非负整数", 400)
            positions[instrument] = int(raw_quantity)
        return positions
    except csv.Error as exc:
        raise portfolio.PortfolioError(f"CSV格式错误：{exc}", 400) from exc


def reconcile(db: Session, user_id: int, csv_text: str, broker_cash, statement_date=None):
    statement_day = portfolio.execution_date(statement_date)
    if statement_day != portfolio.today() or now().date() != statement_day:
        raise portfolio.PortfolioError("仅支持核对当前上海日期的持仓与现金", 400)
    statement_cash, statement = portfolio.money(broker_cash, "券商现金"), _statement(csv_text)
    if Decimal(str(broker_cash)) != statement_cash:
        raise portfolio.PortfolioError("券商现金须精确到分，不能通过舍入隐藏对账差异", 400)
    _begin_read(db)
    account = portfolio.get_account(db, user_id)
    version, _ = _ledger(db, account)
    local = {inst: pos.quantity for inst, pos in portfolio.holdings(db, account.id).items() if pos.quantity}
    positions = [{"instrument": inst, "ledger_quantity": local.get(inst, 0), "statement_quantity": statement.get(inst, 0),
                  "difference": statement.get(inst, 0) - local.get(inst, 0)} for inst in sorted(local.keys() | statement.keys())]
    difference = statement_cash - account.cash
    return {"as_of": now().isoformat(), "statement_date": statement_day.isoformat(), "ledger_version": version,
        "matched": difference == 0 and all(row["difference"] == 0 for row in positions),
        "cash": {"ledger": float(account.cash), "statement": float(statement_cash), "difference": float(difference)},
        "positions": positions, "warnings": ["只读核对现金和持仓数量，不导入或修改账本。",
            "差异为券商数减本地数；核对相等不证明价格、成本、费用或公司行为正确。",
            "请确认券商导出与本地录入涵盖同一时点；本接口不解析券商专属格式。"]}
