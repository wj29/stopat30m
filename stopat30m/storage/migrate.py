"""Database initialization and data migration from legacy file formats.

Run via: python -m stopat30m.storage.migrate
"""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path

from loguru import logger

from .database import get_db, init_db
from .models import AnalysisHistory, BacktestRun, SignalHistory, TradeRecord, User


def migrate_all() -> None:
    """Run full migration: create tables, then import legacy CSV/JSON data."""
    init_db()
    logger.info("Database tables created/verified")

    _migrate_trade_ledger()
    _migrate_signal_csvs()
    _migrate_backtest_runs()
    _assign_orphan_data_to_admin()


def _migrate_trade_ledger() -> None:
    """Import trades from legacy CSV ledger if it exists."""
    candidates = [
        Path("./output/paper/trades.csv"),
        Path("./data/trades.csv"),
    ]
    for csv_path in candidates:
        if not csv_path.exists():
            continue
        with get_db() as db:
            existing = db.query(TradeRecord).count()
            if existing > 0:
                logger.info(f"Trade records already migrated ({existing} rows), skipping")
                return

            imported = 0
            with open(csv_path) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        record = TradeRecord(
                            trade_date=datetime.fromisoformat(row.get("date", row.get("trade_date", ""))),
                            instrument=row.get("instrument", ""),
                            direction=row.get("direction", ""),
                            quantity=int(float(row.get("quantity", 0))),
                            price=float(row.get("price", 0)),
                            amount=float(row.get("amount", 0)),
                            commission=float(row.get("commission", 0)),
                            stamp_tax=float(row.get("stamp_tax", 0)),
                            transfer_fee=float(row.get("transfer_fee", 0)),
                            total_cost=float(row.get("total_cost", 0)),
                            note=row.get("note", ""),
                            source="csv_import",
                        )
                        db.add(record)
                        imported += 1
                    except Exception as e:
                        logger.warning(f"Skipping trade row: {e}")

            logger.info(f"Imported {imported} trade records from {csv_path}")
            return


def _migrate_signal_csvs() -> None:
    """Import historical signal CSVs."""
    sig_dir = Path("./output/signals")
    if not sig_dir.exists():
        return

    with get_db() as db:
        existing = db.query(SignalHistory).count()
        if existing > 0:
            logger.info(f"Signal history already migrated ({existing} rows), skipping")
            return

        imported = 0
        for csv_path in sorted(sig_dir.glob("signal_*.csv")):
            batch_id = csv_path.stem
            try:
                with open(csv_path) as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        record = SignalHistory(
                            signal_date=row.get("date", ""),
                            instrument=row.get("instrument", ""),
                            score=float(row.get("score", 0)),
                            signal=row.get("signal", ""),
                            weight=float(row.get("weight", 0)),
                            method=row.get("method", "top_k"),
                            batch_id=batch_id,
                        )
                        db.add(record)
                        imported += 1
            except Exception as e:
                logger.warning(f"Failed to import {csv_path}: {e}")

        logger.info(f"Imported {imported} signal records")


def _migrate_backtest_runs() -> None:
    """Import backtest run summaries from output directories."""
    bt_root = Path("./output/backtests")
    if not bt_root.exists():
        return

    with get_db() as db:
        existing = db.query(BacktestRun).count()
        if existing > 0:
            logger.info(f"Backtest runs already migrated ({existing} rows), skipping")
            return

        imported = 0
        for kind in ["signal", "account"]:
            kind_dir = bt_root / kind
            if not kind_dir.exists():
                continue
            for run_dir in sorted(kind_dir.iterdir()):
                if not run_dir.is_dir():
                    continue
                report_path = run_dir / "report.json"
                config_path = run_dir / "config.json"

                report = {}
                config = {}
                if report_path.exists():
                    try:
                        report = json.loads(report_path.read_text())
                    except Exception:
                        pass
                if config_path.exists():
                    try:
                        config = json.loads(config_path.read_text())
                    except Exception:
                        pass

                tag = run_dir.name.split("_", 2)[-1] if "_" in run_dir.name else ""
                record = BacktestRun(
                    kind=kind,
                    tag=tag,
                    run_dir=str(run_dir),
                    annual_return=report.get("annual_return"),
                    sharpe=report.get("sharpe"),
                    max_drawdown=report.get("max_drawdown"),
                    total_trades=report.get("total_trades"),
                    win_rate=report.get("win_rate"),
                    config=config,
                    report=report,
                )
                db.add(record)
                imported += 1

        logger.info(f"Imported {imported} backtest run summaries")


def _assign_orphan_data_to_admin() -> None:
    """Assign rows with NULL user_id to the first admin user (one-time migration)."""
    with get_db() as db:
        admin = db.query(User).filter(User.role == "admin").order_by(User.id).first()
        if admin is None:
            return

        for model, col in [
            (TradeRecord, TradeRecord.user_id),
            (AnalysisHistory, AnalysisHistory.user_id),
            (BacktestRun, BacktestRun.user_id),
        ]:
            count = db.query(model).filter(col == None).count()  # noqa: E711
            if count > 0:
                db.query(model).filter(col == None).update({col: admin.id})  # noqa: E711
                logger.info(f"Assigned {count} orphan {model.__tablename__} rows to admin (id={admin.id})")


if __name__ == "__main__":
    migrate_all()
