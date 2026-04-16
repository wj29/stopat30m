"""SQLAlchemy ORM models for stopat30m structured data."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Auth models
# ---------------------------------------------------------------------------


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(50), unique=True, nullable=False, index=True)
    password_hash = Column(String(256), nullable=False)
    role = Column(String(10), nullable=False, default="user")
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_login = Column(DateTime, nullable=True)


class InviteCode(Base):
    __tablename__ = "invite_codes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(32), unique=True, nullable=False, index=True)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    used_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    expires_at = Column(DateTime, nullable=False)
    used_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


# ---------------------------------------------------------------------------
# Business models
# ---------------------------------------------------------------------------


class WatchlistItem(Base):
    """User's watchlist (自选股)."""

    __tablename__ = "watchlist"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    code = Column(String(10), nullable=False)
    name = Column(String(50), default="")
    note = Column(String(200), default="")
    sort_order = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_watchlist_user_code", "user_id", "code", unique=True),
    )


class WatchlistSchedule(Base):
    """Per-user watchlist auto-analysis schedule."""

    __tablename__ = "watchlist_schedule"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, unique=True)
    enabled = Column(Boolean, default=False)
    run_time = Column(String(5), default="18:00")
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ChatSession(Base):
    """Agent chat conversation session."""

    __tablename__ = "chat_sessions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    title = Column(String(200), default="新对话")
    stock_code = Column(String(10), nullable=True)
    stock_name = Column(String(50), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    is_deleted = Column(Boolean, default=False)


class ChatMessage(Base):
    """Individual message within a chat session."""

    __tablename__ = "chat_messages"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(Integer, ForeignKey("chat_sessions.id", ondelete="CASCADE"), nullable=False, index=True)
    role = Column(String(20), nullable=False)
    content = Column(Text, nullable=False, default="")
    tool_calls = Column(JSON, nullable=True)
    tokens_used = Column(Integer, default=0)
    model_used = Column(String(100), default="")
    created_at = Column(DateTime, default=datetime.utcnow)


class AnalysisHistory(Base):
    """Per-stock analysis record: technical scoring + LLM report."""

    __tablename__ = "analysis_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True, index=True)
    name = Column(String(50), default="")
    status = Column(String(10), nullable=False, default="completed", server_default="completed", index=True)
    analysis_date = Column(DateTime, default=datetime.utcnow, index=True)

    # Technical analysis (from TrendAnalyzer)
    signal_score = Column(Float, default=0.0)
    buy_signal = Column(String(20), default="")
    signal_reasons = Column(JSON, default=list)
    risk_factors = Column(JSON, default=list)
    trend_status = Column(String(30), default="")
    technical_detail = Column(JSON, default=dict)

    # Model prediction (from Qlib model)
    model_score = Column(Float, nullable=True)
    model_percentile = Column(Float, nullable=True)

    # LLM analysis
    llm_sentiment = Column(Float, nullable=True)
    llm_operation_advice = Column(String(20), default="")
    llm_confidence = Column(Float, nullable=True)
    llm_summary = Column(Text, default="")
    llm_dashboard = Column(JSON, default=dict)
    llm_raw_response = Column(Text, default="")
    llm_model_used = Column(String(100), default="")

    # Metadata
    data_source = Column(String(30), default="")
    processing_time_ms = Column(Integer, default=0)

    __table_args__ = (
        Index("ix_analysis_code_date", "code", "analysis_date"),
    )


class StockDaily(Base):
    """Daily OHLCV cache for quick queries (supplements Qlib binary store)."""

    __tablename__ = "stock_daily"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False)
    date = Column(String(10), nullable=False)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    amount = Column(Float, nullable=True)
    pct_chg = Column(Float, nullable=True)
    data_source = Column(String(30), default="")

    __table_args__ = (
        Index("ix_daily_code_date", "code", "date", unique=True),
    )


class TradeRecord(Base):
    """Trade execution record (replaces CSV ledger)."""

    __tablename__ = "trade_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_date = Column(DateTime, default=datetime.utcnow, index=True)
    instrument = Column(String(10), nullable=False, index=True)
    direction = Column(String(10), nullable=False)
    quantity = Column(Integer, nullable=False)
    price = Column(Float, nullable=False)
    amount = Column(Float, nullable=False)
    commission = Column(Float, default=0.0)
    stamp_tax = Column(Float, default=0.0)
    transfer_fee = Column(Float, default=0.0)
    total_cost = Column(Float, default=0.0)
    note = Column(String(200), default="")
    source = Column(String(20), default="manual")
    user_id = Column(Integer, nullable=True, index=True)


class BacktestRun(Base):
    """Backtest run summary (replaces JSON report files)."""

    __tablename__ = "backtest_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    kind = Column(String(20), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True, index=True)
    tag = Column(String(100), default="")
    run_dir = Column(String(500), default="")
    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    # Key metrics
    annual_return = Column(Float, nullable=True)
    sharpe = Column(Float, nullable=True)
    max_drawdown = Column(Float, nullable=True)
    total_trades = Column(Integer, nullable=True)
    win_rate = Column(Float, nullable=True)

    # Config snapshot
    config = Column(JSON, default=dict)
    report = Column(JSON, default=dict)


class SignalHistory(Base):
    """Historical signal record."""

    __tablename__ = "signal_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    signal_date = Column(String(10), nullable=False, index=True)
    instrument = Column(String(10), nullable=False, index=True)
    score = Column(Float, nullable=False)
    signal = Column(String(10), nullable=False)
    weight = Column(Float, default=0.0)
    method = Column(String(20), default="top_k")
    batch_id = Column(String(50), default="")

    __table_args__ = (
        Index("ix_signal_date_inst", "signal_date", "instrument"),
    )
