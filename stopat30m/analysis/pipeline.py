"""Analysis pipeline: data fetch -> technical scoring -> enrichment -> LLM analysis -> store."""

from __future__ import annotations

import time
from datetime import datetime
from typing import Callable, Generator

from loguru import logger

from stopat30m.data.normalize import bare_code, normalize_instrument
from stopat30m.data.realtime import fetch_stock_names
from stopat30m.data.sources import fetch_daily_ohlcv
from stopat30m.storage.database import get_db
from stopat30m.storage.models import AnalysisHistory

from .llm_analyzer import LLMAnalyzer, build_user_prompt
from .schemas import DashboardResult, FullAnalysisResponse, LLMAnalysisResult, OHLCVBar
from .trend_analyzer import StockTrendAnalyzer

ANALYSIS_STEPS = [
    (1, "获取股票名称"),
    (2, "拉取行情数据"),
    (3, "技术面分析"),
    (4, "模型预测查询"),
    (5, "搜索新闻情报"),
    (6, "获取筹码分布"),
    (7, "获取基本面数据"),
    (8, "LLM 深度分析"),
    (9, "写入数据库"),
]
TOTAL_STEPS = len(ANALYSIS_STEPS)


def _get_model_prediction(code: str) -> dict | None:
    """Look up the latest model prediction for a stock.

    Returns a dict with rich context (score, percentile, rank, pool info,
    model metadata) or None if no prediction is available.
    """
    try:
        from pathlib import Path

        from stopat30m.config import PROJECT_ROOT, get as cfg_get

        model_dir = PROJECT_ROOT / "output" / "models"
        if not model_dir.exists():
            return None

        model_files = sorted(model_dir.glob("*.pkl"), reverse=True)
        if not model_files:
            return None

        from stopat30m.backtest.common import load_prediction_bundle, PREDICTION_ROOT
        pred_files = sorted(PREDICTION_ROOT.glob("*.pkl"), reverse=True) if PREDICTION_ROOT.exists() else []

        if not pred_files:
            return None

        pred, _, metadata = load_prediction_bundle(pred_files[0])
        norm = normalize_instrument(code)

        latest_date = pred.index.get_level_values(0).max()
        day_pred = pred.xs(latest_date, level=0)

        score: float | None = None
        matched_idx = None

        if norm in day_pred.index:
            matched_idx = norm
        else:
            bare = bare_code(norm)
            for idx in day_pred.index:
                if bare_code(str(idx)) == bare:
                    matched_idx = idx
                    break

        if matched_idx is None:
            return None

        score = float(day_pred[matched_idx])
        percentile = float((day_pred <= score).mean())
        pool_size = len(day_pred)
        rank_in_pool = int((day_pred > score).sum()) + 1
        top_k = int(cfg_get("signal", "top_k", 10) or 10)
        sorted_pred = day_pred.sort_values(ascending=False)
        is_top_n = matched_idx in sorted_pred.head(top_k).index

        date_str = str(latest_date)[:10]
        model_type = str(cfg_get("model", "type", "lgbm") or "lgbm")
        model_file = model_files[0].name

        return {
            "score": score,
            "percentile": percentile,
            "rank_in_pool": rank_in_pool,
            "pool_size": pool_size,
            "top_n": is_top_n,
            "top_k": top_k,
            "prediction_date": date_str,
            "model_type": model_type,
            "model_file": model_file,
            "pred_file": pred_files[0].name,
        }

    except Exception as e:
        logger.debug(f"Model prediction lookup failed for {code}: {e}")

    return None


def _extract_ohlcv_bars(df, tail: int = 60) -> list[OHLCVBar]:
    """Extract the last *tail* OHLCV bars from a DataFrame for chart rendering."""
    import pandas as pd

    if df is None or df.empty:
        return []
    recent = df.tail(tail).copy()

    col_map = {}
    for target, candidates in {
        "date": ["date", "datetime", "trade_date"],
        "open": ["open", "Open"],
        "high": ["high", "High"],
        "low": ["low", "Low"],
        "close": ["close", "Close"],
        "volume": ["volume", "Volume", "vol"],
    }.items():
        for c in candidates:
            if c in recent.columns:
                col_map[target] = c
                break

    if recent.index.name in ("date", "datetime", "trade_date") and "date" not in col_map:
        recent = recent.reset_index()
        col_map["date"] = recent.columns[0]

    required = {"date", "open", "high", "low", "close"}
    if not required.issubset(col_map.keys()):
        return []

    bars: list[OHLCVBar] = []
    for _, row in recent.iterrows():
        d = row[col_map["date"]]
        if isinstance(d, pd.Timestamp):
            d = d.strftime("%Y-%m-%d")
        else:
            d = str(d)[:10]
        bars.append(OHLCVBar(
            date=d,
            open=float(row[col_map["open"]]),
            high=float(row[col_map["high"]]),
            low=float(row[col_map["low"]]),
            close=float(row[col_map["close"]]),
            volume=float(row.get(col_map.get("volume", "__none__"), 0)),
        ))
    return bars


ProgressCallback = Callable[[int, int, str], None]


def _run_agent_analysis(
    *,
    code: str,
    stock_name: str,
    tech_result,
    model_prediction,
    ohlcv_df,
    news_context,
    chip_data,
    fundamental_data,
    emit: ProgressCallback,
) -> "LLMAnalysisResult":
    """Step 8: run agent-based or legacy LLM analysis depending on config."""
    from stopat30m.config import get as cfg_get

    agent_cfg = cfg_get("agent") or {}
    arch = agent_cfg.get("arch", "single") if isinstance(agent_cfg, dict) else "single"

    if arch in ("single", "multi"):
        try:
            return _agent_based_analysis(
                code=code,
                stock_name=stock_name,
                tech_result=tech_result,
                model_prediction=model_prediction,
                ohlcv_df=ohlcv_df,
                news_context=news_context,
                chip_data=chip_data,
                fundamental_data=fundamental_data,
                emit=emit,
            )
        except Exception as e:
            logger.warning("Agent analysis failed, falling back to legacy LLM: %s", e)
            emit(8, TOTAL_STEPS, f"Agent 分析失败，回退至 LLM（{e}）")

    return _legacy_llm_analysis(
        tech_result=tech_result,
        model_prediction=model_prediction,
        ohlcv_df=ohlcv_df,
        stock_name=stock_name,
        news_context=news_context,
        chip_data=chip_data,
        fundamental_data=fundamental_data,
    )


def _agent_based_analysis(
    *,
    code, stock_name, tech_result, model_prediction, ohlcv_df,
    news_context, chip_data, fundamental_data, emit,
) -> "LLMAnalysisResult":
    from stopat30m.agent.factory import build_agent_executor

    executor = build_agent_executor()
    emit(8, TOTAL_STEPS, "Agent 正在分析...")

    task = f"分析股票 {code} ({stock_name})"
    context = {
        "stock_code": code,
        "stock_name": stock_name,
        "trend_result": tech_result.to_dict() if tech_result else None,
        "model_prediction": model_prediction,
        "news_context": news_context,
        "chip_data": chip_data,
        "fundamental_data": fundamental_data,
    }

    result = executor.run(task, context=context)

    if result.success and result.dashboard:
        emit(8, TOTAL_STEPS, f"Agent 分析完成（{result.total_steps} 步）")
        return _dashboard_to_llm_result(result.dashboard, result.model or "agent")

    if result.error:
        logger.warning("Agent returned error: %s", result.error)
    raise RuntimeError(result.error or "Agent produced no dashboard")


def _legacy_llm_analysis(*, tech_result, model_prediction, ohlcv_df,
                          stock_name, news_context, chip_data, fundamental_data) -> "LLMAnalysisResult":
    llm = LLMAnalyzer()
    return llm.analyze(
        tech_result,
        model_prediction=model_prediction,
        ohlcv_df=ohlcv_df,
        stock_name=stock_name,
        news_context=news_context,
        chip_data=chip_data,
        fundamental_data=fundamental_data,
    )


def _dashboard_to_llm_result(dashboard: dict, model: str) -> "LLMAnalysisResult":
    """Convert agent dashboard dict to LLMAnalysisResult for backward compat."""
    import re as _re

    d = dashboard.get("dashboard", {}) if isinstance(dashboard.get("dashboard"), dict) else {}
    intel = d.get("intelligence", {}) if isinstance(d, dict) else {}
    bp = d.get("battle_plan", {}) if isinstance(d, dict) else {}
    sp = bp.get("sniper_points", {}) if isinstance(bp, dict) else {}

    def _parse_price(s):
        if s is None:
            return None
        if isinstance(s, (int, float)):
            return float(s)
        m = _re.search(r"[\d.]+", str(s))
        return float(m.group()) if m else None

    score_raw = dashboard.get("sentiment_score", 50) or 50
    try:
        score = float(score_raw)
    except (TypeError, ValueError):
        score = 50.0
    sentiment_norm = (score / 100) * 2 - 1

    dash_obj = DashboardResult(
        stock_name=str(dashboard.get("stock_name", "")),
        sentiment_score=int(score),
        trend_prediction=str(dashboard.get("trend_prediction", "震荡")),
        operation_advice=str(dashboard.get("operation_advice", "观望")),
        decision_type=str(dashboard.get("decision_type", "hold")),
        confidence_level=str(dashboard.get("confidence_level", "中")),
        dashboard=d,
        analysis_summary=str(dashboard.get("analysis_summary", "")),
        key_points=str(dashboard.get("key_points", "")),
        risk_warning=str(dashboard.get("risk_warning", "")),
        buy_reason=str(dashboard.get("buy_reason", "")),
        trend_analysis=str(dashboard.get("trend_analysis", "")),
        short_term_outlook=str(dashboard.get("short_term_outlook", "")),
        medium_term_outlook=str(dashboard.get("medium_term_outlook", "")),
        technical_analysis=str(dashboard.get("technical_analysis", "")),
        fundamental_analysis=str(dashboard.get("fundamental_analysis", "")),
        news_summary=str(dashboard.get("news_summary", "")),
        data_sources=str(dashboard.get("data_sources", "")),
    )

    return LLMAnalysisResult(
        sentiment_score=max(-1.0, min(1.0, sentiment_norm)),
        operation_advice=dashboard.get("operation_advice", "观望"),
        confidence={"高": 0.9, "中": 0.6, "低": 0.3}.get(str(dashboard.get("confidence_level", "中")), 0.5),
        summary=dashboard.get("analysis_summary", ""),
        key_points=[p.strip() for p in str(dashboard.get("key_points", "")).split(",") if p.strip()],
        risk_warnings=intel.get("risk_alerts", []) if isinstance(intel, dict) else [],
        target_price=_parse_price(sp.get("take_profit")),
        stop_loss_price=_parse_price(sp.get("stop_loss")),
        model_used=model,
        raw_response=str(dashboard),
        dashboard=dash_obj,
    )


def _noop_progress(step: int, total: int, msg: str) -> None:
    pass


def _build_llm_dashboard_dict(llm_result) -> dict:
    """Build the full dashboard dict for storage and API response."""
    base = {
        "key_points": llm_result.key_points,
        "risk_warnings": llm_result.risk_warnings,
        "target_price": llm_result.target_price,
        "stop_loss_price": llm_result.stop_loss_price,
    }
    if llm_result.dashboard:
        d = llm_result.dashboard
        base["stock_name"] = d.stock_name
        base["sentiment_score"] = d.sentiment_score
        base["trend_prediction"] = d.trend_prediction
        base["operation_advice"] = d.operation_advice
        base["decision_type"] = d.decision_type
        base["confidence_level"] = d.confidence_level
        base["dashboard"] = d.dashboard
        base["analysis_summary"] = d.analysis_summary
        base["risk_warning"] = d.risk_warning
        base["buy_reason"] = d.buy_reason
        base["trend_analysis"] = d.trend_analysis
        base["short_term_outlook"] = d.short_term_outlook
        base["medium_term_outlook"] = d.medium_term_outlook
        base["technical_analysis"] = d.technical_analysis
        base["fundamental_analysis"] = d.fundamental_analysis
        base["news_summary"] = d.news_summary
        base["data_sources"] = d.data_sources
    return base


def analyze_stock(
    code: str,
    on_progress: ProgressCallback | None = None,
    on_ohlcv: Callable[[dict], None] | None = None,
    *,
    user_id: int | None = None,
    upsert: bool = False,
) -> FullAnalysisResponse:
    """Run the full analysis pipeline for a single stock."""
    emit = on_progress or _noop_progress
    t0 = time.time()
    norm = normalize_instrument(code)

    # 1 — stock name
    emit(1, TOTAL_STEPS, "获取股票名称")
    names = fetch_stock_names([norm])
    stock_name = names.get(norm, "")

    # 2 — OHLCV
    emit(2, TOTAL_STEPS, "拉取行情数据")
    df, data_source = fetch_daily_ohlcv(code)

    src_label = data_source or "未知"
    if df is not None and not df.empty:
        emit(2, TOTAL_STEPS, f"行情数据就绪（来源：{src_label}，{len(df)} 条）")
    else:
        emit(2, TOTAL_STEPS, f"行情数据获取失败（尝试来源：{src_label}）")

    if on_ohlcv and df is not None and not df.empty:
        try:
            bars = _extract_ohlcv_bars(df, tail=60)
            if bars:
                on_ohlcv({"type": "ohlcv", "bars": [b.model_dump() for b in bars]})
        except Exception:
            pass
    if df is None or df.empty:
        return FullAnalysisResponse(
            code=norm,
            name=stock_name,
            analysis_date=datetime.now().isoformat(),
            signal_score=0,
            buy_signal="数据不足",
            risk_factors=["无法获取股票行情数据"],
            processing_time_ms=int((time.time() - t0) * 1000),
        )

    # 3 — technical
    emit(3, TOTAL_STEPS, "技术面打分")
    analyzer = StockTrendAnalyzer()
    tech_result = analyzer.analyze(df, norm)

    # 4 — model
    emit(4, TOTAL_STEPS, "模型预测查询")
    model_prediction = _get_model_prediction(code)
    model_score = model_prediction["score"] if model_prediction else None
    model_percentile = model_prediction["percentile"] if model_prediction else None
    if model_prediction:
        rank = model_prediction["rank_in_pool"]
        pool = model_prediction["pool_size"]
        top_label = "Top推荐" if model_prediction["top_n"] else ""
        emit(4, TOTAL_STEPS, f"模型预测就绪（排名 {rank}/{pool} {top_label}）")
    else:
        emit(4, TOTAL_STEPS, "模型预测：暂无可用预测（跳过）")

    # 5 — news
    emit(5, TOTAL_STEPS, "搜索新闻情报")
    news_context = _fetch_news_context(norm, stock_name)
    if news_context:
        emit(5, TOTAL_STEPS, "新闻情报就绪")
    else:
        emit(5, TOTAL_STEPS, "新闻情报：无可用数据（跳过）")

    # 6 — chip
    emit(6, TOTAL_STEPS, "获取筹码分布")
    chip_data = _fetch_chip_data(norm)
    if chip_data:
        emit(6, TOTAL_STEPS, "筹码分布就绪（来源：AKShare）")
    else:
        emit(6, TOTAL_STEPS, "筹码分布：无可用数据（跳过）")

    # 7 — fundamental
    emit(7, TOTAL_STEPS, "获取基本面数据")
    fundamental_data = _fetch_fundamental_data(norm)
    if fundamental_data:
        emit(7, TOTAL_STEPS, "基本面数据就绪（来源：AKShare）")
    else:
        emit(7, TOTAL_STEPS, "基本面数据：无可用数据（跳过）")

    # 8 — LLM / Agent
    emit(8, TOTAL_STEPS, "LLM 深度分析（可能较慢）")
    llm_result = _run_agent_analysis(
        code=norm,
        stock_name=stock_name,
        tech_result=tech_result,
        model_prediction=model_prediction,
        ohlcv_df=df,
        news_context=news_context,
        chip_data=chip_data,
        fundamental_data=fundamental_data,
        emit=emit,
    )

    elapsed_ms = int((time.time() - t0) * 1000)
    llm_dashboard_dict = _build_llm_dashboard_dict(llm_result)

    # 9 — persist
    emit(9, TOTAL_STEPS, "写入数据库")
    llm_available = llm_result.summary != "LLM分析不可用"
    field_values = dict(
        name=stock_name,
        signal_score=tech_result.signal_score,
        buy_signal=tech_result.buy_signal.value,
        signal_reasons=tech_result.signal_reasons,
        risk_factors=tech_result.risk_factors,
        trend_status=tech_result.trend_status.value,
        technical_detail=tech_result.to_dict(),
        model_score=model_score,
        model_percentile=model_percentile,
        llm_sentiment=llm_result.sentiment_score if llm_available else None,
        llm_operation_advice=llm_result.operation_advice,
        llm_confidence=llm_result.confidence if llm_available else None,
        llm_summary=llm_result.summary,
        llm_dashboard=llm_dashboard_dict,
        llm_raw_response=llm_result.raw_response,
        llm_model_used=llm_result.model_used,
        data_source=data_source,
        processing_time_ms=elapsed_ms,
        analysis_date=datetime.now(),
    )
    with get_db() as db:
        record: AnalysisHistory | None = None
        if upsert and user_id is not None:
            record = (
                db.query(AnalysisHistory)
                .filter(AnalysisHistory.code == norm, AnalysisHistory.user_id == user_id)
                .order_by(AnalysisHistory.id.desc())
                .first()
            )
        if record is not None:
            for k, v in field_values.items():
                setattr(record, k, v)
        else:
            record = AnalysisHistory(code=norm, user_id=user_id, **field_values)
            db.add(record)
        db.flush()
        record_id = record.id

    logger.info(
        f"Analysis complete for {norm} ({stock_name}): "
        f"score={tech_result.signal_score}, signal={tech_result.buy_signal.value}, "
        f"model={'%.4f' % model_score if model_score else 'N/A'}, "
        f"llm={llm_result.operation_advice}, {elapsed_ms}ms"
    )

    ohlcv_bars = _extract_ohlcv_bars(df, tail=60)

    return FullAnalysisResponse(
        id=record_id,
        code=norm,
        name=stock_name,
        analysis_date=datetime.now().isoformat(),
        signal_score=tech_result.signal_score,
        buy_signal=tech_result.buy_signal.value,
        signal_reasons=tech_result.signal_reasons,
        risk_factors=tech_result.risk_factors,
        trend_status=tech_result.trend_status.value,
        technical_detail=tech_result.to_dict(),
        model_score=model_score,
        model_percentile=model_percentile,
        llm_sentiment=llm_result.sentiment_score if llm_available else None,
        llm_operation_advice=llm_result.operation_advice,
        llm_confidence=llm_result.confidence if llm_available else None,
        llm_summary=llm_result.summary,
        llm_dashboard=llm_dashboard_dict,
        ohlcv=ohlcv_bars,
        data_source=data_source,
        processing_time_ms=elapsed_ms,
    )


# ---------------------------------------------------------------------------
# Data enrichment helpers (news, chip, fundamental)
# ---------------------------------------------------------------------------


def _fetch_news_context(code: str, stock_name: str) -> str | None:
    """Fetch news/intel context via SearchService. Returns None if unavailable."""
    try:
        from stopat30m.analysis.search_service import get_search_service

        svc = get_search_service()
        if not svc.is_available:
            return None
        resp = svc.search_stock_news(code, stock_name)
        if not resp.success or not resp.results:
            return None
        return resp.to_context()
    except Exception as e:
        logger.debug(f"News search failed for {code}: {e}")
        return None


def _fetch_chip_data(code: str) -> dict | None:
    """Fetch chip distribution. Returns None if unavailable."""
    try:
        from stopat30m.analysis.chip import fetch_chip_distribution
        return fetch_chip_distribution(code)
    except Exception as e:
        logger.debug(f"Chip data fetch failed for {code}: {e}")
        return None


def _fetch_fundamental_data(code: str) -> dict | None:
    """Fetch fundamental data (PE/PB/MV + financial reports). Returns None if unavailable."""
    try:
        from stopat30m.analysis.fundamental import fetch_fundamental_context
        return fetch_fundamental_context(code)
    except Exception as e:
        logger.debug(f"Fundamental data fetch failed for {code}: {e}")
        return None


# ---------------------------------------------------------------------------
# Streaming wrapper
# ---------------------------------------------------------------------------


def analyze_stock_streaming(code: str, *, user_id: int | None = None) -> Generator[dict, None, None]:
    """Generator that yields SSE-compatible progress dicts then the final result."""
    import threading
    import queue as _queue

    progress_q: _queue.Queue[dict | None] = _queue.Queue()
    result_holder: list[FullAnalysisResponse] = []

    def _bg() -> None:
        def _push(step: int, total: int, msg: str) -> None:
            progress_q.put({"type": "progress", "step": step, "total": total, "message": msg})
        try:
            res = analyze_stock(code, on_progress=_push, on_ohlcv=progress_q.put, user_id=user_id)
            result_holder.append(res)
        except Exception as exc:
            progress_q.put({"type": "error", "message": str(exc)})
        finally:
            progress_q.put(None)

    t = threading.Thread(target=_bg, daemon=True)
    t.start()

    while True:
        item = progress_q.get()
        if item is None:
            break
        yield item

    t.join(timeout=5)

    if result_holder:
        yield {"type": "result", "data": result_holder[0].model_dump()}
    else:
        yield {"type": "error", "message": "分析结束但未产生结果"}
