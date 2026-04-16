#!/usr/bin/env python3
"""
StopAt30M - AI-powered A-share quantitative trading system.

Commands:
    download    Download A-share data
    train       Train alpha model
    cache-predictions Cache model predictions for reuse
    backtest    Backtest trained model on test data
    signal-backtest  Backtest generated signals
    account-backtest Account-level trading backtest
    signal      Generate trading signals
    trade       Start trading engine
    dashboard   Launch monitoring dashboard
    info        Show factor library info
"""

import sys

import click
from loguru import logger

logger.remove()
logger.add(sys.stderr, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{level:<7}</level> | {message}")


@click.group()
@click.option("--config", default=None, help="Path to config.yaml")
@click.pass_context
def cli(ctx: click.Context, config: str | None) -> None:
    """StopAt30M - AI量化交易系统"""
    ctx.ensure_object(dict)
    if config:
        from stopat30m.config import load_config
        load_config(config)


@cli.command()
@click.option("--source", default="baostock",
              type=click.Choice(["baostock", "qlib+baostock", "qlib", "akshare", "efinance", "tushare"]),
              help="Data source: baostock (default, incremental), qlib+baostock (full rebuild), qlib (base only ~2020)")
@click.option("--target-dir", default=None, help="Target directory")
@click.option("--start-date", default=None, help="Start date YYYY-MM-DD (full download only)")
@click.option("--end-date", default=None, help="End date YYYY-MM-DD (default today)")
@click.option("--full", is_flag=True, default=False,
              help="Full download from scratch (Qlib base + BaoStock to today). Equivalent to --source qlib+baostock")
@click.option("--single-source", is_flag=True, default=False,
              help="Disable multi-source parallel fetching; use only the specified --source")
@click.option("--workers", default=None, type=int,
              help="Parallel sub-processes per source (BaoStock default: 4, others: 1). "
                   "Set to 1 to disable intra-source parallelism.")
@click.option("--rebuild-meta", is_flag=True, default=False,
              help="Rebuild data_meta.json by scanning existing features + fetching listing info (no price data downloaded)")
def download(source: str, target_dir: str | None, start_date: str | None, end_date: str | None,
             full: bool, single_source: bool, workers: int | None, rebuild_meta: bool) -> None:
    """Download A-share market data.

    Default: incremental update using multiple sources in parallel (BaoStock + AkShare).
    BaoStock uses 4 parallel sub-processes by default (no rate limit).
    Use --workers N to control parallelism. Use --single-source for single source only.
    Use --full to do a complete rebuild from Qlib official base + BaoStock.
    """
    if rebuild_meta:
        from pathlib import Path

        from stopat30m.config import get
        from stopat30m.data.fetcher import META_FILENAME, build_meta_from_scan, fetch_stock_listing_info

        qlib_cfg = get("qlib") or {}
        data_dir = Path(target_dir or qlib_cfg.get("provider_uri", "~/.qlib/qlib_data/cn_data")).expanduser()

        click.echo(f"Rebuilding data_meta.json from {data_dir} ...")
        from stopat30m.data.provider import _create_fetcher
        fetcher = _create_fetcher("baostock")
        stock_info = fetch_stock_listing_info(fetcher)
        meta = build_meta_from_scan(data_dir, stock_info=stock_info)
        meta.save(data_dir / META_FILENAME)
        click.echo(
            f"Done: {len(meta.stocks)} stocks, "
            f"trusted_until={meta.trusted_until}, "
            f"qlib_base_end={meta.qlib_base_end}"
        )
        return

    if full:
        source = "qlib+baostock"

    from stopat30m.data.provider import download_cn_data
    download_cn_data(
        source=source, target_dir=target_dir,
        start_date=start_date, end_date=end_date,
        parallel=not single_source,
        workers=workers,
    )


@cli.command("check-data")
@click.option("--fix", is_flag=True, default=False, help="Delete corrupt/empty stock dirs")
def check_data(fix: bool) -> None:
    """Diagnose local Qlib data: completeness, integrity, coverage."""
    from pathlib import Path

    from stopat30m.config import get
    from stopat30m.data.fetcher import META_FILENAME, DataMeta

    qlib_cfg = get("qlib") or {}
    data_dir = Path(qlib_cfg.get("provider_uri", "~/.qlib/qlib_data/cn_data")).expanduser()

    click.echo(f"\n{'=' * 62}")
    click.echo(f"  数据诊断: {data_dir}")
    click.echo(f"{'=' * 62}\n")

    # 1. Calendar
    cal_path = data_dir / "calendars" / "day.txt"
    if not cal_path.exists():
        click.echo("  No calendar found. Run:")
        click.echo("    py main.py download --source qlib+baostock")
        return

    cal_dates = [d.strip() for d in cal_path.read_text().strip().split("\n") if d.strip()]
    click.echo(f"  日历: {cal_dates[0]} ~ {cal_dates[-1]} ({len(cal_dates)} 个交易日)")

    # 2. Checkpoint
    cp_path = data_dir / ".append_progress"
    if cp_path.exists():
        lines = cp_path.read_text().strip().split("\n")
        parts = lines[0].split("|")
        done = len(lines) - 1
        click.echo(f"  checkpoint: 范围 {parts[0]}~{parts[1]}, 已完成 {done} 只 (有中断)")
    else:
        click.echo(f"  checkpoint: 无中断")

    # 3. Meta
    meta_path = data_dir / META_FILENAME
    if not meta_path.exists():
        click.echo(f"\n  data_meta.json 不存在!")
        click.echo(f"  建议运行: py main.py download --source qlib+baostock")
        click.echo(f"  或重建 meta: py main.py download --rebuild-meta")
        click.echo(f"{'=' * 62}\n")
        return

    meta = DataMeta.load(meta_path)

    # --- Per-stock analysis ---
    active_total = 0
    delisted_count = 0
    delisted_incomplete = 0
    index_count = 0
    no_data_new_ipo = 0
    no_data_gap = 0
    end_dates: list[str] = []

    for code, sm in sorted(meta.stocks.items()):
        if sm.status == "index":
            index_count += 1
            continue
        if sm.status == "delisted":
            delisted_count += 1
            if sm.delist_date and (not sm.data_end or sm.data_end < sm.delist_date):
                delisted_incomplete += 1
            continue
        active_total += 1
        if sm.data_end:
            end_dates.append(sm.data_end)
        else:
            no_data_new_ipo += 1

    latest_end = max(end_dates) if end_dates else "N/A"
    earliest_end = min(end_dates) if end_dates else "N/A"
    at_latest = sum(1 for d in end_dates if d == latest_end)
    has_data = len(end_dates)

    no_data_total = no_data_new_ipo
    no_data_pre_watermark = 0
    if meta.trusted_until:
        for code, sm in meta.stocks.items():
            if sm.status in ("delisted", "index") or sm.data_end:
                continue
            if not sm.ipo_date or sm.ipo_date <= meta.trusted_until:
                no_data_pre_watermark += 1
        no_data_post_watermark = no_data_total - no_data_pre_watermark
    else:
        no_data_post_watermark = no_data_total

    # --- Core output: the dividing line ---
    click.echo(f"\n{'─' * 62}")
    if meta.trusted_until:
        click.echo(f"  {meta.trusted_until} 之前  →  所有已有数据完整可信")
        click.echo(f"  {meta.trusted_until} 之后  →  需要增量更新")
    else:
        click.echo(f"  无可信水位线 (本地无任何数据)")
    click.echo(f"{'─' * 62}")

    click.echo(f"\n  可信水位线:   {meta.trusted_until or '(无)'}")
    click.echo(f"  日历范围:     {cal_dates[0]} ~ {cal_dates[-1]} ({len(cal_dates)} 天)")
    click.echo(f"  上次增量:     {meta.last_append}")
    click.echo(f"  股票列表更新: {meta.listing_updated or '(未更新)'}")

    click.echo(f"\n  Active 股票:  {active_total} 只")
    click.echo(f"  有数据:       {has_data} 只 (最远到 {latest_end}, 最短到 {earliest_end})")
    if no_data_post_watermark:
        click.echo(f"  待拉取 (新股): {no_data_post_watermark} 只 (上市晚于水位线)")
    if no_data_pre_watermark:
        click.echo(f"  待拉取 (补全): {no_data_pre_watermark} 只 (原数据源未覆盖)")
    click.echo(f"  退市:         {delisted_count} 只"
               f"{f' (其中 {delisted_incomplete} 只需补到退市日)' if delisted_incomplete else ''}")
    if index_count:
        click.echo(f"  指数 (已排除): {index_count} 只")

    # --- Watermark outliers ---
    outliers = meta.watermark_outliers()
    if outliers:
        click.echo(f"\n  水位线排除 (长期停牌/异常): {len(outliers)} 只")
        for sm in sorted(outliers, key=lambda s: s.data_end or "")[:10]:
            click.echo(f"    {sm.code}  data_end={sm.data_end}  ipo={sm.ipo_date or '?'}")
        if len(outliers) > 10:
            click.echo(f"    ... 还有 {len(outliers) - 10} 只")

    if meta.trusted_until:
        behind = [d for d in end_dates if d < meta.trusted_until]
        if behind:
            click.echo(f"\n  低于水位线的: {len(behind)} 只 (data_end < {meta.trusted_until})")

    # --- Checkpoint ---
    cp_path = data_dir / ".append_progress"
    if cp_path.exists():
        lines = cp_path.read_text().strip().split("\n")
        parts = lines[0].split("|")
        done = len(lines) - 1
        click.echo(f"\n  有中断的下载: {parts[0]}~{parts[1]}, 已完成 {done} 只")

    # --- Recommendation ---
    from datetime import datetime as _dt
    today = _dt.now().strftime("%Y-%m-%d")
    if not meta.trusted_until:
        click.echo(f"\n  本地无数据，建议全量下载:")
        click.echo(f"    py main.py download --full")
    elif meta.trusted_until >= today:
        click.echo(f"\n  数据已是最新，无需更新")
    else:
        gap = f"{meta.trusted_until} → {today}"
        click.echo(f"\n  需要增量更新 ({gap}):")
        click.echo(f"    py main.py download")

    # --- Instruments vs binary consistency ---
    from stopat30m.data.fetcher.qlib_dumper import rebuild_instruments_from_binary
    import struct as _struct

    inst_path_all = data_dir / "instruments" / "all.txt"
    feat_dir = data_dir / "features"
    inst_desync = 0
    if inst_path_all.exists() and feat_dir.exists():
        old_inst: dict[str, tuple[str, str]] = {}
        for _line in inst_path_all.read_text().strip().split("\n"):
            _parts = _line.strip().split("\t")
            if len(_parts) >= 3:
                old_inst[_parts[0]] = (_parts[1], _parts[2])

        for sd in feat_dir.iterdir():
            if not sd.is_dir():
                continue
            sym = sd.name.upper()
            cb = sd / "close.day.bin"
            if not cb.exists() or cb.stat().st_size < 8:
                continue
            start_idx = int(_struct.unpack("<f", cb.read_bytes()[:4])[0])
            n = (cb.stat().st_size - 4) // 4
            end_idx = start_idx + n - 1
            if start_idx < 0 or start_idx >= len(cal_dates):
                continue
            bin_end = cal_dates[min(end_idx, len(cal_dates) - 1)]
            if sym in old_inst:
                if old_inst[sym][1] < bin_end:
                    inst_desync += 1
            else:
                inst_desync += 1

    if inst_desync > 0:
        click.echo(f"\n  ⚠ instruments 与二进制数据不同步: {inst_desync} 只")
        click.echo(f"    (instruments 记录的结束日期落后于实际文件)")
        if not fix:
            click.echo(f"    修复命令: py main.py check-data --fix")
    else:
        click.echo(f"\n  instruments 与二进制数据一致 ✓")

    if fix:
        total_entries, changed_count = rebuild_instruments_from_binary(data_dir)
        if changed_count:
            click.echo(f"\n  已重建 instruments: {total_entries} 条, 修正 {changed_count} 条")
        else:
            click.echo(f"\n  instruments 无需修复")

        from stopat30m.data.fetcher.qlib_dumper import sync_meta_from_binary
        meta_synced = sync_meta_from_binary(data_dir)
        if meta_synced:
            click.echo(f"  已同步 data_meta: {meta_synced} 只股票的 data_end 已更新")
        else:
            click.echo(f"  data_meta 无需修复")

        if (no_data_pre_watermark + no_data_post_watermark) > 0:
            import shutil
            cleaned = 0
            for code, sm in meta.stocks.items():
                if sm.data_end is None and sm.status != "delisted":
                    sym = f"sh{code}" if code.startswith("6") else f"sz{code}"
                    d = feat_dir / sym
                    if d.exists() and not any(d.iterdir()):
                        shutil.rmtree(d, ignore_errors=True)
                        cleaned += 1
            if cleaned:
                click.echo(f"  清理了 {cleaned} 个空目录")

    click.echo(f"\n{'=' * 62}\n")


def _show_top_predictions(pred: "pd.Series | pd.DataFrame", top_k: int) -> None:
    """Print the top-K stocks by predicted 5-day return from the latest date."""
    import pandas as pd

    if isinstance(pred, pd.DataFrame):
        pred = pred.iloc[:, 0]

    if not isinstance(pred.index, pd.MultiIndex):
        click.echo("Predictions have no MultiIndex (datetime, instrument); cannot extract top stocks.")
        return

    latest_date = pred.index.get_level_values(0).max()
    latest = pred.xs(latest_date, level=0).dropna().sort_values(ascending=False)

    if latest.empty:
        click.echo(f"No valid predictions for {latest_date}.")
        return

    top = latest.head(top_k)

    click.echo(f"\n{'=' * 58}")
    click.echo(f"  Top {len(top)} stocks predicted to rise (next 5 trading days)")
    click.echo(f"  Prediction date: {latest_date}")
    click.echo(f"{'=' * 58}")
    click.echo(f"  {'Rank':<6}{'Stock':<16}{'Pred Return':>14}")
    click.echo(f"  {'-' * 52}")
    for rank, (instrument, score) in enumerate(top.items(), 1):
        click.echo(f"  {rank:<6}{str(instrument):<16}{score:>+13.2%}")
    click.echo(f"{'=' * 58}\n")


def _save_train_run_to_db(
    metrics: dict,
    model_type: str,
    model_path: str,
    universe: str,
    num_features: int,
    factor_groups: str | None,
) -> None:
    """Persist training metrics + config snapshot into BacktestRun table."""
    try:
        from stopat30m.storage.database import get_db
        from stopat30m.storage.models import BacktestRun

        model_params = get("model", "params") or {}
        active_params = model_params.get(model_type, {})

        config_snapshot = {
            "model_type": model_type,
            "model_path": model_path,
            "universe": universe,
            "num_features": num_features,
            "factor_groups": factor_groups,
            "model_params": active_params,
            "data": {
                "train_start": get("data", "train_start", ""),
                "train_end": get("data", "train_end", ""),
                "valid_start": get("data", "valid_start", ""),
                "valid_end": get("data", "valid_end", ""),
                "test_start": get("data", "test_start", ""),
            },
        }

        with get_db() as db:
            run = BacktestRun(
                kind="train",
                tag=f"{model_type}_{universe}",
                run_dir=model_path,
                annual_return=None,
                sharpe=None,
                max_drawdown=None,
                total_trades=None,
                win_rate=None,
                config=config_snapshot,
                report=metrics,
            )
            db.add(run)
        logger.info("Training run saved to database")
    except Exception as e:
        logger.warning(f"Could not save training run to DB: {e}")


@cli.command()
@click.option("--model-type", default=None, help="lgbm / xgboost / mlp / lstm / transformer")
@click.option("--save-name", default="model", help="Model save name")
@click.option("--factor-groups", default=None, help="Comma-separated factor groups")
@click.option("--universe", default=None, help="Stock universe: csi300 / csi500 / all")
@click.option("--top-k", default=0, type=int, help="Output top K predicted stocks after training (0=skip)")
def train(model_type: str | None, save_name: str, factor_groups: str | None, universe: str | None, top_k: int) -> None:
    """Train alpha prediction model."""
    import json
    from pathlib import Path

    from stopat30m.data.provider import init_qlib
    from stopat30m.factors.handler import AlphaExtendedHandler
    from stopat30m.model.trainer import TrainingProgress, train_and_evaluate

    progress = TrainingProgress(interval=10.0)
    progress.start()

    init_qlib()

    groups = factor_groups.split(",") if factor_groups else None

    with progress.phase("Building dataset", total_steps=2) as p:
        handler = AlphaExtendedHandler(groups=groups, instruments=universe)
        logger.info(f"Universe: {handler.instruments}, features: {handler.num_features}")
        dataset = handler.build_dataset(on_step=p.step)

    results = train_and_evaluate(dataset, model_type=model_type, save_name=save_name, progress=progress)

    output = Path("./output")
    output.mkdir(parents=True, exist_ok=True)
    with open(output / "metrics.json", "w") as f:
        json.dump(results["metrics"], f, indent=2)

    progress.finish()
    logger.info(f"Model saved: {results['model_path']}")

    _save_train_run_to_db(
        metrics=results["metrics"],
        model_type=model_type or get("model", "type", "lgbm"),
        model_path=str(results["model_path"]),
        universe=handler.instruments,
        num_features=handler.num_features,
        factor_groups=factor_groups,
    )

    if top_k > 0:
        _show_top_predictions(results["predictions"], top_k)


@cli.command()
@click.option("--model-path", default=None, help="Path to .pkl model")
@click.option("--pred-path", default=None, help="Path to cached predictions .pkl")
@click.option("--universe", default=None, help="Stock universe: csi300 / csi500 / all")
@click.option("--top-k", default=None, type=int, help="Number of stocks to hold (default from config)")
@click.option("--rebalance-freq", default=None, type=int, help="Rebalance every N trading days")
@click.option("--deal-price", default=None, type=click.Choice(["open", "close"]),
              help="Execution price: open (realistic) or close (optimistic)")
@click.option("--factor-groups", default=None, help="Comma-separated factor groups")
def backtest(model_path: str | None, pred_path: str | None, universe: str | None,
             top_k: int | None, rebalance_freq: int | None, deal_price: str | None,
             factor_groups: str | None) -> None:
    """Backtest a trained model on test data."""
    from stopat30m.backtest.common import load_prediction_source
    from stopat30m.backtest.engine import BacktestEngine

    pred, _labels, metadata = load_prediction_source(
        model_path=model_path,
        pred_path=pred_path,
        universe=universe,
        factor_groups=factor_groups,
    )
    logger.info(f"Prediction source: {metadata.get('source', 'unknown')}, rows: {len(pred)}")

    kwargs: dict = {}
    if top_k is not None:
        kwargs["top_k"] = top_k
    if rebalance_freq is not None:
        kwargs["rebalance_freq"] = rebalance_freq
    if deal_price is not None:
        kwargs["deal_price"] = deal_price

    engine = BacktestEngine(**kwargs)
    result = engine.run(pred)
    result.save()

    m = result.metrics
    click.echo(f"\n{'=' * 58}")
    click.echo(f"  Backtest Results ({result.config.get('test_days', '?')} trading days)")
    click.echo(f"{'=' * 58}")
    click.echo(f"  {'Annual Return':<24}{m.get('annual_return', 0):>+10.2%}")
    click.echo(f"  {'Sharpe Ratio':<24}{m.get('sharpe', 0):>10.2f}")
    click.echo(f"  {'Sortino Ratio':<24}{m.get('sortino', 0):>10.2f}")
    click.echo(f"  {'Max Drawdown':<24}{m.get('max_drawdown', 0):>10.2%}")
    click.echo(f"  {'Calmar Ratio':<24}{m.get('calmar', 0):>10.2f}")
    click.echo(f"  {'Win Rate':<24}{m.get('win_rate', 0):>10.2%}")
    click.echo(f"  {'Profit/Loss Ratio':<24}{m.get('profit_loss_ratio', 0):>10.2f}")
    if "excess_return" in m:
        click.echo(f"  {'Excess Return (vs bench)':<24}{m['excess_return']:>+10.2%}")
    click.echo(f"  {'Total Trading Days':<24}{m.get('total_trades', 0):>10d}")
    click.echo(f"{'=' * 58}")
    click.echo(f"  Results saved to ./output/backtest/")
    click.echo(f"  View in dashboard: py main.py dashboard")
    click.echo(f"{'=' * 58}\n")


@cli.command("cache-predictions")
@click.option("--model-path", required=True, help="Path to .pkl model")
@click.option("--universe", default=None, help="Stock universe: csi300 / csi500 / all")
@click.option("--factor-groups", default=None, help="Comma-separated factor groups")
@click.option("--tag", default="", help="Optional output tag")
def cache_predictions(
    model_path: str,
    universe: str | None,
    factor_groups: str | None,
    tag: str,
) -> None:
    """Generate predictions once and save them for reuse by backtests."""
    import time as _time
    from stopat30m.backtest.common import load_model_predictions, save_prediction_bundle

    t0 = _time.time()

    def _progress(msg: str) -> None:
        elapsed = _time.time() - t0
        click.echo(f"  {elapsed:6.1f}s  {msg}")

    pred, labels, handler = load_model_predictions(
        model_path=model_path,
        universe=universe,
        factor_groups=factor_groups,
        on_step=_progress,
    )

    _progress("Saving prediction bundle...")
    out = save_prediction_bundle(
        pred,
        labels=labels,
        metadata={
            "source": "model",
            "model_path": model_path,
            "universe": handler.instruments,
            "feature_count": handler.num_features,
            "factor_groups": factor_groups.split(",") if factor_groups else None,
        },
        tag=tag,
    )

    elapsed = _time.time() - t0
    dates = sorted(pred.index.get_level_values(0).unique())
    instruments = pred.index.get_level_values(1).unique()
    click.echo(f"\nDone in {elapsed:.1f}s")
    click.echo(f"  Output:      {out}")
    click.echo(f"  Rows:        {len(pred)}")
    click.echo(f"  Universe:    {handler.instruments}")
    click.echo(f"  Instruments: {len(instruments)}")
    click.echo(f"  Date range:  {str(dates[0])[:10]} ~ {str(dates[-1])[:10]} ({len(dates)} days)")
    click.echo(f"  Features:    {handler.num_features}")


@cli.command("signal-backtest")
@click.option("--model-path", default=None, help="Path to .pkl model")
@click.option("--pred-path", default=None, help="Path to cached predictions .pkl")
@click.option("--universe", default=None, help="Stock universe: csi300 / csi500 / all")
@click.option("--top-k", default=None, type=int, help="Number of stocks per signal")
@click.option("--method", default=None, type=click.Choice(["top_k", "long_short", "quantile"]),
              help="Signal construction method")
@click.option("--rebalance-freq", default=None, type=int, help="Rebalance every N trading days")
@click.option("--horizons", default=None, help="Comma-separated forward return horizons, e.g. 1,3,5,10,20")
@click.option("--group-count", default=None, type=int, help="Number of quantile buckets")
@click.option("--benchmark", default=None, help="Benchmark instrument, e.g. SH000300")
@click.option("--factor-groups", default=None, help="Comma-separated factor groups")
@click.option("--tag", default="", help="Optional output tag")
def signal_backtest(
    model_path: str | None,
    pred_path: str | None,
    universe: str | None,
    top_k: int | None,
    method: str | None,
    rebalance_freq: int | None,
    horizons: str | None,
    group_count: int | None,
    benchmark: str | None,
    factor_groups: str | None,
    tag: str,
) -> None:
    """Run signal-level backtest and save a run under output/backtests/signal."""
    from stopat30m.backtest.common import load_prediction_source
    from stopat30m.backtest.signal_backtest import SignalBacktestEngine
    from stopat30m.config import get

    cfg = get("signal_backtest") or {}
    pred, _, metadata = load_prediction_source(
        model_path=model_path,
        pred_path=pred_path,
        universe=universe,
        factor_groups=factor_groups,
    )
    logger.info(
        f"Prediction source={metadata.get('source', '?')}, "
        f"universe={metadata.get('universe', metadata.get('source_path', '?'))}"
    )

    parsed_horizons = [int(v) for v in horizons.split(",")] if horizons else cfg.get("horizons", [1, 3, 5, 10, 20])
    engine = SignalBacktestEngine(
        top_k=top_k or cfg.get("top_k", 10),
        method=method or cfg.get("method", "top_k"),
        rebalance_freq=rebalance_freq or cfg.get("rebalance_freq", 5),
        horizons=parsed_horizons,
        group_count=group_count or cfg.get("group_count", 10),
        benchmark=benchmark or cfg.get("benchmark", "SH000300"),
    )
    result = engine.run(pred)
    run_dir = result.save(tag=tag)

    click.echo(f"\nSignal backtest saved to: {run_dir}")
    click.echo(f"Top-K: {engine.top_k} | method={engine.method} | rebalance={engine.rebalance_freq}")
    click.echo(f"Avg turnover: {result.summary_metrics.get('avg_turnover', 0):.2%}")
    click.echo(f"Primary IC mean: {result.summary_metrics.get(f'ic_{engine.eval_horizon}d_mean', 0):.4f}")
    click.echo(f"Primary RankIC mean: {result.summary_metrics.get(f'rank_ic_{engine.eval_horizon}d_mean', 0):.4f}")
    click.echo(f"Signal {engine.eval_horizon}D mean return: {result.summary_metrics.get(f'buy_mean_return_{engine.eval_horizon}d', 0):.2%}")


@cli.command("account-backtest")
@click.option("--model-path", default=None, help="Path to .pkl model")
@click.option("--pred-path", default=None, help="Path to cached predictions .pkl")
@click.option("--universe", default=None, help="Stock universe: csi300 / csi500 / all")
@click.option("--top-k", default=None, type=int, help="Number of stocks to hold")
@click.option("--method", default=None, type=click.Choice(["top_k", "quantile"]),
              help="Signal construction method for account backtest")
@click.option("--rebalance-freq", default=None, type=int, help="Rebalance every N trading days")
@click.option("--execution-price", default=None, type=click.Choice(["open", "close"]),
              help="Execution price model")
@click.option("--order-type", default=None, type=click.Choice(["market", "limit"]),
              help="Order submission type for account backtest")
@click.option("--slippage-bps", default=None, type=float, help="Round-trip slippage in basis points per side")
@click.option("--allow-partial-fill/--no-partial-fill", default=None, help="Allow partial fills when liquidity is limited")
@click.option("--participation-rate", default=None, type=float, help="Max fraction of daily volume allowed to trade")
@click.option("--initial-capital", default=None, type=float, help="Initial account capital")
@click.option("--cash-reserve-pct", default=None, type=float, help="Cash reserve ratio")
@click.option("--benchmark", default=None, help="Benchmark instrument, e.g. SH000300")
@click.option("--disable-risk-manager", is_flag=True, help="Disable pre-trade risk manager")
@click.option("--factor-groups", default=None, help="Comma-separated factor groups")
@click.option("--tag", default="", help="Optional output tag")
def account_backtest(
    model_path: str | None,
    pred_path: str | None,
    universe: str | None,
    top_k: int | None,
    method: str | None,
    rebalance_freq: int | None,
    execution_price: str | None,
    order_type: str | None,
    slippage_bps: float | None,
    allow_partial_fill: bool | None,
    participation_rate: float | None,
    initial_capital: float | None,
    cash_reserve_pct: float | None,
    benchmark: str | None,
    disable_risk_manager: bool,
    factor_groups: str | None,
    tag: str,
) -> None:
    """Run account-level backtest and save a run under output/backtests/account."""
    from stopat30m.backtest.account_backtest import AccountBacktestEngine
    from stopat30m.backtest.common import load_prediction_source
    from stopat30m.config import get

    cfg = get("account_backtest") or {}
    pred, _, metadata = load_prediction_source(
        model_path=model_path,
        pred_path=pred_path,
        universe=universe,
        factor_groups=factor_groups,
    )
    logger.info(
        f"Prediction source={metadata.get('source', '?')}, "
        f"universe={metadata.get('universe', metadata.get('source_path', '?'))}"
    )

    engine = AccountBacktestEngine(
        initial_capital=initial_capital or cfg.get("initial_capital", 1_000_000),
        top_k=top_k or cfg.get("top_k", 10),
        method=method or cfg.get("method", "top_k"),
        rebalance_freq=rebalance_freq or cfg.get("rebalance_freq", 5),
        execution_price=execution_price or cfg.get("execution_price", "open"),
        order_type=order_type or cfg.get("order_type", "market"),
        slippage_bps=slippage_bps if slippage_bps is not None else cfg.get("slippage_bps", 0.0),
        allow_partial_fill=allow_partial_fill if allow_partial_fill is not None else cfg.get("allow_partial_fill", False),
        participation_rate=participation_rate if participation_rate is not None else cfg.get("participation_rate", 0.1),
        cash_reserve_pct=cash_reserve_pct if cash_reserve_pct is not None else cfg.get("cash_reserve_pct", 0.02),
        benchmark=benchmark or cfg.get("benchmark", "SH000300"),
        enable_risk_manager=not disable_risk_manager and cfg.get("enable_risk_manager", True),
    )
    result = engine.run(pred)
    run_dir = result.save(tag=tag)

    click.echo(f"\nAccount backtest saved to: {run_dir}")
    click.echo(f"Ending equity: ¥{result.report.get('ending_equity', 0):,.2f}")
    click.echo(f"Annual return: {result.report.get('annual_return', 0):.2%}")
    click.echo(f"Sharpe: {result.report.get('sharpe', 0):.2f}")
    click.echo(f"Max drawdown: {result.report.get('max_drawdown', 0):.2%}")
    click.echo(f"Total fees: ¥{result.report.get('total_fees', 0):,.2f}")
    click.echo(f"Rejected orders: {result.report.get('rejected_orders', 0)}")


@cli.command()
@click.option("--model-path", required=True, help="Path to .pkl model")
@click.option("--date", default=None, help="Signal date (YYYY-MM-DD)")
@click.option("--publish", is_flag=True, help="Publish to Redis")
def signal(model_path: str, date: str | None, publish: bool) -> None:
    """Generate trading signals from trained model."""
    import pickle
    from pathlib import Path

    from stopat30m.data.provider import init_qlib
    from stopat30m.factors.handler import AlphaExtendedHandler
    from stopat30m.signal.generator import SignalGenerator

    init_qlib()

    with open(model_path, "rb") as f:
        model = pickle.load(f)

    handler = AlphaExtendedHandler()
    dataset = handler.build_dataset()
    pred = model.predict(dataset)

    gen = SignalGenerator()
    signals = gen.generate(pred, date=date)
    gen.save_signals(signals)

    if publish:
        gen.publish_to_redis(signals)

    click.echo(signals.to_string())


@cli.command()
@click.option("--signal-source", default=None, help="Signal file or directory")
@click.option("--poll-interval", default=60, help="Poll interval (seconds)")
def trade(signal_source: str | None, poll_interval: int) -> None:
    """Start the trading engine."""
    from pathlib import Path

    from stopat30m.trading.engine import TradingEngine
    from stopat30m.trading.strategy import AlphaPortfolioStrategy

    strategy = AlphaPortfolioStrategy()
    engine = TradingEngine(strategy=strategy)
    engine.init()

    source = Path(signal_source) if signal_source else None
    try:
        engine.start(signal_source=source, poll_interval=poll_interval)
    except KeyboardInterrupt:
        pass
    finally:
        engine.stop()


@cli.command()
def dashboard() -> None:
    """Launch the Streamlit monitoring dashboard (legacy)."""
    import subprocess
    from pathlib import Path
    dashboard_path = Path(__file__).parent / "stopat30m" / "web" / "dashboard.py"
    subprocess.run(["streamlit", "run", str(dashboard_path)], check=False)


@cli.command()
@click.option("--host", default=None, help="Bind host (default from config: server.host)")
@click.option("--port", default=None, type=int, help="Bind port (default from config: server.port)")
@click.option("--reload", "do_reload", is_flag=True, default=False, help="Enable auto-reload")
def serve(host: str | None, port: int | None, do_reload: bool) -> None:
    """Launch the FastAPI backend server."""
    import uvicorn
    from stopat30m.config import get
    uvicorn.run(
        "stopat30m.api.app:app",
        host=host or get("server", "host", "0.0.0.0"),
        port=port or get("server", "port", 8000),
        reload=do_reload,
    )


@cli.command("dev")
def dev() -> None:
    """Start both backend + frontend dev servers (one command)."""
    import subprocess
    import sys
    import os
    from pathlib import Path
    from stopat30m.config import get

    frontend_dir = Path(__file__).parent / "stopat30m" / "web" / "frontend"
    if not (frontend_dir / "package.json").exists():
        logger.error(f"Frontend not found at {frontend_dir}. Run npm install first.")
        raise SystemExit(1)

    api_port = str(get("server", "port", 8000))
    api_host = get("server", "host", "0.0.0.0")
    python = sys.executable

    env = os.environ.copy()
    env["API_PORT"] = api_port

    logger.info(f"Starting backend on {api_host}:{api_port} ...")
    backend = subprocess.Popen(
        [python, "-m", "uvicorn", "stopat30m.api.app:app",
         "--host", api_host, "--port", api_port, "--reload"],
    )

    logger.info(f"Starting frontend (proxy → localhost:{api_port}) ...")
    frontend = subprocess.Popen(
        ["npx", "vite", "--host"],
        cwd=str(frontend_dir),
        env=env,
    )

    try:
        backend.wait()
    except KeyboardInterrupt:
        pass
    finally:
        frontend.terminate()
        backend.terminate()


@cli.command("build")
def build_frontend() -> None:
    """Build frontend for production (output to dist/)."""
    import subprocess
    from pathlib import Path

    frontend_dir = Path(__file__).parent / "stopat30m" / "web" / "frontend"
    if not (frontend_dir / "package.json").exists():
        logger.error(f"Frontend not found at {frontend_dir}")
        raise SystemExit(1)

    logger.info("Building frontend ...")
    result = subprocess.run(["npx", "vite", "build"], cwd=str(frontend_dir))
    if result.returncode == 0:
        logger.info(f"Build complete: {frontend_dir / 'dist'}")
        logger.info("Run 'py main.py serve' to start production server.")
    else:
        logger.error("Frontend build failed")
        raise SystemExit(1)


@cli.group()
def paper() -> None:
    """Paper trading account management (模拟交易)."""


@paper.command("init")
@click.option("--capital", required=True, type=float, help="Initial capital (元)")
@click.option("--state-dir", default=None, help="State directory (default: output/paper)")
def paper_init(capital: float, state_dir: str | None) -> None:
    """Initialize a new paper trading account."""
    from stopat30m.trading.broker.paper import PaperBroker

    kwargs = {}
    if state_dir:
        kwargs["state_dir"] = state_dir
    broker = PaperBroker.init_account(capital, **kwargs)
    acct = broker.get_account()
    click.echo(f"Paper account created: ¥{acct.cash:,.0f} cash")
    click.echo(f"State: {broker._state_path}")


@paper.command("status")
@click.option("--state-dir", default=None, help="State directory")
def paper_status(state_dir: str | None) -> None:
    """Show paper trading account summary."""
    from stopat30m.trading.broker.paper import PaperBroker

    broker = PaperBroker(state_dir=state_dir) if state_dir else PaperBroker()
    if not broker.is_initialized:
        click.echo("Paper account not initialized. Run: py main.py paper init --capital <amount>")
        return

    acct = broker.get_account()
    positions = broker.get_positions()

    click.echo(f"\n{'=' * 58}")
    click.echo(f"  Paper Trading Account")
    click.echo(f"{'=' * 58}")
    click.echo(f"  {'Initial Capital':<24}¥{acct.initial_capital:>12,.0f}")
    click.echo(f"  {'Cash':<24}¥{acct.cash:>12,.0f}")
    click.echo(f"  {'Market Value':<24}¥{acct.market_value:>12,.0f}")
    click.echo(f"  {'Equity':<24}¥{acct.equity:>12,.0f}")
    click.echo(f"  {'Total P&L':<24}¥{acct.total_pnl:>+12,.0f}")
    click.echo(f"  {'Return':<24}{acct.return_pct:>+12.2%}")
    click.echo(f"  {'Realized P&L':<24}¥{acct.realized_pnl:>+12,.0f}")
    click.echo(f"  {'Total Fees':<24}¥{acct.total_fees:>12,.2f}")
    click.echo(f"  {'Positions':<24}{len(positions):>12d}")
    click.echo(f"{'=' * 58}")

    if positions:
        click.echo(f"\n  {'Code':<12}{'Qty':>8}{'AvgCost':>10}{'CurPrice':>10}{'P&L':>12}{'P&L%':>8}")
        click.echo(f"  {'-' * 56}")
        for inst, pos in sorted(positions.items()):
            frozen = f" [冻结{pos.frozen_quantity}]" if pos.frozen_quantity > 0 else ""
            click.echo(
                f"  {inst:<12}{pos.quantity:>8}{pos.avg_cost:>10.2f}"
                f"{pos.current_price:>10.2f}{pos.unrealized_pnl:>+12,.0f}"
                f"{pos.pnl_pct:>+7.1%}{frozen}"
            )
        click.echo(f"{'=' * 58}\n")


@paper.command("settle")
@click.option("--date", default=None, help="Settlement date (default: today)")
@click.option("--state-dir", default=None, help="State directory")
def paper_settle(date: str | None, state_dir: str | None) -> None:
    """Run end-of-day settlement (unfreeze T+1, record NAV)."""
    from datetime import datetime as dt

    from stopat30m.trading.broker.paper import PaperBroker

    if date is None:
        date = dt.now().strftime("%Y-%m-%d")

    broker = PaperBroker(state_dir=state_dir) if state_dir else PaperBroker()
    if not broker.is_initialized:
        click.echo("Paper account not initialized.")
        return

    snap = broker.settle_day(date)
    click.echo(f"Settlement {date}:")
    click.echo(f"  Equity: ¥{snap.equity:,.0f}")
    click.echo(f"  Cash:   ¥{snap.cash:,.0f}")
    click.echo(f"  MV:     ¥{snap.market_value:,.0f}")
    click.echo(f"  Return: {snap.daily_return:+.2%}")


@paper.command("reset")
@click.option("--state-dir", default=None, help="State directory")
@click.confirmation_option(prompt="This will delete all paper trading data. Continue?")
def paper_reset(state_dir: str | None) -> None:
    """Reset paper account (keeps initial capital, wipes all trades)."""
    from stopat30m.trading.broker.paper import PaperBroker

    broker = PaperBroker(state_dir=state_dir) if state_dir else PaperBroker()
    if not broker.is_initialized:
        click.echo("Paper account not initialized.")
        return

    capital = broker.get_account().initial_capital
    broker.reset()
    click.echo(f"Paper account reset to ¥{capital:,.0f}")


@paper.command("export")
@click.option("--state-dir", default=None, help="State directory")
def paper_export(state_dir: str | None) -> None:
    """Show paths to exported CSV files."""
    from stopat30m.trading.broker.paper import PaperBroker

    broker = PaperBroker(state_dir=state_dir) if state_dir else PaperBroker()
    if not broker.is_initialized:
        click.echo("Paper account not initialized.")
        return

    click.echo(f"State:  {broker._state_path}")
    click.echo(f"Trades: {broker._trades_csv}")
    click.echo(f"NAV:    {broker._nav_csv}")

    fills = broker.get_fills()
    nav = broker.get_nav_history()
    click.echo(f"\n{len(fills)} fills, {len(nav)} NAV snapshots")


@cli.command("schedule")
@click.option("--time", "schedule_time", default=None, help="Daily execution time HH:MM (default from config)")
@click.option("--watchlist", default=None, help="Comma-separated stock codes to analyze")
@click.option("--run-immediately/--no-run-immediately", default=None,
              help="Execute once immediately on startup")
def schedule_cmd(schedule_time: str | None, watchlist: str | None, run_immediately: bool | None) -> None:
    """Start the scheduler for daily analysis + event monitoring."""
    from stopat30m.config import get
    from stopat30m.scheduler import run_with_schedule

    sched_cfg = get("schedule") or {}
    time_str = schedule_time or sched_cfg.get("time", "18:00")
    codes_raw = watchlist or sched_cfg.get("watchlist", "")
    codes = [c.strip() for c in codes_raw.split(",") if c.strip()]
    do_run_immediately = run_immediately if run_immediately is not None else sched_cfg.get("run_immediately", True)

    if not codes:
        click.echo("No watchlist configured. Set schedule.watchlist in config.yaml or use --watchlist.")
        raise SystemExit(1)

    click.echo(f"Schedule: daily at {time_str}, watchlist={codes}, run_immediately={do_run_immediately}")

    def daily_task():
        from stopat30m.analysis.pipeline import analyze_stock
        for code in codes:
            try:
                logger.info(f"Scheduled analysis: {code}")
                analyze_stock(code)
            except Exception as e:
                logger.error(f"Scheduled analysis failed for {code}: {e}")

    background_tasks = []

    em_cfg = get("event_monitor") or {}
    if em_cfg.get("enabled", False):
        interval = em_cfg.get("check_interval_seconds", 300)

        def event_check():
            from stopat30m.analysis.event_monitor import run_event_monitor_once
            run_event_monitor_once()

        background_tasks.append({
            "task": event_check,
            "interval_seconds": interval,
            "run_immediately": True,
            "name": "event_monitor",
        })

    run_with_schedule(
        task=daily_task,
        schedule_time=time_str,
        run_immediately=do_run_immediately,
        background_tasks=background_tasks if background_tasks else None,
    )


@cli.command("market-review")
@click.option("--no-push", is_flag=True, default=False, help="Skip notification push")
def market_review_cmd(no_push: bool) -> None:
    """Run a one-shot A-share market review and optionally push notification."""
    from stopat30m.analysis.market_review import run_market_review
    from stopat30m.notification import get_notification_service

    notifier = get_notification_service()
    result = run_market_review(notifier=notifier, send_notification=not no_push)

    if result:
        click.echo("Market review completed.")
        click.echo(result[:500] + ("..." if len(result) > 500 else ""))
    else:
        click.echo("Market review failed. Check logs.")


@cli.command("notify-test")
@click.option("--message", default=None, help="Custom test message")
def notify_test(message: str | None) -> None:
    """Send a test notification to all configured channels."""
    from stopat30m.notification import get_notification_service

    notifier = get_notification_service()
    channels = notifier.get_available_channels() if hasattr(notifier, "get_available_channels") else []

    if not channels and not hasattr(notifier, "is_available"):
        click.echo("No notification channels configured.")
        click.echo("Set webhook URLs or tokens in config.yaml under 'notification:'")
        return

    test_msg = message or (
        "🔔 StopAt30M 通知测试\n\n"
        "如果你看到这条消息，说明通知渠道配置正确。\n\n"
        f"发送时间: {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    click.echo(f"Sending test notification...")
    if hasattr(notifier, 'send'):
        success = notifier.send(test_msg)
        if success:
            click.echo("Test notification sent successfully.")
        else:
            click.echo("Test notification failed. Check logs for details.")
    else:
        click.echo("NotificationService.send() not available.")


@cli.command()
def info() -> None:
    """Show factor library statistics."""
    from stopat30m.factors.expressions import get_factor_groups
    from stopat30m.factors.handler import _build_alpha158_features

    base = _build_alpha158_features()
    groups = get_factor_groups()
    ext_total = sum(len(v) for v in groups.values())

    click.echo(f"\nStopAt30M Factor Library")
    click.echo(f"{'=' * 50}")
    click.echo(f"Alpha158 base factors:   {len(base)}")
    click.echo(f"Extended factors:        {ext_total}")
    click.echo(f"Total factors:           {len(base) + ext_total}")
    click.echo(f"\nExtended factor groups:")
    click.echo(f"{'-' * 50}")
    for name, factors in groups.items():
        click.echo(f"  {name:<20s}  {len(factors):>4d} factors")
    click.echo()


# ---------------------------------------------------------------------------
# User management CLI
# ---------------------------------------------------------------------------


@cli.group("user")
def user_group() -> None:
    """User management commands."""
    pass


@user_group.command("create-admin")
@click.option("--username", prompt="Admin username", help="Admin username")
@click.option("--password", prompt=True, hide_input=True, confirmation_prompt=True, help="Admin password")
def create_admin(username: str, password: str) -> None:
    """Create an admin user (interactive)."""
    from stopat30m.auth.service import hash_password
    from stopat30m.storage.database import get_db, init_db
    from stopat30m.storage.models import User

    init_db()
    with get_db() as db:
        existing = db.query(User).filter(User.username == username).first()
        if existing:
            click.echo(f"User '{username}' already exists (role={existing.role}).")
            return

        user = User(
            username=username,
            password_hash=hash_password(password),
            role="admin",
        )
        db.add(user)
        db.flush()
        click.echo(f"Admin user '{username}' created (id={user.id}).")


@user_group.command("list")
def list_users() -> None:
    """List all users."""
    from stopat30m.storage.database import get_db, init_db
    from stopat30m.storage.models import User

    init_db()
    with get_db() as db:
        users = db.query(User).order_by(User.id).all()
        if not users:
            click.echo("No users.")
            return
        click.echo(f"{'ID':<5} {'Username':<20} {'Role':<8} {'Active':<8} {'Last Login'}")
        click.echo("-" * 65)
        for u in users:
            last = u.last_login.strftime("%Y-%m-%d %H:%M") if u.last_login else "never"
            active = "yes" if u.is_active else "NO"
            click.echo(f"{u.id:<5} {u.username:<20} {u.role:<8} {active:<8} {last}")


@user_group.command("reset-password")
@click.argument("username")
@click.option("--password", prompt=True, hide_input=True, confirmation_prompt=True, help="New password")
def reset_password(username: str, password: str) -> None:
    """Reset a user's password."""
    from stopat30m.auth.service import hash_password
    from stopat30m.storage.database import get_db, init_db
    from stopat30m.storage.models import User

    init_db()
    with get_db() as db:
        user = db.query(User).filter(User.username == username).first()
        if not user:
            click.echo(f"User '{username}' not found.")
            return
        user.password_hash = hash_password(password)
        click.echo(f"Password for '{username}' has been reset.")


@user_group.command("disable")
@click.argument("username")
def disable_user(username: str) -> None:
    """Disable a user account."""
    from stopat30m.storage.database import get_db, init_db
    from stopat30m.storage.models import User

    init_db()
    with get_db() as db:
        user = db.query(User).filter(User.username == username).first()
        if not user:
            click.echo(f"User '{username}' not found.")
            return
        if not user.is_active:
            click.echo(f"User '{username}' is already disabled.")
            return
        user.is_active = False
        click.echo(f"User '{username}' has been disabled.")


@cli.group("invite")
def invite_group() -> None:
    """Invite code management."""
    pass


@invite_group.command("create")
@click.option("--expires", default="7d", help="Expiry duration (e.g. 7d, 30d)")
@click.option("--admin-user", default=None, help="Admin username (default: first admin)")
def create_invite(expires: str, admin_user: str | None) -> None:
    """Generate an invite code."""
    import secrets
    from datetime import datetime, timedelta, timezone

    from stopat30m.storage.database import get_db, init_db
    from stopat30m.storage.models import InviteCode, User

    init_db()

    days = int(expires.rstrip("d"))

    with get_db() as db:
        if admin_user:
            admin = db.query(User).filter(User.username == admin_user, User.role == "admin").first()
        else:
            admin = db.query(User).filter(User.role == "admin").first()

        if not admin:
            click.echo("No admin user found. Create one first: py main.py user create-admin")
            return

        code = secrets.token_urlsafe(16)
        now = datetime.now(timezone.utc)
        inv = InviteCode(
            code=code,
            created_by=admin.id,
            expires_at=now + timedelta(days=days),
            created_at=now,
        )
        db.add(inv)
        db.flush()
        click.echo(f"Invite code: {code}")
        click.echo(f"Expires: {inv.expires_at.strftime('%Y-%m-%d %H:%M')} ({days} days)")


if __name__ == "__main__":
    from pathlib import Path  # noqa: F811

    try:
        from stopat30m.config import load_config
        load_config()
    except FileNotFoundError:
        pass

    log_cfg_file = None
    try:
        from stopat30m.config import get
        log_cfg = get("logging") or {}
        if log_cfg.get("file"):
            log_path = Path(log_cfg["file"])
            log_path.parent.mkdir(parents=True, exist_ok=True)
            logger.add(
                str(log_path),
                level=log_cfg.get("level", "INFO"),
                rotation=log_cfg.get("rotation", "10 MB"),
                retention=log_cfg.get("retention", "30 days"),
            )
    except Exception:
        pass

    cli()
