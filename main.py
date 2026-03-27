#!/usr/bin/env python3
"""
StopAt30M - AI-powered A-share quantitative trading system.

Commands:
    download    Download A-share data
    train       Train alpha model
    backtest    Backtest trained model on test data
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
              type=click.Choice(["baostock", "qlib+baostock", "qlib", "akshare", "tushare"]),
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
    no_data_new_ipo = 0
    no_data_gap = 0
    end_dates: list[str] = []

    for code, sm in sorted(meta.stocks.items()):
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

    # Stocks without data: distinguish "new IPO after watermark" vs "not in original source"
    no_data_total = no_data_new_ipo
    no_data_pre_watermark = 0
    if meta.trusted_until:
        for code, sm in meta.stocks.items():
            if sm.status == "delisted" or sm.data_end:
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

    if fix and (no_data_pre_watermark + no_data_post_watermark) > 0:
        import shutil
        feat_dir = data_dir / "features"
        cleaned = 0
        for code, sm in meta.stocks.items():
            if sm.data_end is None and sm.status != "delisted":
                sym = f"sh{code}" if code.startswith("6") else f"sz{code}"
                d = feat_dir / sym
                if d.exists() and not any(d.iterdir()):
                    shutil.rmtree(d, ignore_errors=True)
                    cleaned += 1
        if cleaned:
            click.echo(f"\n  清理了 {cleaned} 个空目录")

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

    if top_k > 0:
        _show_top_predictions(results["predictions"], top_k)


@cli.command()
@click.option("--model-path", required=True, help="Path to .pkl model")
@click.option("--universe", default=None, help="Stock universe: csi300 / csi500 / all")
@click.option("--top-k", default=None, type=int, help="Number of stocks to hold (default from config)")
@click.option("--rebalance-freq", default=None, type=int, help="Rebalance every N trading days")
@click.option("--deal-price", default=None, type=click.Choice(["open", "close"]),
              help="Execution price: open (realistic) or close (optimistic)")
@click.option("--factor-groups", default=None, help="Comma-separated factor groups")
def backtest(model_path: str, universe: str | None, top_k: int | None,
             rebalance_freq: int | None, deal_price: str | None,
             factor_groups: str | None) -> None:
    """Backtest a trained model on test data."""
    import pickle

    from stopat30m.backtest.engine import BacktestEngine
    from stopat30m.data.provider import init_qlib
    from stopat30m.factors.handler import AlphaExtendedHandler

    init_qlib()

    with open(model_path, "rb") as f:
        model = pickle.load(f)

    groups = factor_groups.split(",") if factor_groups else None
    handler = AlphaExtendedHandler(groups=groups, instruments=universe)
    logger.info(f"Universe: {handler.instruments}, features: {handler.num_features}")
    dataset = handler.build_dataset()

    logger.info("Generating predictions on test segment...")
    pred = model.predict(dataset)

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
    """Launch the web monitoring dashboard."""
    import subprocess
    from pathlib import Path
    dashboard_path = Path(__file__).parent / "stopat30m" / "web" / "dashboard.py"
    subprocess.run(["streamlit", "run", str(dashboard_path)], check=False)


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
