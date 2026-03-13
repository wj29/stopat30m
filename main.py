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
@click.option("--source", default="qlib", type=click.Choice(["qlib", "akshare", "baostock", "tushare"]),
              help="Data source: qlib (public ~2020), akshare (free), baostock (free, no rate limit), tushare (paid)")
@click.option("--target-dir", default=None, help="Target directory")
@click.option("--start-date", default=None, help="Start date YYYY-MM-DD (full download only)")
@click.option("--end-date", default=None, help="End date YYYY-MM-DD (default today)")
@click.option("--append", is_flag=True, default=False,
              help="Incremental mode: only fetch data newer than the existing dataset")
@click.option("--retry-skipped", is_flag=True, default=False,
              help="Retry stocks skipped in a previous append (e.g. due to rate limiting)")
def download(source: str, target_dir: str | None, start_date: str | None, end_date: str | None, append: bool, retry_skipped: bool) -> None:
    """Download A-share market data."""
    from stopat30m.data.provider import download_cn_data
    download_cn_data(source=source, target_dir=target_dir, start_date=start_date, end_date=end_date, append=append, retry_skipped=retry_skipped)


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
