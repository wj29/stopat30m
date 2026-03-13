#!/usr/bin/env python3
"""Start the trading engine."""

from pathlib import Path

import click
from loguru import logger


@click.command()
@click.option("--signal-source", default=None, help="Path to signal CSV file or directory")
@click.option("--redis", is_flag=True, help="Subscribe to Redis for signals (default if no signal-source)")
@click.option("--poll-interval", default=60, help="Signal poll interval in seconds (file mode)")
@click.option("--config", default=None, help="Path to config.yaml override")
def main(
    signal_source: str | None,
    redis: bool,
    poll_interval: int,
    config: str | None,
) -> None:
    """Start the alpha trading engine."""
    if config:
        from stopat30m.config import load_config
        load_config(config)

    from stopat30m.trading.engine import TradingEngine
    from stopat30m.trading.strategy import AlphaPortfolioStrategy

    strategy = AlphaPortfolioStrategy()
    engine = TradingEngine(strategy=strategy)

    engine.init()

    source = Path(signal_source) if signal_source else None
    if source is None and not redis:
        signal_dir = Path("./output/signals")
        if signal_dir.exists() and list(signal_dir.glob("signal_*.csv")):
            source = signal_dir
            logger.info(f"Using signal directory: {source}")
        else:
            logger.info("No signal source specified, using Redis mode")

    try:
        engine.start(signal_source=source, poll_interval=poll_interval)
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    finally:
        engine.stop()
        summary = strategy.get_portfolio_summary()
        logger.info(f"Final portfolio: {summary}")


if __name__ == "__main__":
    main()
