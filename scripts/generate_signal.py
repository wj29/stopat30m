#!/usr/bin/env python3
"""Generate trading signals from a trained model."""

import pickle
from pathlib import Path

import click
from loguru import logger


@click.command()
@click.option("--model-path", required=True, help="Path to trained model .pkl file")
@click.option("--publish", is_flag=True, help="Also publish signals to Redis")
@click.option("--date", default=None, help="Generate signals for specific date (YYYY-MM-DD)")
@click.option("--config", default=None, help="Path to config.yaml override")
def main(model_path: str, publish: bool, date: str | None, config: str | None) -> None:
    """Generate trading signals from trained model predictions."""
    if config:
        from stopat30m.config import load_config
        load_config(config)

    from stopat30m.data.provider import init_qlib
    from stopat30m.factors.handler import AlphaExtendedHandler
    from stopat30m.signal.generator import SignalGenerator

    init_qlib()

    model_file = Path(model_path)
    if not model_file.exists():
        logger.error(f"Model file not found: {model_file}")
        return

    with open(model_file, "rb") as f:
        model = pickle.load(f)

    handler = AlphaExtendedHandler()
    dataset = handler.build_dataset()

    pred = model.predict(dataset)
    logger.info(f"Predictions generated: {pred.shape}")

    gen = SignalGenerator()
    signals = gen.generate(pred, date=date)
    path = gen.save_signals(signals, tag=date or "latest")

    if publish:
        try:
            gen.publish_to_redis(signals)
        except Exception as e:
            logger.error(f"Failed to publish to Redis: {e}")

    logger.info(f"Signals saved to: {path}")
    logger.info(f"\n{signals.to_string()}")


if __name__ == "__main__":
    main()
