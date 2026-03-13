#!/usr/bin/env python3
"""Train alpha model using Qlib + extended factors."""

import json
from pathlib import Path

import click
from loguru import logger


@click.command()
@click.option("--model-type", default=None, help="Model type: lgbm, xgboost, mlp, lstm, transformer")
@click.option("--config", default=None, help="Path to config.yaml override")
@click.option("--save-name", default="model", help="Name prefix for saved model")
@click.option("--factor-groups", default=None, help="Comma-separated factor groups to use")
def main(
    model_type: str | None,
    config: str | None,
    save_name: str,
    factor_groups: str | None,
) -> None:
    """Train an alpha model with extended factor set."""
    if config:
        from stopat30m.config import load_config
        load_config(config)

    from stopat30m.data.provider import init_qlib
    from stopat30m.factors.handler import AlphaExtendedHandler
    from stopat30m.model.trainer import train_and_evaluate

    init_qlib()

    groups = factor_groups.split(",") if factor_groups else None
    handler = AlphaExtendedHandler(groups=groups)
    logger.info(f"Factor count: {handler.num_features}")

    dataset = handler.build_dataset()
    results = train_and_evaluate(dataset, model_type=model_type, save_name=save_name)

    output_dir = Path("./output")
    output_dir.mkdir(parents=True, exist_ok=True)
    metrics_path = output_dir / "metrics.json"
    with open(metrics_path, "w") as f:
        json.dump(results["metrics"], f, indent=2, ensure_ascii=False)

    logger.info(f"Model saved to: {results['model_path']}")
    logger.info(f"Metrics saved to: {metrics_path}")
    logger.info(f"Metrics: {results['metrics']}")


if __name__ == "__main__":
    main()
