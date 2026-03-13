#!/usr/bin/env python3
"""Download A-share data for Qlib."""

import click
from loguru import logger


@click.command()
@click.option("--target-dir", default=None, help="Target directory for data download")
def main(target_dir: str | None) -> None:
    """Download CN stock data using Qlib's data provider."""
    from stopat30m.data.provider import download_cn_data, data_exists

    if data_exists() and target_dir is None:
        logger.info("Data already exists. Use --target-dir to re-download to a new location.")
        return

    download_cn_data(target_dir)


if __name__ == "__main__":
    main()
