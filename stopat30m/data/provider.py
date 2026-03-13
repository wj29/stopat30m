"""Data download and management for Qlib.

Supported sources:
  - qlib:    Qlib public dataset (free, data to ~2020-09)
  - akshare: AKShare (free, latest daily data, no auth)
  - tushare: Tushare Pro (paid, needs token, higher quality)
"""

from __future__ import annotations

from pathlib import Path

from loguru import logger

from stopat30m.config import get


VALID_SOURCES = ("qlib", "akshare", "baostock", "tushare")


def get_data_dir() -> Path:
    uri = get("qlib", "provider_uri", "~/.qlib/qlib_data/cn_data")
    return Path(uri).expanduser()


def data_exists() -> bool:
    data_dir = get_data_dir()
    required = ["calendars", "instruments", "features"]
    return all((data_dir / d).exists() for d in required)


def download_cn_data(
    source: str = "qlib",
    target_dir: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    append: bool = False,
    retry_skipped: bool = False,
) -> None:
    """Download A-share data.

    Args:
        source: Data source — 'qlib' (public), 'akshare' (free), 'tushare' (paid)
        target_dir: Override target directory
        start_date: Start date (akshare/tushare full-download only)
        end_date: End date (akshare/tushare only, default today)
        append: If True, read existing calendar and only fetch new data since
                the last available date.  Requires an existing dataset on disk.
        retry_skipped: If True, retry stocks that were skipped in a previous
                       append (e.g. due to rate limiting).
    """
    if source not in VALID_SOURCES:
        raise ValueError(f"Unknown source '{source}'. Choose from: {VALID_SOURCES}")

    if (append or retry_skipped) and source == "qlib":
        raise ValueError("--append/--retry-skipped is not supported with --source qlib (static snapshot).")

    target = target_dir or str(get_data_dir())
    target_path = Path(target).expanduser()
    target_path.mkdir(parents=True, exist_ok=True)

    if source == "qlib":
        _download_qlib_public(target_path)
    elif append or retry_skipped:
        _append_from_fetcher(source, target_path, end_date, retry_skipped=retry_skipped)
    else:
        _download_from_fetcher(source, target_path, start_date, end_date)


def _create_fetcher(source: str):
    """Instantiate the appropriate DataFetcher for the given source."""
    from stopat30m.data.fetcher import AKShareFetcher, BaoStockFetcher, TushareFetcher

    if source == "akshare":
        return AKShareFetcher()
    elif source == "baostock":
        return BaoStockFetcher()
    elif source == "tushare":
        token = get("tushare", "token", "")
        if not token:
            raise ValueError(
                "Tushare token not configured. "
                "Set tushare.token in config.yaml or register at https://tushare.pro"
            )
        return TushareFetcher(token=token)
    else:
        raise ValueError(f"Unknown fetcher source: {source}")


def _download_qlib_public(target_path: Path) -> None:
    """Download from Qlib's public dataset (data to ~2020-09)."""
    from qlib.cli.data import GetData

    logger.info(f"Downloading Qlib public CN data to {target_path} ...")
    try:
        GetData().qlib_data(
            target_dir=str(target_path),
            region="cn",
            exists_skip=False,
        )
        logger.info("Qlib public data download complete.")
    except Exception as exc:
        logger.error(f"Failed to download Qlib public data: {exc}")
        raise


def _download_from_fetcher(
    source: str,
    target_path: Path,
    start_date: str | None,
    end_date: str | None,
) -> None:
    """Download from AKShare or Tushare, convert to Qlib format."""
    from stopat30m.data.fetcher import download_with_source

    data_cfg = get("data") or {}
    start = start_date or data_cfg.get("download_start", "2005-01-01")
    end = end_date  # None → fetcher defaults to today

    fetcher = _create_fetcher(source)
    download_with_source(
        fetcher=fetcher,
        target_dir=target_path,
        start_date=start,
        end_date=end,
    )


def _append_from_fetcher(
    source: str,
    target_path: Path,
    end_date: str | None,
    retry_skipped: bool = False,
) -> None:
    """Append new data to an existing Qlib dataset."""
    from stopat30m.data.fetcher import append_with_source

    fetcher = _create_fetcher(source)

    append_with_source(
        fetcher=fetcher,
        target_dir=target_path,
        end_date=end_date,
        retry_skipped=retry_skipped,
    )


def init_qlib() -> None:
    """Initialize Qlib with config settings."""
    import qlib

    provider_uri = str(get_data_dir())
    region = get("qlib", "region", "cn")

    qlib.init(provider_uri=provider_uri, region=region)
    logger.info(f"Qlib initialized: provider_uri={provider_uri}, region={region}")
