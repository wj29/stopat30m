"""Data download and management for Qlib.

Supported sources:
  - qlib:          Qlib public dataset (free, complete to ~2020-09)
  - baostock:      BaoStock (free, no rate limit, full/incremental)
  - efinance:      Efinance (free, east-money backend, stable API)
  - akshare:       AKShare (free, latest daily data, no auth)
  - tushare:       Tushare Pro (paid, needs token, higher quality)
  - qlib+baostock: Recommended — Qlib base + BaoStock incremental to today
"""

from __future__ import annotations

from pathlib import Path

from loguru import logger

from stopat30m.config import get


VALID_SOURCES = ("qlib", "akshare", "baostock", "efinance", "tushare", "qlib+baostock")


def get_data_dir() -> Path:
    uri = get("qlib", "provider_uri", "~/.qlib/qlib_data/cn_data")
    return Path(uri).expanduser()


def data_exists() -> bool:
    data_dir = get_data_dir()
    required = ["calendars", "instruments", "features"]
    return all((data_dir / d).exists() for d in required)


def download_cn_data(
    source: str = "baostock",
    target_dir: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    parallel: bool = True,
    workers: int | None = None,
) -> None:
    """Download A-share data.

    Default behavior is **incremental**: if local data already exists, only
    fetch new data since the last trusted date (per-stock).  When *parallel*
    is True (default), multiple data sources are used concurrently.

    Args:
        source: Primary data source. 'baostock' (default), 'qlib+baostock'
                (full rebuild), 'qlib' (base only), 'akshare', 'tushare'.
        target_dir: Override target directory.
        start_date: Start date (full download only, ignored for incremental).
        end_date: End date (default today).
        parallel: Use multiple sources in parallel (default True).
        workers: Number of parallel sub-processes per source (BaoStock only).
                 None → fetcher default (4 for BaoStock).
    """
    if source not in VALID_SOURCES:
        raise ValueError(f"Unknown source '{source}'. Choose from: {VALID_SOURCES}")

    target = target_dir or str(get_data_dir())
    target_path = Path(target).expanduser()
    target_path.mkdir(parents=True, exist_ok=True)

    if source == "qlib":
        _download_qlib_public(target_path)
    elif source == "qlib+baostock":
        _download_qlib_then_append(target_path, end_date, parallel=parallel, workers=workers)
    else:
        cal_path = target_path / "calendars" / "day.txt"
        if cal_path.exists():
            _append_from_fetcher(source, target_path, end_date, parallel=parallel, workers=workers)
        else:
            logger.info("No existing data found. Running full download...")
            _download_from_fetcher(source, target_path, start_date, end_date)


def _create_fetcher(source: str, workers: int | None = None):
    """Instantiate the appropriate DataFetcher for the given source."""
    from stopat30m.data.fetcher import AKShareFetcher, BaoStockFetcher, EfinanceFetcher, TushareFetcher

    if source == "akshare":
        return AKShareFetcher()
    elif source == "baostock":
        kwargs: dict = {}
        if workers is not None:
            kwargs["workers"] = workers
        return BaoStockFetcher(**kwargs)
    elif source == "efinance":
        return EfinanceFetcher()
    elif source == "tushare":
        token = get("tushare", "token", "")
        if not token:
            raise ValueError(
                "Tushare token not configured. "
                "Set tushare.token in config.yaml or register at https://tushare.pro"
            )
        return TushareFetcher(token=token, delay=1.3)
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


def _download_qlib_then_append(
    target_path: Path, end_date: str | None, parallel: bool = True,
    workers: int | None = None,
) -> None:
    """Recommended download: Qlib complete base + BaoStock incremental to today.

    Step 1: Download Qlib public snapshot (~2020-09, complete and verified).
    Step 2: Build data_meta.json (per-stock metadata + trusted watermark).
    Step 3: Append from BaoStock for dates after the Qlib snapshot to today.
    """
    from stopat30m.data.fetcher import (
        META_FILENAME, build_meta_from_scan, fetch_stock_listing_info,
    )

    cal_path = target_path / "calendars" / "day.txt"
    if cal_path.exists():
        cal_dates = [d.strip() for d in cal_path.read_text().strip().split("\n") if d.strip()]
        logger.info(f"Existing data found: {cal_dates[0]} ~ {cal_dates[-1]} ({len(cal_dates)} days)")
        logger.info("Skipping Qlib base download.")
    else:
        logger.info("Step 1/3: Downloading Qlib official base data...")
        _download_qlib_public(target_path)
        logger.info("Step 1/3: Qlib base download complete.")

    meta_path = target_path / META_FILENAME
    if not meta_path.exists():
        logger.info("Step 2/3: Building data_meta.json...")
        fetcher = _create_fetcher("baostock")
        stock_info = fetch_stock_listing_info(fetcher)
        meta = build_meta_from_scan(target_path, stock_info=stock_info)
        meta.save(meta_path)
        logger.info(
            f"Step 2/3: Meta created — {len(meta.stocks)} stocks, "
            f"trusted_until={meta.trusted_until}"
        )
    else:
        logger.info("Step 2/3: data_meta.json already exists, skipping.")

    logger.info("Step 3/3: Appending latest data from BaoStock...")
    _append_from_fetcher("baostock", target_path, end_date, parallel=parallel, workers=workers)
    logger.info("All done. Data is now complete and up to date.")


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


def _create_available_fetchers(primary_source: str, workers: int | None = None) -> list:
    """Create fetchers for parallel append: primary + any available secondary sources.

    Always includes the primary source. Adds secondary sources that are
    importable and configured (e.g. TuShare needs a token).

    Priority order for secondaries: BaoStock > Efinance > AKShare > Tushare.
    """
    from stopat30m.data.fetcher import AKShareFetcher, BaoStockFetcher, EfinanceFetcher

    primary = _create_fetcher(primary_source, workers=workers)
    fetchers = [primary]

    secondaries = [
        ("baostock", BaoStockFetcher),
        ("efinance", EfinanceFetcher),
        ("akshare", AKShareFetcher),
    ]

    for name, cls in secondaries:
        if name == primary_source:
            continue
        try:
            fetchers.append(cls())
            logger.info(f"Secondary source enabled: {name}")
        except ImportError:
            logger.debug(f"Secondary source {name} not available (not installed)")

    tushare_token = get("tushare", "token", "")
    if tushare_token and primary_source != "tushare":
        try:
            from stopat30m.data.fetcher import TushareFetcher
            fetchers.append(TushareFetcher(token=tushare_token, delay=1.3))
            logger.info("Secondary source enabled: tushare (rate limited: ~25 stocks/min)")
        except ImportError:
            logger.debug("Secondary source tushare not available (not installed)")

    return fetchers


def _append_from_fetcher(
    source: str,
    target_path: Path,
    end_date: str | None,
    parallel: bool = True,
    workers: int | None = None,
) -> None:
    """Append new data to an existing Qlib dataset (per-stock incremental).

    When *parallel* is True (default), automatically detects additional
    data sources and fetches stocks in parallel across multiple sources.
    """
    from stopat30m.data.fetcher import append_with_source

    if parallel:
        fetchers = _create_available_fetchers(source, workers=workers)
    else:
        fetchers = [_create_fetcher(source, workers=workers)]

    append_with_source(
        fetchers=fetchers,
        target_dir=target_path,
        end_date=end_date,
    )


def init_qlib() -> None:
    """Initialize Qlib with config settings."""
    import qlib

    provider_uri = str(get_data_dir())
    region = get("qlib", "region", "cn")

    qlib.init(provider_uri=provider_uri, region=region)
    logger.info(f"Qlib initialized: provider_uri={provider_uri}, region={region}")
