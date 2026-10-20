"""Data download and management for Qlib.

Supported sources:
  - qlib:          Legacy loader; use research ashare-base for a pinned archive
  - baostock:      BaoStock (public daily data)
  - efinance:      Efinance (free, east-money backend, stable API)
  - akshare:       AKShare (free, latest daily data, no auth)
  - tushare:       Tushare Pro (paid, needs token, higher quality)
  - qlib+baostock: Legacy direct concatenation disabled; use audited ashare-update
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
    """Reject the former unscoped whole-market download before collection/writes.

    Keep the public signature for existing CLI/web callers. New research
    collection uses audited dated membership targets, not fetch_stock_list.
    """
    if source not in VALID_SOURCES:
        raise ValueError(f"Unknown source '{source}'. Choose from: {VALID_SOURCES}")

    if source in {"qlib", "qlib+baostock"}:
        raise ValueError(
            "旧 Qlib 下载/直接拼接入口已停用。请使用 python main.py research ashare-base "
            "取得锁定且核验的历史包，再用 research ashare-collect / ashare-update 做逐股票接缝验证。"
            "官方旧包目前暂停提供，不能假设其完整截止日期，也不能直接拼接另一来源的复权价。"
        )

    target = target_dir or str(get_data_dir())
    target_path = Path(target).expanduser()
    if (target_path.parent / "manifest.json").is_file() or (target_path / "incremental_manifest.json").is_file():
        raise ValueError("研究数据版本不可原地更新；请用 research ashare-update 发布新版本。")
    raise ValueError(
        "研究范围已限定为沪深300和中证1000，旧全市场下载/更新入口已停用。"
        "请使用通过日期和来源校验的股票池目标进行研究采集；"
        "现有沪深300每日模拟盘仍按独立任务更新。详见 docs/ASHARE_SCOPE.md。"
    )


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
    """Retired compatibility name; no unpinned download or extraction is allowed."""
    raise ValueError("Use research ashare-base with a pinned source specification and archive checksum")


def _download_qlib_then_append(
    target_path: Path, end_date: str | None, parallel: bool = True,
    workers: int | None = None,
) -> None:
    """Retired compatibility name; use audited immutable per-stock versions."""
    raise ValueError("Use research ashare-base / ashare-collect / ashare-update; direct source concatenation is disabled")


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
