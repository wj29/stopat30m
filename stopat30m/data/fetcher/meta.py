"""Per-stock metadata and rolling trusted watermark for incremental data updates."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
from loguru import logger

from stopat30m.data.normalize import is_index_symbol

from .qlib_dumper import read_bin_file

META_FILENAME = "data_meta.json"


@dataclass
class StockMeta:
    """Per-stock metadata tracking data coverage and listing status."""
    code: str
    ipo_date: str | None = None
    delist_date: str | None = None
    data_start: str | None = None
    data_end: str | None = None
    status: str = "active"


@dataclass
class DataMeta:
    """Persistent metadata for the local Qlib dataset."""

    qlib_base_end: str = ""
    last_append: str = ""
    listing_updated: str = ""
    stocks: dict[str, StockMeta] = field(default_factory=dict)

    _DELIST_TOLERANCE_DAYS = 5
    _OUTLIER_LAG_DAYS = 180

    @property
    def trusted_until(self) -> str:
        """Global watermark = min(data_end) across tradable active stocks,
        excluding delisted, missing-ipo, and outlier-lagging stocks."""
        constraining: list[str] = []
        for sm in self.stocks.values():
            if not sm.data_end:
                continue
            if sm.status in ("delisted", "index"):
                continue
            if not sm.ipo_date:
                continue
            constraining.append(sm.data_end)
        if not constraining:
            return ""

        max_date = max(constraining)
        cutoff = (
            datetime.strptime(max_date, "%Y-%m-%d")
            - timedelta(days=self._OUTLIER_LAG_DAYS)
        ).strftime("%Y-%m-%d")
        filtered = [d for d in constraining if d >= cutoff]
        return min(filtered) if filtered else min(constraining)

    def watermark_outliers(self) -> list[StockMeta]:
        dates: list[str] = []
        for sm in self.stocks.values():
            if not sm.data_end or sm.status in ("delisted", "index") or not sm.ipo_date:
                continue
            dates.append(sm.data_end)
        if not dates:
            return []
        max_date = max(dates)
        cutoff = (
            datetime.strptime(max_date, "%Y-%m-%d")
            - timedelta(days=self._OUTLIER_LAG_DAYS)
        ).strftime("%Y-%m-%d")
        return [
            sm for sm in self.stocks.values()
            if sm.data_end
            and sm.status not in ("delisted", "index")
            and sm.ipo_date
            and sm.data_end < cutoff
        ]

    @classmethod
    def _delisted_complete(cls, sm: StockMeta) -> bool:
        if not sm.delist_date or not sm.data_end:
            return False
        delist = datetime.strptime(sm.delist_date, "%Y-%m-%d")
        cutoff = (delist - timedelta(days=cls._DELIST_TOLERANCE_DAYS)).strftime("%Y-%m-%d")
        return sm.data_end >= cutoff

    def save(self, path: Path) -> None:
        payload = {
            "qlib_base_end": self.qlib_base_end,
            "trusted_until": self.trusted_until,
            "last_append": self.last_append,
            "listing_updated": self.listing_updated,
            "stocks": {code: asdict(sm) for code, sm in self.stocks.items()},
        }
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=1))
        tmp.replace(path)

    @classmethod
    def load(cls, path: Path) -> DataMeta:
        raw = json.loads(path.read_text())
        stocks = {
            code: StockMeta(**vals)
            for code, vals in raw.get("stocks", {}).items()
        }
        meta = cls(
            qlib_base_end=raw.get("qlib_base_end", ""),
            last_append=raw.get("last_append", ""),
            listing_updated=raw.get("listing_updated", ""),
            stocks=stocks,
        )
        purged = meta._purge_index_codes()
        if purged:
            logger.info(f"Purged {purged} index codes from meta on load")
            meta.save(path)
        return meta

    def _purge_index_codes(self) -> int:
        count = 0
        for code, sm in self.stocks.items():
            if sm.status == "index":
                continue
            qlib_sh = f"sh{code}"
            qlib_sz = f"sz{code}"
            if is_index_symbol(qlib_sh) and not sm.ipo_date:
                sm.status = "index"
                count += 1
            elif is_index_symbol(qlib_sz) and not sm.ipo_date:
                sm.status = "index"
                count += 1
        return count

    @staticmethod
    def _fetch_end(sm: StockMeta, target_date: str) -> str:
        if sm.status == "delisted" and sm.delist_date:
            return sm.delist_date
        return target_date

    def needs_fetch(self, code: str, target_date: str) -> tuple[bool, str | None, str]:
        sm = self.stocks.get(code)
        if sm is None:
            return (True, None, target_date)

        if sm.status == "index":
            return (False, None, target_date)

        fetch_target = self._fetch_end(sm, target_date)

        if sm.data_end and sm.data_end >= fetch_target:
            return (False, None, fetch_target)

        if sm.status == "delisted" and self._delisted_complete(sm):
            return (False, None, fetch_target)

        if sm.data_end:
            start = _next_day_str(sm.data_end)
        elif sm.ipo_date:
            start = sm.ipo_date
        else:
            start = None
        return (True, start, fetch_target)

    def needs_listing_refresh(self) -> bool:
        today = datetime.now().strftime("%Y-%m-%d")
        return self.listing_updated != today


def _next_day_str(date_str: str) -> str:
    d = datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)
    return d.strftime("%Y-%m-%d")


def build_meta_from_scan(
    data_dir: Path,
    stock_info: dict[str, dict] | None = None,
) -> DataMeta:
    """Build DataMeta by scanning existing binary files + optional listing info."""
    cal_path = data_dir / "calendars" / "day.txt"
    if not cal_path.exists():
        raise FileNotFoundError(f"No calendar at {cal_path}")

    cal_dates = [d.strip() for d in cal_path.read_text().strip().split("\n") if d.strip()]
    cal_len = len(cal_dates)
    last_cal_date = cal_dates[-1]

    feat_dir = data_dir / "features"
    stocks: dict[str, StockMeta] = {}

    if feat_dir.exists():
        for sym_dir in sorted(feat_dir.iterdir()):
            if not sym_dir.is_dir():
                continue
            qlib_sym = sym_dir.name
            if is_index_symbol(qlib_sym):
                continue
            bare = qlib_sym[2:] if len(qlib_sym) > 2 else qlib_sym

            close_bin = sym_dir / "close.day.bin"
            data_start_date = None
            data_end_date = None

            if close_bin.exists() and close_bin.stat().st_size >= 8:
                try:
                    start_idx, data = read_bin_file(close_bin)
                    end_idx = start_idx + len(data) - 1
                    if 0 <= start_idx < cal_len:
                        data_start_date = cal_dates[start_idx]
                    if 0 <= end_idx < cal_len:
                        data_end_date = cal_dates[end_idx]
                except Exception:
                    pass

            info = (stock_info or {}).get(bare, {})
            stocks[bare] = StockMeta(
                code=bare,
                ipo_date=info.get("ipo_date"),
                delist_date=info.get("delist_date"),
                data_start=data_start_date,
                data_end=data_end_date,
                status=info.get("status", "active"),
            )

    if stock_info:
        local_missing = 0
        for bare, info in stock_info.items():
            if bare not in stocks:
                stocks[bare] = StockMeta(
                    code=bare,
                    ipo_date=info.get("ipo_date"),
                    delist_date=info.get("delist_date"),
                    data_start=None,
                    data_end=None,
                    status=info.get("status", "active"),
                )
                if info.get("status") != "delisted":
                    local_missing += 1
        if local_missing:
            logger.warning(f"{local_missing} active stocks have NO local data")

    today = datetime.now().strftime("%Y-%m-%d")
    meta = DataMeta(
        qlib_base_end=last_cal_date,
        last_append=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        listing_updated=today if stock_info else "",
        stocks=stocks,
    )

    no_data_active = sum(1 for sm in stocks.values() if not sm.data_end and sm.status != "delisted")
    logger.info(
        f"Built meta: {len(stocks)} stocks, "
        f"trusted_until={meta.trusted_until or '(none)'}"
        f"{f', {no_data_active} active stocks without local data' if no_data_active else ''}"
    )
    return meta


def fetch_stock_listing_info(fetcher) -> dict[str, dict]:
    """Fetch IPO/delist dates from a DataFetcher's stock list.

    If the fetcher has ``fetch_stock_list_with_info`` (e.g. BaoStock), use it
    directly for accurate IPO/delist/status data.  Otherwise, try BaoStock as a
    secondary source for listing metadata, falling back to bare code list only
    as a last resort.
    """
    if hasattr(fetcher, "fetch_stock_list_with_info"):
        try:
            return fetcher.fetch_stock_list_with_info()
        except Exception as e:
            logger.warning(f"fetch_stock_list_with_info failed: {e}")

    # Try BaoStock for accurate listing info even when using another primary source
    try:
        from .baostock import BaoStockFetcher
        bs = BaoStockFetcher(workers=1)
        info = bs.fetch_stock_list_with_info()
        if info:
            logger.info(f"Used BaoStock for listing info ({len(info)} stocks)")
            return info
    except Exception as e:
        logger.debug(f"BaoStock listing info fallback failed: {e}")

    # Last resort: code list only (no IPO/delist — will cause wider fetch windows)
    info: dict[str, dict] = {}
    try:
        codes = fetcher.fetch_stock_list()
        for code in codes:
            info[code] = {"ipo_date": None, "delist_date": None, "status": "active"}
        logger.warning(
            f"Using bare code list without IPO/delist info ({len(info)} stocks). "
            "This may cause unnecessary fetch attempts for delisted/suspended stocks."
        )
    except Exception as e:
        logger.warning(f"Failed to fetch stock listing info: {e}")
    return info
