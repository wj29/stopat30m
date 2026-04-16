"""Download and incremental append orchestration for Qlib data."""

from __future__ import annotations

import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

from loguru import logger

from stopat30m.data.normalize import to_qlib_symbol

from .base import QLIB_FIELDS, DataFetcher, _FetchResult
from .meta import (
    META_FILENAME,
    DataMeta,
    StockMeta,
    build_meta_from_scan,
    fetch_stock_listing_info,
)
from .qlib_dumper import QlibDumper, append_binary, read_bin_file, write_fresh_binary


# ---------------------------------------------------------------------------
# Worker result
# ---------------------------------------------------------------------------


@dataclass
class WorkerResult:
    source_name: str
    success: int = 0
    empty: int = 0
    resumed: int = 0
    errors: int = 0
    error_codes: list[str] = field(default_factory=list)
    instruments: dict[str, tuple[str, str]] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Source colors for terminal logging
# ---------------------------------------------------------------------------

def _colored_src(src: str) -> str:
    from stopat30m.data.sources import get_source_color
    color = get_source_color(src.split("#")[0])
    return f"<{color}>[{src}]</{color}>"


def _log_stock(
    src: str, i: int, total: int, code: str, status: str,
    t0: float, result: WorkerResult,
) -> None:
    elapsed = time.time() - t0
    done = result.success + result.empty
    if done > 0 and elapsed > 0:
        rate = done / elapsed
        eta_m = (total - i) / rate / 60
        pace = f"{rate:.1f}/s ETA {eta_m:.0f}m"
    else:
        pace = ""
    pct = i * 100 // total
    tag = _colored_src(src)
    logger.opt(colors=True).info(f"{tag} {pct:>3d}% {i}/{total} {code} {status} {pace}")


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------


def _load_checkpoint(path: Path) -> tuple[str, str, set[str]]:
    lines = path.read_text().strip().split("\n")
    header_parts = lines[0].split("|")
    fetch_start, end_date = header_parts[0], header_parts[1]
    done_codes = {line.strip() for line in lines[1:] if line.strip()}
    return fetch_start, end_date, done_codes


def _write_checkpoint_header(path: Path, fetch_start: str, end_date: str) -> None:
    path.write_text(f"{fetch_start}|{end_date}\n")


def _checkpoint_stock(path: Path, code: str) -> None:
    with open(path, "a") as f:
        f.write(code + "\n")


# ---------------------------------------------------------------------------
# Subprocess worker for intra-source parallelism
# ---------------------------------------------------------------------------


def _subprocess_fetch_batch(
    source_name: str,
    work: list[tuple[str, str | None, str, bool]],
    cal_to_idx: dict[str, int],
    target_str: str,
    done_codes: list[str],
    fallback_start: str,
    worker_id: int,
) -> list[_FetchResult]:
    """Fetch a batch of stocks in a subprocess with its own data source connection."""
    import sys as _sys

    from loguru import logger as _log
    _log.remove()
    _log.add(
        _sys.stderr, level="INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<7}</level> | {message}",
        colorize=True,
    )

    from stopat30m.data.sources import create_fetcher

    override = {"workers": 1} if source_name == "baostock" else {}
    fetcher = create_fetcher(source_name, **override)

    tag = _colored_src(f"{source_name}#{worker_id}")
    target = Path(target_str)
    done_set = set(done_codes)
    results: list[_FetchResult] = []
    total = len(work)
    t0 = time.time()
    ok_count = 0
    err_count = 0
    consecutive_errors = 0
    _EARLY_ABORT = 20

    today_str = datetime.now().strftime("%Y-%m-%d")

    for i, (code, stock_start, stock_end, is_delisted) in enumerate(work, 1):
        if code in done_set:
            continue

        fetch_from = stock_start or fallback_start
        if fetch_from > stock_end:
            results.append(_FetchResult(code, "skip", "", "", 0, ""))
            continue

        # If the only missing day is today, empty is expected (data not
        # published yet) — don't let it trigger early abort.
        today_only = (fetch_from >= today_str)
        pct = i * 100 // total

        try:
            df = fetcher.fetch_daily(code, fetch_from, stock_end)
        except Exception as e:
            err_count += 1
            if not today_only:
                consecutive_errors += 1
            results.append(_FetchResult(code, "error", "", "", 0, str(e)))
            _log.opt(colors=True).warning(f"{tag} {pct:>3d}% {i}/{total} {code} error: {e}")
            if consecutive_errors >= _EARLY_ABORT:
                _log.warning(f"{source_name}#{worker_id}: {_EARLY_ABORT} consecutive errors, aborting remaining")
                for _, (c, _, _, _) in enumerate(work[i:]):
                    if c not in done_set:
                        results.append(_FetchResult(c, "error", "", "", 0, "source down (aborted)"))
                break
            continue

        if df is None or df.empty:
            if is_delisted:
                consecutive_errors = 0
                results.append(_FetchResult(code, "empty", "", stock_end, 0, ""))
                continue
            if today_only:
                results.append(_FetchResult(code, "skip", "", "", 0, "today not ready"))
                continue
            err_count += 1
            consecutive_errors += 1
            results.append(_FetchResult(code, "error", "", "", 0, "empty data for active stock"))
            if consecutive_errors >= _EARLY_ABORT:
                _log.warning(f"{source_name}#{worker_id}: {_EARLY_ABORT} consecutive errors, aborting remaining")
                for _, (c, _, _, _) in enumerate(work[i:]):
                    if c not in done_set:
                        results.append(_FetchResult(c, "error", "", "", 0, "source down (aborted)"))
                break
            continue

        consecutive_errors = 0
        qlib_sym = to_qlib_symbol(code)
        feat_dir = target / "features" / qlib_sym.lower()
        if feat_dir.exists():
            append_binary(feat_dir, df, cal_to_idx)
        else:
            feat_dir.mkdir(parents=True, exist_ok=True)
            write_fresh_binary(feat_dir, df, cal_to_idx)

        dates = sorted(df["date"].tolist())
        results.append(_FetchResult(code, "ok", dates[0], dates[-1], len(df), ""))
        ok_count += 1

    elapsed = time.time() - t0
    _log.opt(colors=True).info(
        f"{tag} batch done: {ok_count} ok, {err_count} errors in {elapsed / 60:.1f}min"
    )
    return results


# ---------------------------------------------------------------------------
# Subprocess-parallel coordinator
# ---------------------------------------------------------------------------


def _worker_fetch_with_subprocesses(
    fetcher: DataFetcher,
    work: list[tuple[str, str | None, str]],
    meta: DataMeta,
    cal_to_idx: dict[str, int],
    target: Path,
    checkpoint_path: Path,
    meta_lock: threading.Lock,
    meta_path: Path,
    done_codes: set[str],
    fallback_start: str = "2005-01-01",
    save_interval: int = 50,
) -> WorkerResult:
    src = fetcher.name
    concurrency = min(fetcher.concurrency, len(work))
    result = WorkerResult(source_name=src)

    if concurrency <= 1:
        return _worker_fetch(
            fetcher, work, meta, cal_to_idx, target,
            checkpoint_path, meta_lock, meta_path,
            done_codes, fallback_start, save_interval,
        )

    work_with_flags: list[tuple[str, str | None, str, bool]] = []
    for code, start, end in work:
        sm = meta.stocks.get(code)
        is_delisted = sm is not None and sm.status == "delisted"
        work_with_flags.append((code, start, end, is_delisted))

    chunks: list[list[tuple[str, str | None, str, bool]]] = [[] for _ in range(concurrency)]
    for idx, item in enumerate(work_with_flags):
        chunks[idx % concurrency].append(item)

    ctag = _colored_src(src)
    logger.opt(colors=True).info(
        f"{ctag} Launching {len(chunks)} sub-processes ({len(work)} stocks total)"
    )

    done_list = sorted(done_codes)
    target_str = str(target)
    t0 = time.time()

    with ProcessPoolExecutor(max_workers=concurrency) as pool:
        futures = {
            pool.submit(
                _subprocess_fetch_batch,
                src, chunk, cal_to_idx, target_str,
                done_list, fallback_start, wid,
            ): wid
            for wid, chunk in enumerate(chunks)
        }

        for future in as_completed(futures):
            wid = futures[future]
            try:
                batch = future.result()
            except Exception as e:
                logger.opt(colors=True).error(f"{_colored_src(f'{src}#{wid}')} subprocess crashed: {e}")
                result.errors += 1
                result.error_codes.append(f"subprocess#{wid}")
                continue

            with meta_lock:
                for fr in batch:
                    if fr.status == "ok":
                        result.success += 1
                        _checkpoint_stock(checkpoint_path, fr.code)

                        qlib_sym = to_qlib_symbol(fr.code)
                        if qlib_sym in result.instruments:
                            old_s, old_e = result.instruments[qlib_sym]
                            result.instruments[qlib_sym] = (
                                min(old_s, fr.data_start),
                                max(old_e, fr.data_end),
                            )
                        else:
                            result.instruments[qlib_sym] = (fr.data_start, fr.data_end)

                        sm = meta.stocks.get(fr.code)
                        if sm:
                            sm.data_end = fr.data_end
                            if not sm.data_start:
                                sm.data_start = fr.data_start

                    elif fr.status == "empty":
                        result.empty += 1
                        _checkpoint_stock(checkpoint_path, fr.code)
                        sm = meta.stocks.get(fr.code)
                        if sm and fr.data_end:
                            sm.data_end = fr.data_end

                    elif fr.status == "skip":
                        _checkpoint_stock(checkpoint_path, fr.code)

                    elif fr.status == "error":
                        result.errors += 1
                        result.error_codes.append(fr.code)

                meta.last_append = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                meta.save(meta_path)

    return result


# ---------------------------------------------------------------------------
# Per-worker sequential fetch loop
# ---------------------------------------------------------------------------


def _worker_fetch(
    fetcher: DataFetcher,
    work: list[tuple[str, str | None, str]],
    meta: DataMeta,
    cal_to_idx: dict[str, int],
    target: Path,
    checkpoint_path: Path,
    meta_lock: threading.Lock,
    meta_path: Path,
    done_codes: set[str],
    fallback_start: str = "2005-01-01",
    save_interval: int = 50,
) -> WorkerResult:
    if fetcher.concurrency > 1 and len(work) > 1:
        return _worker_fetch_with_subprocesses(
            fetcher, work, meta, cal_to_idx, target,
            checkpoint_path, meta_lock, meta_path,
            done_codes, fallback_start, save_interval,
        )

    src = fetcher.name
    result = WorkerResult(source_name=src)
    total = len(work)
    t0 = time.time()
    consecutive_errors = 0
    _EARLY_ABORT_THRESHOLD = 20
    today_str = datetime.now().strftime("%Y-%m-%d")

    for i, (code, stock_start, stock_end) in enumerate(work, 1):
        if code in done_codes:
            result.resumed += 1
            continue

        fetch_from = stock_start or fallback_start
        if fetch_from > stock_end:
            with meta_lock:
                _checkpoint_stock(checkpoint_path, code)
            continue

        today_only = (fetch_from >= today_str)

        try:
            df = fetcher.fetch_daily(code, fetch_from, stock_end)
        except Exception as e:
            result.errors += 1
            result.error_codes.append(code)
            if not today_only:
                consecutive_errors += 1
            _log_stock(src, i, total, code, f"error: {e}", t0, result)
            if consecutive_errors >= _EARLY_ABORT_THRESHOLD:
                remaining_count = total - i
                logger.warning(
                    f"[{src}] {_EARLY_ABORT_THRESHOLD} consecutive errors — "
                    f"source appears down, aborting {remaining_count} remaining stocks "
                    f"(will retry via cross-source)"
                )
                for _, (c, _, _) in enumerate(work[i:], i + 1):
                    if c not in done_codes:
                        result.errors += 1
                        result.error_codes.append(c)
                break
            continue

        sm = meta.stocks.get(code)
        is_delisted = sm and sm.status == "delisted"

        if df is None or df.empty:
            if is_delisted:
                result.empty += 1
                consecutive_errors = 0
                if sm:
                    sm.data_end = stock_end
                with meta_lock:
                    _checkpoint_stock(checkpoint_path, code)
                _log_stock(src, i, total, code, "empty(delisted)", t0, result)
                continue

            if today_only:
                with meta_lock:
                    _checkpoint_stock(checkpoint_path, code)
                _log_stock(src, i, total, code, "skip(today not ready)", t0, result)
                continue

            result.errors += 1
            result.error_codes.append(code)
            consecutive_errors += 1
            _log_stock(src, i, total, code, "error(empty active)", t0, result)
            if consecutive_errors >= _EARLY_ABORT_THRESHOLD:
                remaining_count = total - i
                logger.warning(
                    f"[{src}] {_EARLY_ABORT_THRESHOLD} consecutive errors — "
                    f"source appears down, aborting {remaining_count} remaining stocks "
                    f"(will retry via cross-source)"
                )
                for _, (c, _, _) in enumerate(work[i:], i + 1):
                    if c not in done_codes:
                        result.errors += 1
                        result.error_codes.append(c)
                break
            continue

        consecutive_errors = 0
        qlib_sym = to_qlib_symbol(code)
        sym_lower = qlib_sym.lower()
        feat_dir = target / "features" / sym_lower

        if feat_dir.exists():
            append_binary(feat_dir, df, cal_to_idx)
        else:
            feat_dir.mkdir(parents=True, exist_ok=True)
            write_fresh_binary(feat_dir, df, cal_to_idx)

        dates_in_data = sorted(df["date"].tolist())
        if qlib_sym in result.instruments:
            old_s, old_e = result.instruments[qlib_sym]
            result.instruments[qlib_sym] = (
                min(old_s, dates_in_data[0]),
                max(old_e, dates_in_data[-1]),
            )
        else:
            result.instruments[qlib_sym] = (dates_in_data[0], dates_in_data[-1])

        if sm:
            sm.data_end = dates_in_data[-1]
            if not sm.data_start:
                sm.data_start = dates_in_data[0]

        result.success += 1
        with meta_lock:
            _checkpoint_stock(checkpoint_path, code)

        _log_stock(src, i, total, code, f"ok +{len(df)}d", t0, result)

        if result.success % save_interval == 0:
            with meta_lock:
                meta.last_append = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                meta.save(meta_path)

    return result


# ---------------------------------------------------------------------------
# Work partitioning across sources
# ---------------------------------------------------------------------------

def _partition_work(
    work: list[tuple[str, str | None, str]],
    fetchers: list[DataFetcher],
) -> list[list[tuple[str, str | None, str]]]:
    if len(fetchers) == 1:
        # Even with one source, filter out unsupported codes to error list
        f = fetchers[0]
        supported = [(c, s, e) for c, s, e in work if f.can_fetch(c)]
        skipped = len(work) - len(supported)
        if skipped:
            logger.info(f"[{f.name}] skipping {skipped} unsupported stocks (e.g. 688xxx)")
        return [supported]

    from stopat30m.data.sources import get_download_weight
    weights = [get_download_weight(f.name) or 3 for f in fetchers]

    # Separate work items that specific sources can't handle
    general: list[tuple[str, str | None, str]] = []
    overflow: dict[int, list[tuple[str, str | None, str]]] = {i: [] for i in range(len(fetchers))}

    for item in work:
        code = item[0]
        capable = [i for i, f in enumerate(fetchers) if f.can_fetch(code)]
        if len(capable) == len(fetchers):
            general.append(item)
        elif capable:
            # Route to the first capable source with highest weight
            best = max(capable, key=lambda i: weights[i])
            overflow[best].append(item)
        else:
            general.append(item)

    total_w = sum(weights)
    n = len(general)

    partitions: list[list[tuple[str, str | None, str]]] = []
    offset = 0
    for i, w in enumerate(weights):
        if i == len(weights) - 1:
            chunk = general[offset:]
        else:
            count = round(n * w / total_w)
            chunk = general[offset:offset + count]
            offset += count
        partitions.append(chunk + overflow[i])

    routed = sum(len(v) for v in overflow.values())
    if routed:
        logger.info(f"Routed {routed} source-restricted stocks to capable sources")

    return partitions


# ---------------------------------------------------------------------------
# Full download entry point
# ---------------------------------------------------------------------------


def download_with_source(
    fetcher: DataFetcher,
    target_dir: str | Path,
    start_date: str = "2005-01-01",
    end_date: str | None = None,
) -> None:
    target = Path(target_dir).expanduser()
    target.mkdir(parents=True, exist_ok=True)

    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    logger.info(f"Data source: {fetcher.name}")
    logger.info(f"Target: {target}")
    logger.info(f"Date range: {start_date} ~ {end_date}")

    logger.info("Fetching trading calendar...")
    calendar = fetcher.fetch_trade_calendar(start_date, end_date)
    if not calendar:
        raise RuntimeError("Empty trading calendar")
    logger.info(f"Calendar: {len(calendar)} days ({calendar[0]} ~ {calendar[-1]})")

    logger.info("Fetching stock list...")
    stock_codes = fetcher.fetch_stock_list()
    logger.info(f"Found {len(stock_codes)} stocks")

    logger.info("Fetching index components...")
    csi300 = fetcher.fetch_index_components("000300")
    csi500 = fetcher.fetch_index_components("000905")
    logger.info(f"CSI300: {len(csi300)} stocks, CSI500: {len(csi500)} stocks")

    dumper = QlibDumper(target)
    dumper.set_calendar(calendar)

    total = len(stock_codes)
    success = 0
    failed = 0
    t0 = time.time()

    for i, code in enumerate(stock_codes, 1):
        if i % 100 == 0 or i == total:
            elapsed = time.time() - t0
            rate = i / elapsed if elapsed > 0 else 0
            eta = (total - i) / rate if rate > 0 else 0
            logger.info(
                f"[{i}/{total}] {success} ok, {failed} fail | "
                f"{rate:.1f} stocks/s, ETA {eta / 60:.0f}min"
            )

        try:
            df = fetcher.fetch_daily(code, start_date, end_date)
        except Exception as e:
            logger.debug(f"Exception for {code}: {e}")
            failed += 1
            continue

        if df is None or df.empty:
            failed += 1
            continue

        qlib_sym = to_qlib_symbol(code)
        dumper.dump_stock(qlib_sym, df)
        success += 1

    elapsed_total = time.time() - t0
    logger.info(
        f"Download complete: {success}/{total} stocks in {elapsed_total / 60:.1f}min "
        f"({failed} failed)"
    )

    dumper.finalize(csi300_codes=csi300, csi500_codes=csi500)
    logger.info(f"Qlib data written to {target}")


# ---------------------------------------------------------------------------
# Incremental append entry point
# ---------------------------------------------------------------------------


def append_with_source(
    fetchers: list[DataFetcher] | DataFetcher,
    target_dir: str | Path,
    end_date: str | None = None,
) -> None:
    if isinstance(fetchers, DataFetcher):
        fetchers = [fetchers]

    primary = fetchers[0]
    target = Path(target_dir).expanduser()
    checkpoint_path = target / ".append_progress"
    meta_path = target / META_FILENAME

    cal_path = target / "calendars" / "day.txt"
    if not cal_path.exists():
        raise FileNotFoundError(
            f"No existing calendar at {cal_path}. "
            "Run a full download first: python main.py download --full"
        )

    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    existing_cal = [d.strip() for d in cal_path.read_text().strip().split("\n") if d.strip()]

    # Phase 1: Prepare
    last_date = existing_cal[-1]
    next_day = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    calendar_extended = False
    if next_day <= end_date:
        new_dates = primary.fetch_trade_calendar(next_day, end_date)
        if new_dates:
            logger.info(f"Extending calendar: {len(new_dates)} new days ({new_dates[0]} ~ {new_dates[-1]})")
            merged_cal = sorted(set(existing_cal) | set(new_dates))
            cal_dir = target / "calendars"
            cal_dir.mkdir(parents=True, exist_ok=True)
            (cal_dir / "day.txt").write_text("\n".join(merged_cal) + "\n")
            existing_cal = merged_cal
            calendar_extended = True

    if not calendar_extended:
        merged_cal = existing_cal

    cal_to_idx = {d: i for i, d in enumerate(merged_cal)}

    has_meta = meta_path.exists()
    if has_meta:
        meta = DataMeta.load(meta_path)
        logger.info(f"Meta loaded: {len(meta.stocks)} stocks, trusted_until={meta.trusted_until}")
    else:
        logger.warning("data_meta.json not found. Building from scan...")
        stock_info = fetch_stock_listing_info(primary)
        meta = build_meta_from_scan(target, stock_info=stock_info)
        meta.save(meta_path)
        logger.info(f"Meta created: {len(meta.stocks)} stocks, trusted_until={meta.trusted_until}")

    done_codes: set[str] = set()
    if checkpoint_path.exists():
        _, cp_end, done_codes = _load_checkpoint(checkpoint_path)

        reconciled = 0
        for code in done_codes:
            sm = meta.stocks.get(code)
            if not sm:
                continue
            goal = DataMeta._fetch_end(sm, cp_end)
            if not sm.data_end or sm.data_end < goal:
                sm.data_end = goal
                reconciled += 1
        if reconciled:
            meta.save(meta_path)
            logger.info(f"Reconciled {reconciled} checkpoint-done stocks into meta")

        if cp_end != end_date:
            logger.info(f"Checkpoint target date changed ({cp_end} -> {end_date}). Discarding stale checkpoint.")
            checkpoint_path.unlink()
            done_codes = set()
        else:
            logger.info(f"Resuming: {len(done_codes)} stocks already done")

    if meta.needs_listing_refresh():
        logger.info("Refreshing stock listing info (daily)...")
        stock_info = fetch_stock_listing_info(primary)
        for code, info in stock_info.items():
            sm = meta.stocks.get(code)
            if sm is None:
                meta.stocks[code] = StockMeta(
                    code=code,
                    ipo_date=info.get("ipo_date"),
                    delist_date=info.get("delist_date"),
                    status=info.get("status", "active"),
                )
            else:
                sm.delist_date = info.get("delist_date") or sm.delist_date
                sm.status = info.get("status", sm.status)
                sm.ipo_date = info.get("ipo_date") or sm.ipo_date
        meta.listing_updated = datetime.now().strftime("%Y-%m-%d")
        meta.save(meta_path)
        logger.info(f"Listing updated: {len(meta.stocks)} stocks")
    else:
        logger.info(f"Listing already refreshed today ({meta.listing_updated}), using cache")

    logger.info("Fetching index components...")
    csi300 = primary.fetch_index_components("000300")
    csi500 = primary.fetch_index_components("000905")
    logger.info(f"CSI300: {len(csi300)}, CSI500: {len(csi500)}")

    instruments: dict[str, tuple[str, str]] = {}
    inst_path = target / "instruments" / "all.txt"
    if inst_path.exists():
        for line in inst_path.read_text().strip().split("\n"):
            parts = line.strip().split("\t")
            if len(parts) >= 3:
                instruments[parts[0]] = (parts[1], parts[2])

    all_codes = sorted(meta.stocks.keys())
    work: list[tuple[str, str | None, str]] = []
    skip_delisted = 0
    skip_complete = 0

    for code in all_codes:
        needs, start, stock_end = meta.needs_fetch(code, end_date)
        if not needs:
            sm = meta.stocks[code]
            if sm.status == "delisted":
                skip_delisted += 1
            else:
                skip_complete += 1
        else:
            work.append((code, start, stock_end))

    total_work = len(work)
    if not calendar_extended and total_work == 0:
        logger.info(f"Data already up to date (trusted_until={meta.trusted_until}). Nothing to append.")
        return

    logger.info(
        f"Work plan: {total_work} stocks to fetch, "
        f"{skip_complete} already complete, {skip_delisted} delisted (skipped)"
    )

    if not checkpoint_path.exists():
        _write_checkpoint_header(checkpoint_path, meta.trusted_until or "0000-00-00", end_date)

    # Phase 1.5: Pre-flight health check
    # Use well-known liquid stocks for probing instead of work[0] which
    # might be delisted/suspended/newly-listed with no data yet.
    if len(fetchers) > 1 and work:
        _PROBE_CANDIDATES = ["000001", "600519", "600036", "000858", "601318"]
        work_codes = {c for c, _, _ in work}
        probe_code = next((c for c in _PROBE_CANDIDATES if c in work_codes), None)

        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        probe_start_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
        probe_end_date = yesterday

        if not probe_code:
            probe_code = work[0][0]

        logger.info(f"Pre-flight probe: {probe_code} ({probe_start_date} ~ {probe_end_date})")
        healthy_fetchers: list[DataFetcher] = []
        for f in fetchers:
            if not f.can_fetch(probe_code):
                # Don't disqualify based on a probe it can't support
                healthy_fetchers.append(f)
                logger.opt(colors=True).info(
                    f"  {_colored_src(f.name)} <blue>跳过探活</blue> ({probe_code} 不在支持范围)"
                )
                continue
            try:
                probe_df = f.fetch_daily(probe_code, probe_start_date, probe_end_date)
                if probe_df is not None and not probe_df.empty:
                    healthy_fetchers.append(f)
                    logger.opt(colors=True).info(
                        f"  {_colored_src(f.name)} <green>健康</green> (probe {probe_code}: {len(probe_df)} rows)"
                    )
                else:
                    logger.opt(colors=True).warning(
                        f"  {_colored_src(f.name)} <yellow>不可用</yellow> (probe {probe_code}: empty)"
                    )
            except Exception as e:
                logger.opt(colors=True).warning(
                    f"  {_colored_src(f.name)} <yellow>不可用</yellow> (probe {probe_code}: {type(e).__name__})"
                )
        if healthy_fetchers:
            if len(healthy_fetchers) < len(fetchers):
                dropped = [f.name for f in fetchers if f not in healthy_fetchers]
                logger.warning(f"Dropped unhealthy sources: {dropped}")
            fetchers = healthy_fetchers
        else:
            logger.error("All sources failed pre-flight check! Proceeding with all sources anyway.")

    # Phase 2: Parallel fetch
    # Track codes that no source can handle
    unsupported_codes = [c for c, _, _ in work if not any(f.can_fetch(c) for f in fetchers)]
    if unsupported_codes:
        logger.warning(
            f"{len(unsupported_codes)} stocks unsupported by all sources (e.g. 688xxx on baostock-only): "
            f"{', '.join(unsupported_codes[:10])}{'...' if len(unsupported_codes) > 10 else ''}"
        )

    partitions = _partition_work(work, fetchers)
    meta_lock = threading.Lock()
    t0 = time.time()

    source_names = [f.name for f in fetchers]
    for fname, partition in zip(source_names, partitions):
        logger.opt(colors=True).info(f"  {_colored_src(fname)} {len(partition)} stocks assigned")

    if len(fetchers) == 1:
        results = [_worker_fetch(
            fetcher=fetchers[0],
            work=partitions[0],
            meta=meta,
            cal_to_idx=cal_to_idx,
            target=target,
            checkpoint_path=checkpoint_path,
            meta_lock=meta_lock,
            meta_path=meta_path,
            done_codes=done_codes,
            fallback_start=next_day,
        )]
    else:
        results: list[WorkerResult] = []
        with ThreadPoolExecutor(max_workers=len(fetchers)) as pool:
            futures = {
                pool.submit(
                    _worker_fetch,
                    fetcher=f,
                    work=p,
                    meta=meta,
                    cal_to_idx=cal_to_idx,
                    target=target,
                    checkpoint_path=checkpoint_path,
                    meta_lock=meta_lock,
                    meta_path=meta_path,
                    done_codes=done_codes,
                    fallback_start=next_day,
                ): f.name
                for f, p in zip(fetchers, partitions)
            }
            for future in as_completed(futures):
                src_name = futures[future]
                try:
                    r = future.result()
                    results.append(r)
                    err_msg = f", {r.errors} errors" if r.errors else ""
                    logger.opt(colors=True).info(
                        f"{_colored_src(src_name)} finished: {r.success} ok, {r.empty} empty{err_msg}"
                    )
                except Exception as exc:
                    logger.opt(colors=True).error(f"{_colored_src(src_name)} worker crashed: {exc}")
                    results.append(WorkerResult(source_name=src_name, errors=1, error_codes=["CRASH"]))

    # Phase 2.5: Cross-source retry — route failed stocks to the best healthy source
    if len(fetchers) > 1:
        # Rank sources by success rate from Phase 2
        source_success: dict[str, int] = {}
        source_total: dict[str, int] = {}
        failed_by_source: dict[str, str] = {}
        for r in results:
            source_success[r.source_name] = r.success
            source_total[r.source_name] = r.success + r.errors + r.empty
            for code in r.error_codes:
                if code.startswith("subprocess#") or code == "CRASH":
                    continue
                failed_by_source[code] = r.source_name

        if failed_by_source:
            retry_work: list[tuple[str, str | None, str]] = []
            for code in failed_by_source:
                needs, start, stock_end = meta.needs_fetch(code, end_date)
                if needs:
                    retry_work.append((code, start, stock_end))

            if retry_work:
                source_map = {f.name: f for f in fetchers}
                # Sort sources by success rate descending — prefer the most reliable
                ranked_sources = sorted(
                    fetchers,
                    key=lambda f: source_success.get(f.name, 0) / max(source_total.get(f.name, 1), 1),
                    reverse=True,
                )
                best_source = ranked_sources[0]
                logger.info(
                    f"Cross-source retry: {len(retry_work)} stocks failed. "
                    f"Best source: {best_source.name} "
                    f"({source_success.get(best_source.name, 0)}/{source_total.get(best_source.name, 1)} ok)"
                )

                # Route ALL failures to the best source (not back to a broken one)
                logger.opt(colors=True).info(
                    f"  {_colored_src(best_source.name)} retrying {len(retry_work)} stocks"
                )
                rr = _worker_fetch(
                    fetcher=best_source,
                    work=retry_work,
                    meta=meta,
                    cal_to_idx=cal_to_idx,
                    target=target,
                    checkpoint_path=checkpoint_path,
                    meta_lock=meta_lock,
                    meta_path=meta_path,
                    done_codes=set(),
                    fallback_start=next_day,
                )
                results.append(rr)
                logger.opt(colors=True).info(
                    f"  {_colored_src(best_source.name)} retry done: "
                    f"{rr.success} ok, {rr.empty} empty, {rr.errors} still failed"
                )

    # Phase 3: Finalize
    for r in results:
        for sym, (s, e) in r.instruments.items():
            if sym in instruments:
                old_s, old_e = instruments[sym]
                instruments[sym] = (min(old_s, s), max(old_e, e))
            else:
                instruments[sym] = (s, e)

    inst_dir = target / "instruments"
    inst_dir.mkdir(parents=True, exist_ok=True)

    lines = [f"{sym}\t{s}\t{e}" for sym, (s, e) in sorted(instruments.items())]
    (inst_dir / "all.txt").write_text("\n".join(lines) + "\n")

    for filename, raw_codes in [("csi300.txt", csi300), ("csi500.txt", csi500)]:
        if not raw_codes:
            continue
        qlib_codes = {to_qlib_symbol(c) for c in raw_codes}
        subset = [f"{sym}\t{s}\t{e}" for sym, (s, e) in sorted(instruments.items()) if sym in qlib_codes]
        if subset:
            (inst_dir / filename).write_text("\n".join(subset) + "\n")

    # Safety net: rebuild instruments and sync meta from binary files to
    # catch any desync caused by subprocesses writing features without
    # updating instruments/meta.
    from .qlib_dumper import rebuild_instruments_from_binary, sync_meta_from_binary
    rebuild_instruments_from_binary(target)
    sync_meta_from_binary(target)

    elapsed_total = time.time() - t0
    total_success = sum(r.success for r in results)
    total_empty = sum(r.empty for r in results)
    total_resumed = sum(r.resumed for r in results)
    total_errors = sum(r.errors for r in results)
    all_error_codes = [c for r in results for c in r.error_codes]

    checkpoint_path.unlink(missing_ok=True)

    if total_errors:
        logger.warning(
            f"{total_errors} stocks failed (will retry next run): "
            f"{', '.join(all_error_codes[:20])}"
            f"{'...' if len(all_error_codes) > 20 else ''}"
        )

    meta.last_append = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    meta.save(meta_path)

    logger.info(
        f"Append done: "
        f"{total_success} updated, {total_empty} empty, "
        f"{total_errors} errors, {total_resumed} resumed "
        f"({len(fetchers)} source{'s' if len(fetchers) > 1 else ''}) "
        f"in {elapsed_total / 60:.1f}min"
    )
    logger.info(
        f"Calendar: {len(merged_cal)} days ({merged_cal[0]} ~ {merged_cal[-1]}), "
        f"trusted_until: {meta.trusted_until or '(none)'}"
    )
