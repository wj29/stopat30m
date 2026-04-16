"""Qlib binary format writer and binary file helpers."""

from __future__ import annotations

import struct
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger

from stopat30m.data.normalize import to_qlib_symbol

from .base import QLIB_FIELDS


class QlibDumper:
    """Convert stock DataFrames to Qlib's binary directory structure.

    Target layout::

        <qlib_dir>/
            calendars/day.txt
            instruments/all.txt
            instruments/csi300.txt
            instruments/csi500.txt
            features/<symbol>/
                open.day.bin
                close.day.bin
                ...
    """

    def __init__(self, qlib_dir: str | Path):
        self.qlib_dir = Path(qlib_dir)
        self._calendar: list[str] = []
        self._date_to_idx: dict[str, int] = {}
        self._instruments: dict[str, tuple[str, str]] = {}

    def set_calendar(self, dates: list[str]) -> None:
        self._calendar = sorted(dates)
        self._date_to_idx = {d: i for i, d in enumerate(self._calendar)}

    def dump_stock(self, qlib_symbol: str, df: pd.DataFrame) -> None:
        if df.empty:
            return

        valid_indices = []
        for _, row in df.iterrows():
            idx = self._date_to_idx.get(row["date"])
            if idx is not None:
                valid_indices.append(idx)

        if not valid_indices:
            return

        start_idx = min(valid_indices)
        end_idx = max(valid_indices)
        span = end_idx - start_idx + 1

        sym_lower = qlib_symbol.lower()
        feat_dir = self.qlib_dir / "features" / sym_lower
        feat_dir.mkdir(parents=True, exist_ok=True)

        for field in QLIB_FIELDS:
            data = np.full(span, np.nan, dtype=np.float32)
            for _, row in df.iterrows():
                idx = self._date_to_idx.get(row["date"])
                if idx is not None:
                    data[idx - start_idx] = float(row[field])

            header = np.array([start_idx], dtype=np.float32)
            bin_path = feat_dir / f"{field}.day.bin"
            with open(bin_path, "wb") as f:
                f.write(header.tobytes())
                f.write(data.tobytes())

        dates = df["date"].tolist()
        self._instruments[qlib_symbol] = (min(dates), max(dates))

    def write_calendar(self) -> None:
        cal_dir = self.qlib_dir / "calendars"
        cal_dir.mkdir(parents=True, exist_ok=True)
        (cal_dir / "day.txt").write_text("\n".join(self._calendar) + "\n")
        logger.info(
            f"Calendar: {len(self._calendar)} trading days "
            f"({self._calendar[0]} ~ {self._calendar[-1]})"
        )

    def write_instruments(
        self,
        csi300_codes: list[str] | None = None,
        csi500_codes: list[str] | None = None,
    ) -> None:
        inst_dir = self.qlib_dir / "instruments"
        inst_dir.mkdir(parents=True, exist_ok=True)

        lines = []
        for sym in sorted(self._instruments):
            s, e = self._instruments[sym]
            lines.append(f"{sym}\t{s}\t{e}")
        (inst_dir / "all.txt").write_text("\n".join(lines) + "\n")
        logger.info(f"Instruments: {len(lines)} stocks written to all.txt")

        for filename, raw_codes in [("csi300.txt", csi300_codes), ("csi500.txt", csi500_codes)]:
            if not raw_codes:
                continue
            qlib_codes = {to_qlib_symbol(c) for c in raw_codes}
            subset = []
            for sym in sorted(self._instruments):
                if sym in qlib_codes:
                    s, e = self._instruments[sym]
                    subset.append(f"{sym}\t{s}\t{e}")
            if subset:
                (inst_dir / filename).write_text("\n".join(subset) + "\n")
                logger.info(f"Index {filename}: {len(subset)} stocks")

    def finalize(
        self,
        csi300_codes: list[str] | None = None,
        csi500_codes: list[str] | None = None,
    ) -> None:
        self.write_calendar()
        self.write_instruments(csi300_codes, csi500_codes)


# ---------------------------------------------------------------------------
# Binary file helpers
# ---------------------------------------------------------------------------


def read_bin_file(bin_path: Path) -> tuple[int, np.ndarray]:
    """Read a Qlib binary feature file. Returns (start_index, data_array)."""
    raw = bin_path.read_bytes()
    start_idx = int(struct.unpack("<f", raw[:4])[0])
    data = np.frombuffer(raw[4:], dtype=np.float32).copy()
    return start_idx, data


def append_binary(
    feat_dir: Path,
    df: pd.DataFrame,
    cal_to_idx: dict[str, int],
) -> None:
    """Append new data points to an existing stock's binary files."""
    for field in QLIB_FIELDS:
        bin_path = feat_dir / f"{field}.day.bin"

        if bin_path.exists() and bin_path.stat().st_size >= 8:
            old_start, old_data = read_bin_file(bin_path)
            old_end = old_start + len(old_data) - 1
        else:
            old_start = None
            old_data = np.array([], dtype=np.float32)
            old_end = -1

        new_points: dict[int, float] = {}
        for _, row in df.iterrows():
            idx = cal_to_idx.get(row["date"])
            if idx is not None:
                new_points[idx] = float(row[field])

        if not new_points:
            continue

        new_min = min(new_points)
        new_max = max(new_points)

        if old_start is not None:
            final_start = min(old_start, new_min)
            final_end = max(old_end, new_max)
        else:
            final_start = new_min
            final_end = new_max

        span = final_end - final_start + 1
        merged = np.full(span, np.nan, dtype=np.float32)

        if old_start is not None and len(old_data) > 0:
            off = old_start - final_start
            merged[off: off + len(old_data)] = old_data

        for idx, val in new_points.items():
            merged[idx - final_start] = val

        header = np.array([final_start], dtype=np.float32)
        with open(bin_path, "wb") as f:
            f.write(header.tobytes())
            f.write(merged.tobytes())


def write_fresh_binary(
    feat_dir: Path,
    df: pd.DataFrame,
    cal_to_idx: dict[str, int],
) -> None:
    """Write Qlib binary files for a stock not previously in the dataset."""
    indices = [cal_to_idx[r["date"]] for _, r in df.iterrows() if r["date"] in cal_to_idx]
    if not indices:
        return

    start_idx = min(indices)
    span = max(indices) - start_idx + 1

    for field in QLIB_FIELDS:
        data = np.full(span, np.nan, dtype=np.float32)
        for _, row in df.iterrows():
            idx = cal_to_idx.get(row["date"])
            if idx is not None:
                data[idx - start_idx] = float(row[field])

        header = np.array([start_idx], dtype=np.float32)
        with open(feat_dir / f"{field}.day.bin", "wb") as f:
            f.write(header.tobytes())
            f.write(data.tobytes())


# ---------------------------------------------------------------------------
# Instruments rebuild from binary files
# ---------------------------------------------------------------------------


def _find_last_valid_index(bin_path: Path) -> tuple[int, int, int]:
    """Read a Qlib binary file and return (start_idx, end_idx, valid_count).

    ``end_idx`` is the index of the last non-NaN value.  If the entire
    file is NaN, ``end_idx`` equals ``start_idx - 1`` (i.e. empty).
    """
    raw = bin_path.read_bytes()
    start_idx = int(struct.unpack("<f", raw[:4])[0])
    data = np.frombuffer(raw[4:], dtype=np.float32)
    if len(data) == 0:
        return start_idx, start_idx - 1, 0

    # Walk backwards to skip trailing NaN
    last = len(data) - 1
    while last >= 0 and np.isnan(data[last]):
        last -= 1

    valid_count = int(np.count_nonzero(~np.isnan(data[: last + 1]))) if last >= 0 else 0
    end_idx = start_idx + last if last >= 0 else start_idx - 1
    return start_idx, end_idx, valid_count


def rebuild_instruments_from_binary(qlib_dir: str | Path) -> tuple[int, int]:
    """Rebuild instruments/all.txt from actual binary file ranges.

    Returns (total_entries, changed_count).

    Uses the last non-NaN value in each stock's close.day.bin to determine
    the true data end date, rather than relying solely on file size.
    """
    qlib_dir = Path(qlib_dir).expanduser()
    feat_dir = qlib_dir / "features"
    inst_path = qlib_dir / "instruments" / "all.txt"
    cal_path = qlib_dir / "calendars" / "day.txt"

    if not cal_path.exists() or not feat_dir.exists():
        logger.warning("rebuild_instruments: calendar or features dir missing, skipping")
        return 0, 0

    cal = [d.strip() for d in cal_path.read_text().strip().split("\n") if d.strip()]
    total_days = len(cal)

    old: dict[str, tuple[str, str]] = {}
    if inst_path.exists():
        for line in inst_path.read_text().strip().split("\n"):
            parts = line.strip().split("\t")
            if len(parts) >= 3:
                old[parts[0]] = (parts[1], parts[2])

    new: dict[str, tuple[str, str]] = {}
    changed = 0

    for sd in sorted(feat_dir.iterdir()):
        if not sd.is_dir():
            continue
        sym = sd.name.upper()
        close_bin = sd / "close.day.bin"
        if not close_bin.exists() or close_bin.stat().st_size < 8:
            if sym in old:
                new[sym] = old[sym]
            continue

        start_idx, end_idx, valid_count = _find_last_valid_index(close_bin)

        if start_idx < 0 or start_idx >= total_days or valid_count == 0:
            if sym in old:
                new[sym] = old[sym]
            continue

        bin_start = cal[start_idx]
        bin_end = cal[min(end_idx, total_days - 1)]

        if sym in old:
            old_s, old_e = old[sym]
            new_s = min(old_s, bin_start)
            new_e = max(old_e, bin_end)
        else:
            new_s, new_e = bin_start, bin_end

        if sym not in old or old[sym] != (new_s, new_e):
            changed += 1
        new[sym] = (new_s, new_e)

    for sym, v in old.items():
        if sym not in new:
            new[sym] = v

    inst_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"{sym}\t{s}\t{e}" for sym, (s, e) in sorted(new.items())]
    inst_path.write_text("\n".join(lines) + "\n")

    if changed:
        logger.info(f"Instruments rebuilt: {len(new)} entries, {changed} updated")
    else:
        logger.debug(f"Instruments check: {len(new)} entries, all consistent")

    return len(new), changed


def sync_meta_from_binary(qlib_dir: str | Path) -> int:
    """Sync data_meta.json ``data_end`` fields with actual binary file ranges.

    Returns the number of stock entries whose ``data_end`` was advanced.
    This fixes the case where incremental updates wrote binary data but
    the meta file wasn't persisted (e.g. interrupted run).
    """
    qlib_dir = Path(qlib_dir).expanduser()
    feat_dir = qlib_dir / "features"
    cal_path = qlib_dir / "calendars" / "day.txt"
    meta_path = qlib_dir / "data_meta.json"

    if not cal_path.exists() or not feat_dir.exists() or not meta_path.exists():
        return 0

    cal = [d.strip() for d in cal_path.read_text().strip().split("\n") if d.strip()]
    total_days = len(cal)

    from .meta import DataMeta
    from stopat30m.data.normalize import to_qlib_symbol

    meta = DataMeta.load(meta_path)
    updated = 0

    for code, sm in meta.stocks.items():
        if sm.status == "index":
            continue
        qlib_sym = to_qlib_symbol(code).lower()
        close_bin = feat_dir / qlib_sym / "close.day.bin"
        if not close_bin.exists() or close_bin.stat().st_size < 8:
            continue

        start_idx, end_idx, valid_count = _find_last_valid_index(close_bin)
        if valid_count == 0 or end_idx < 0 or end_idx >= total_days:
            continue

        bin_end_date = cal[end_idx]
        if not sm.data_end or sm.data_end < bin_end_date:
            sm.data_end = bin_end_date
            if not sm.data_start:
                bin_start_date = cal[max(0, start_idx)]
                sm.data_start = bin_start_date
            updated += 1

    if updated:
        meta.save(meta_path)
        logger.info(f"Meta synced from binary: {updated} stocks advanced data_end")
    else:
        logger.debug("Meta sync: all data_end consistent with binary files")

    return updated
