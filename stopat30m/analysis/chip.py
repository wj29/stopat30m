# -*- coding: utf-8 -*-
"""Chip distribution (筹码分布) — ported from daily_stock_analysis fetchers."""

from __future__ import annotations

import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from loguru import logger

from stopat30m.config import get
from stopat30m.data.normalize import bare_code

# ---------------------------------------------------------------------------
# From realtime_types.py — ChipDistribution, safe_float, safe_int
# ---------------------------------------------------------------------------


def safe_float(val: Any, default: Optional[float] = None) -> Optional[float]:
    """
    安全转换为浮点数

    处理场景：
    - None / 空字符串 → default
    - pandas NaN / numpy NaN → default
    - 数值字符串 → float
    - 已是数值 → float
    """
    try:
        if val is None:
            return default

        if isinstance(val, str):
            val = val.strip()
            if val == "" or val == "-" or val == "--":
                return default

        import math

        try:
            if math.isnan(float(val)):
                return default
        except (ValueError, TypeError):
            pass

        return float(val)
    except (ValueError, TypeError):
        return default


def safe_int(val: Any, default: Optional[int] = None) -> Optional[int]:
    """安全转换为整数 — 先转换为 float，再取整，处理 "123.0" 这类情况"""
    f_val = safe_float(val, default=None)
    if f_val is not None:
        return int(f_val)
    return default


@dataclass
class ChipDistribution:
    """
    筹码分布数据

    反映持仓成本分布和获利情况
    """

    code: str
    date: str = ""
    source: str = "akshare"

    # 获利情况
    profit_ratio: float = 0.0  # 获利比例(0-1)
    avg_cost: float = 0.0  # 平均成本

    # 筹码集中度
    cost_90_low: float = 0.0  # 90%筹码成本下限
    cost_90_high: float = 0.0  # 90%筹码成本上限
    concentration_90: float = 0.0  # 90%筹码集中度（越小越集中）

    cost_70_low: float = 0.0  # 70%筹码成本下限
    cost_70_high: float = 0.0  # 70%筹码成本上限
    concentration_70: float = 0.0  # 70%筹码集中度

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "code": self.code,
            "date": self.date,
            "source": self.source,
            "profit_ratio": self.profit_ratio,
            "avg_cost": self.avg_cost,
            "cost_90_low": self.cost_90_low,
            "cost_90_high": self.cost_90_high,
            "concentration_90": self.concentration_90,
            "concentration_70": self.concentration_70,
        }

    def get_chip_status(self, current_price: float) -> str:
        """
        获取筹码状态描述

        Args:
            current_price: 当前股价

        Returns:
            筹码状态描述
        """
        status_parts = []

        # 获利比例分析
        if self.profit_ratio >= 0.9:
            status_parts.append("获利盘极高(获利盘>90%)")
        elif self.profit_ratio >= 0.7:
            status_parts.append("获利盘较高(获利盘70-90%)")
        elif self.profit_ratio >= 0.5:
            status_parts.append("获利盘中等(获利盘50-70%)")
        elif self.profit_ratio >= 0.3:
            status_parts.append("套牢盘中等(套牢盘50-70%)")
        elif self.profit_ratio >= 0.1:
            status_parts.append("套牢盘较高(套牢盘70-90%)")
        else:
            status_parts.append("套牢盘极高(套牢盘>90%)")

        # 筹码集中度分析 (90%集中度 < 10% 表示集中)
        if self.concentration_90 < 0.08:
            status_parts.append("筹码高度集中")
        elif self.concentration_90 < 0.15:
            status_parts.append("筹码较集中")
        elif self.concentration_90 < 0.25:
            status_parts.append("筹码分散度中等")
        else:
            status_parts.append("筹码较分散")

        # 成本与现价关系
        if current_price > 0 and self.avg_cost > 0:
            cost_diff = (current_price - self.avg_cost) / self.avg_cost * 100
            if cost_diff > 20:
                status_parts.append(f"现价高于平均成本{cost_diff:.1f}%")
            elif cost_diff > 5:
                status_parts.append(f"现价略高于成本{cost_diff:.1f}%")
            elif cost_diff > -5:
                status_parts.append("现价接近平均成本")
            else:
                status_parts.append(f"现价低于平均成本{abs(cost_diff):.1f}%")

        return "，".join(status_parts)


# ---------------------------------------------------------------------------
# From akshare_fetcher.py — guards + rate limit (chip path)
# ---------------------------------------------------------------------------

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]


def _is_etf_code(stock_code: str) -> bool:
    """
    判断代码是否为 ETF 基金

    ETF 代码规则：
    - 上交所 ETF: 51xxxx, 52xxxx, 56xxxx, 58xxxx
    - 深交所 ETF: 15xxxx, 16xxxx, 18xxxx
    """
    etf_prefixes = ("51", "52", "56", "58", "15", "16", "18")
    code = stock_code.strip().split(".")[0]
    return code.startswith(etf_prefixes) and len(code) == 6


def _is_hk_code(stock_code: str) -> bool:
    """
    判断代码是否为港股

    港股代码规则：
    - 5位数字代码，如 '00700' (腾讯控股)
    - 部分港股代码可能带有前缀，如 'hk00700', 'hk1810'
    """
    code = stock_code.strip().lower()
    if code.endswith(".hk"):
        numeric_part = code[:-3]
        return numeric_part.isdigit() and 1 <= len(numeric_part) <= 5
    if code.startswith("hk"):
        numeric_part = code[2:]
        return numeric_part.isdigit() and 1 <= len(numeric_part) <= 5
    return code.isdigit() and len(code) == 5


def _is_us_code(stock_code: str) -> bool:
    """判断代码是否为美股股票（与 Tushare fetcher 规则一致）。"""
    code = stock_code.strip().upper()
    return bool(re.match(r"^[A-Z]{1,5}(\.[A-Z])?$", code))


_ak_sleep_min = 2.0
_ak_sleep_max = 5.0
_ak_last_request_time: Optional[float] = None

# push2his connectivity probe — once confirmed unreachable, skip all AKShare chip calls
_push2his_ok: Optional[bool] = None
_push2his_probe_ts: float = 0
_PUSH2HIS_PROBE_TTL = 600  # re-probe every 10 min


def _is_push2his_reachable() -> bool:
    """Quick HTTPS probe to push2his.eastmoney.com. Cached for 10 min."""
    global _push2his_ok, _push2his_probe_ts
    now = time.time()
    if _push2his_ok is not None and now - _push2his_probe_ts < _PUSH2HIS_PROBE_TTL:
        return _push2his_ok

    import requests as _req
    try:
        r = _req.get(
            "https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=0.000001&fields1=f1&fields2=f51&klt=101&fqt=1&beg=0&end=0&lmt=1",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=3,
        )
        ok = r.status_code == 200 and len(r.text) > 10
    except Exception:
        ok = False
    _push2his_ok = ok
    _push2his_probe_ts = now
    if not ok:
        logger.info("[筹码] push2his.eastmoney.com 不可达，AKShare 筹码路径将跳过 (10分钟后重试)")
    return ok


def _set_random_user_agent() -> None:
    try:
        random.choice(USER_AGENTS)
    except Exception:
        pass


def _enforce_akshare_rate_limit() -> None:
    global _ak_last_request_time
    if _ak_last_request_time is not None:
        elapsed = time.time() - _ak_last_request_time
        if elapsed < _ak_sleep_min:
            time.sleep(_ak_sleep_min - elapsed)

    sleep_time = random.uniform(_ak_sleep_min, _ak_sleep_max)
    time.sleep(sleep_time)
    _ak_last_request_time = time.time()


def _get_chip_distribution_akshare(stock_code: str) -> Optional[ChipDistribution]:
    """
    获取筹码分布数据（akshare_fetcher.get_chip_distribution）

    数据来源：ak.stock_cyq_em() (push2his.eastmoney.com)
    """
    if _is_us_code(stock_code):
        logger.debug(f"[API跳过] {stock_code} 是美股，无筹码分布数据")
        return None
    if _is_hk_code(stock_code):
        logger.debug(f"[API跳过] {stock_code} 是港股，无筹码分布数据")
        return None
    if _is_etf_code(stock_code):
        logger.debug(f"[API跳过] {stock_code} 是 ETF/指数，无筹码分布数据")
        return None

    if not _is_push2his_reachable():
        return None

    import akshare as ak

    try:
        _set_random_user_agent()
        _enforce_akshare_rate_limit()

        logger.info(f"[API调用] ak.stock_cyq_em(symbol={stock_code}) 获取筹码分布...")
        api_start = time.time()

        df = ak.stock_cyq_em(symbol=stock_code)

        api_elapsed = time.time() - api_start

        if df.empty:
            logger.warning(f"[API返回] ak.stock_cyq_em 返回空数据, 耗时 {api_elapsed:.2f}s")
            return None

        logger.info(f"[API返回] ak.stock_cyq_em 成功: 返回 {len(df)} 天数据, 耗时 {api_elapsed:.2f}s")

        latest = df.iloc[-1]

        chip = ChipDistribution(
            code=stock_code,
            date=str(latest.get("日期", "")),
            profit_ratio=safe_float(latest.get("获利比例")),
            avg_cost=safe_float(latest.get("平均成本")),
            cost_90_low=safe_float(latest.get("90成本-低")),
            cost_90_high=safe_float(latest.get("90成本-高")),
            concentration_90=safe_float(latest.get("90集中度")),
            cost_70_low=safe_float(latest.get("70成本-低")),
            cost_70_high=safe_float(latest.get("70成本-高")),
            concentration_70=safe_float(latest.get("70集中度")),
        )

        logger.info(
            f"[筹码分布] {stock_code} 日期={chip.date}: 获利比例={chip.profit_ratio:.1%}, "
            f"平均成本={chip.avg_cost}, 90%集中度={chip.concentration_90:.2%}, "
            f"70%集中度={chip.concentration_70:.2%}"
        )
        return chip

    except Exception as e:
        logger.warning(f"[筹码] AKShare 获取 {stock_code} 筹码分布失败: {e}")
        return None


# ---------------------------------------------------------------------------
# From tushare_fetcher.py — cyq_chips path + compute_cyq_metrics
# ---------------------------------------------------------------------------

_ETF_SH_PREFIXES = ("51", "52", "56", "58")
_ETF_SZ_PREFIXES = ("15", "16", "18")
_ETF_ALL_PREFIXES = _ETF_SH_PREFIXES + _ETF_SZ_PREFIXES


def _is_etf_code_ts(stock_code: str) -> bool:
    code = stock_code.strip().split(".")[0]
    return code.startswith(_ETF_ALL_PREFIXES) and len(code) == 6


def _is_hk_market(code: str) -> bool:
    """
    判定是否为港股代码（data_provider/base.py）。
    支持 `HK00700` 及纯 5 位数字形式（A 股 ETF/股票常见为 6 位）。
    """
    normalized = (code or "").strip().upper()
    if normalized.endswith(".HK"):
        base = normalized[:-3]
        return base.isdigit() and 1 <= len(base) <= 5
    if normalized.startswith("HK"):
        digits = normalized[2:]
        return digits.isdigit() and 1 <= len(digits) <= 5
    if normalized.isdigit() and len(normalized) == 5:
        return True
    return False


def is_bse_code(code: str) -> bool:
    """Check if the code is a Beijing Stock Exchange (BSE) A-share code."""
    c = (code or "").strip().split(".")[0]
    if len(c) != 6 or not c.isdigit():
        return False

    if c.startswith("900"):
        return False

    return c.startswith(("92", "43", "81", "82", "83", "87", "88"))


def normalize_stock_code(stock_code: str) -> str:
    """Normalize stock code (data_provider/base.py) — needed for Tushare ts_code."""
    code = stock_code.strip()
    upper = code.upper()

    if upper.startswith("HK") and not upper.startswith("HK."):
        candidate = upper[2:]
        if candidate.isdigit() and 1 <= len(candidate) <= 5:
            return f"HK{candidate.zfill(5)}"

    if upper.startswith(("SH", "SZ")) and not upper.startswith("SH.") and not upper.startswith("SZ."):
        candidate = code[2:]
        if candidate.isdigit() and len(candidate) in (5, 6):
            return candidate

    if upper.startswith("BJ") and not upper.startswith("BJ."):
        candidate = code[2:]
        if candidate.isdigit() and len(candidate) == 6:
            return candidate

    if "." in code:
        base, suffix = code.rsplit(".", 1)
        if suffix.upper() == "HK" and base.isdigit() and 1 <= len(base) <= 5:
            return f"HK{base.zfill(5)}"
        if suffix.upper() in ("SH", "SZ", "SS", "BJ") and base.isdigit():
            return base

    return code


def _detect_exchange_hint(stock_code: str) -> Optional[str]:
    """Return SH/SZ/BJ when the raw user input carries an explicit exchange hint."""
    upper = (stock_code or "").strip().upper()
    if upper.startswith(("SH", "SS")) or upper.endswith((".SH", ".SS")):
        return "SH"
    if upper.startswith("SZ") or upper.endswith(".SZ"):
        return "SZ"
    if upper.startswith("BJ") or upper.endswith(".BJ"):
        return "BJ"
    return None


def _convert_stock_code(stock_code: str) -> str:
    """
    转换 A 股 / ETF / 北交所等为 Tushare ts_code（tushare_fetcher._convert_stock_code）。
    """
    raw_code = stock_code.strip()

    if "." in raw_code:
        ts_code = raw_code.upper()
        if ts_code.endswith(".SS"):
            return f"{ts_code[:-3]}.SH"
        return ts_code

    if _is_us_code(raw_code):
        raise RuntimeError(f"Tushare 不支持美股 {raw_code}")

    if _is_hk_market(raw_code):
        return normalize_stock_code(raw_code)

    code = normalize_stock_code(raw_code)
    exchange_hint = _detect_exchange_hint(raw_code)

    if exchange_hint == "SH":
        return f"{code}.SH"
    if exchange_hint == "SZ":
        return f"{code}.SZ"
    if exchange_hint == "BJ":
        return f"{code}.BJ"

    if code.startswith(_ETF_SH_PREFIXES) and len(code) == 6:
        return f"{code}.SH"
    if code.startswith(_ETF_SZ_PREFIXES) and len(code) == 6:
        return f"{code}.SZ"

    if is_bse_code(code):
        return f"{code}.BJ"

    if code.startswith(("600", "601", "603", "688")):
        return f"{code}.SH"
    elif code.startswith(("000", "002", "300")):
        return f"{code}.SZ"
    else:
        logger.warning(f"无法确定股票 {code} 的市场，默认使用深市")
        return f"{code}.SZ"


def compute_cyq_metrics(df: pd.DataFrame, current_price: float) -> dict:
    """
    基于 Tushare 的筹码分布明细表 (cyq_chips) 计算常用筹码指标
    （tushare_fetcher.compute_cyq_metrics）
    """
    df_sorted = df.sort_values(by="price", ascending=True).reset_index(drop=True)

    total_percent = df_sorted["percent"].sum()

    df_sorted["norm_percent"] = df_sorted["percent"] / total_percent * 100

    df_sorted["cumsum"] = df_sorted["norm_percent"].cumsum()

    winner_rate = df_sorted[df_sorted["price"] <= current_price]["norm_percent"].sum()

    avg_cost = np.average(df_sorted["price"], weights=df_sorted["norm_percent"])

    def get_percentile_price(target_pct):
        idx = df_sorted["cumsum"].searchsorted(target_pct)
        idx = min(idx, len(df_sorted) - 1)
        return df_sorted.loc[idx, "price"]

    cost_90_low = get_percentile_price(5)
    cost_90_high = get_percentile_price(95)
    if (cost_90_high + cost_90_low) != 0:
        concentration_90 = (cost_90_high - cost_90_low) / (cost_90_high + cost_90_low) * 100
    else:
        concentration_90 = 0.0

    cost_70_low = get_percentile_price(15)
    cost_70_high = get_percentile_price(85)
    if (cost_70_high + cost_70_low) != 0:
        concentration_70 = (cost_70_high - cost_70_low) / (cost_70_high + cost_70_low) * 100
    else:
        concentration_70 = 0.0

    return {
        "获利比例": round(winner_rate / 100, 4),
        "平均成本": round(avg_cost, 4),
        "90成本-低": round(cost_90_low, 4),
        "90成本-高": round(cost_90_high, 4),
        "90集中度": round(concentration_90 / 100, 4),
        "70成本-低": round(cost_70_low, 4),
        "70成本-高": round(cost_70_high, 4),
        "70集中度": round(concentration_70 / 100, 4),
    }


class _TushareChipSession:
    """Minimal state for trade calendar + rate limit (tushare_fetcher methods)."""

    def __init__(self, pro: Any, rate_limit_per_minute: int = 80) -> None:
        self._api = pro
        self.rate_limit_per_minute = rate_limit_per_minute
        self._call_count = 0
        self._minute_start: Optional[float] = None
        self.date_list: Optional[List[str]] = None
        self._date_list_end: Optional[str] = None

    def _check_rate_limit(self) -> None:
        current_time = time.time()

        if self._minute_start is None:
            self._minute_start = current_time
            self._call_count = 0
        elif current_time - self._minute_start >= 60:
            self._minute_start = current_time
            self._call_count = 0
            logger.debug("速率限制计数器已重置")

        if self._call_count >= self.rate_limit_per_minute:
            elapsed = current_time - self._minute_start
            sleep_time = max(0, 60 - elapsed) + 1

            logger.warning(
                f"Tushare 达到速率限制 ({self._call_count}/{self.rate_limit_per_minute} 次/分钟)，"
                f"等待 {sleep_time:.1f} 秒..."
            )

            time.sleep(sleep_time)

            self._minute_start = time.time()
            self._call_count = 0

        self._call_count += 1
        logger.debug(f"Tushare 当前分钟调用次数: {self._call_count}/{self.rate_limit_per_minute}")

    def _call_api_with_rate_limit(self, method_name: str, **kwargs) -> pd.DataFrame:
        self._check_rate_limit()
        method = getattr(self._api, method_name)
        return method(**kwargs)

    def _get_china_now(self) -> datetime:
        return datetime.now(ZoneInfo("Asia/Shanghai"))

    def _get_trade_dates(self, end_date: Optional[str] = None) -> List[str]:
        china_now = self._get_china_now()
        requested_end_date = end_date or china_now.strftime("%Y%m%d")

        if self.date_list is not None and self._date_list_end == requested_end_date:
            return self.date_list

        start_date = (china_now - timedelta(days=20)).strftime("%Y%m%d")
        df_cal = self._call_api_with_rate_limit(
            "trade_cal",
            exchange="SSE",
            start_date=start_date,
            end_date=requested_end_date,
        )

        if df_cal is None or df_cal.empty or "cal_date" not in df_cal.columns:
            logger.warning("[Tushare] trade_cal 返回为空，无法更新交易日历缓存")
            self.date_list = []
            self._date_list_end = requested_end_date
            return self.date_list

        trade_dates = sorted(
            df_cal[df_cal["is_open"] == 1]["cal_date"].astype(str).tolist(),
            reverse=True,
        )
        self.date_list = trade_dates
        self._date_list_end = requested_end_date
        return trade_dates

    @staticmethod
    def _pick_trade_date(trade_dates: List[str], use_today: bool) -> Optional[str]:
        if not trade_dates:
            return None
        if use_today or len(trade_dates) == 1:
            return trade_dates[0]
        return trade_dates[1]

    def get_trade_time(self, early_time: str = "09:30", late_time: str = "16:30") -> Optional[str]:
        """
        获取当前时间可以获得数据的开始时间日期（tushare_fetcher.get_trade_time）
        """
        china_now = self._get_china_now()
        china_date = china_now.strftime("%Y%m%d")
        china_clock = china_now.strftime("%H:%M")

        trade_dates = self._get_trade_dates(china_date)
        if not trade_dates:
            return None

        if china_date in trade_dates:
            if early_time < china_clock < late_time:
                use_today = False
            else:
                use_today = True
        else:
            use_today = False

        start_date = self._pick_trade_date(trade_dates, use_today=use_today)
        if start_date is None:
            return None

        if not use_today:
            logger.info(
                f"[Tushare] 当前时间 {china_clock} 可能无法获取当天筹码分布，尝试获取前一个交易日的数据 {start_date}"
            )

        return start_date

    def get_chip_distribution(self, stock_code: str) -> Optional[ChipDistribution]:
        """
        获取筹码分布数据（tushare_fetcher.get_chip_distribution）
        """
        if _is_us_code(stock_code):
            logger.warning(f"[Tushare] TushareFetcher 不支持美股 {stock_code} 的筹码分布")
            return None

        if _is_etf_code_ts(stock_code):
            logger.warning(f"[Tushare] TushareFetcher 不支持 ETF {stock_code} 的筹码分布")
            return None

        if _is_hk_market(stock_code):
            logger.warning(f"[Tushare] TushareFetcher 不支持港股 {stock_code} 的筹码分布")
            return None

        try:
            start_date = self.get_trade_time(early_time="00:00", late_time="19:00")
            if not start_date:
                return None

            ts_code = _convert_stock_code(stock_code)

            df = self._call_api_with_rate_limit(
                "cyq_chips",
                ts_code=ts_code,
                start_date=start_date,
                end_date=start_date,
            )
            if df is not None and not df.empty:
                daily_df = self._call_api_with_rate_limit(
                    "daily",
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=start_date,
                )
                if daily_df is None or daily_df.empty:
                    return None
                current_price = daily_df.iloc[0]["close"]
                metrics = compute_cyq_metrics(df, current_price)

                chip = ChipDistribution(
                    code=stock_code,
                    date=datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d"),
                    profit_ratio=metrics["获利比例"],
                    avg_cost=metrics["平均成本"],
                    cost_90_low=metrics["90成本-低"],
                    cost_90_high=metrics["90成本-高"],
                    concentration_90=metrics["90集中度"],
                    cost_70_low=metrics["70成本-低"],
                    cost_70_high=metrics["70成本-高"],
                    concentration_70=metrics["70集中度"],
                )

                logger.info(
                    f"[筹码分布] {stock_code} 日期={chip.date}: 获利比例={chip.profit_ratio:.1%}, "
                    f"平均成本={chip.avg_cost}, 90%集中度={chip.concentration_90:.2%}, "
                    f"70%集中度={chip.concentration_70:.2%}"
                )
                return chip

        except Exception as e:
            logger.warning(f"[Tushare] 获取筹码分布失败 {stock_code}: {e}")
            return None

        return None


def _get_chip_distribution_tushare(stock_code: str, token: str) -> Optional[ChipDistribution]:
    try:
        import tushare as ts
    except ImportError:
        logger.debug("tushare not installed, skipping chip distribution fallback")
        return None

    pro = ts.pro_api(token)
    session = _TushareChipSession(pro)
    return session.get_chip_distribution(stock_code)


def _chip_to_payload(chip: ChipDistribution) -> Dict[str, Any]:
    """Serialize ChipDistribution for API/LLM (numeric + %-formatted concentration + status text)."""
    out = chip.to_dict()
    c90 = chip.concentration_90 if chip.concentration_90 is not None else 0.0
    c70 = chip.concentration_70 if chip.concentration_70 is not None else 0.0
    pr = chip.profit_ratio if chip.profit_ratio is not None else 0.0
    ac = chip.avg_cost if chip.avg_cost is not None else 0.0
    out["profit_ratio"] = pr
    out["avg_cost"] = ac
    out["concentration_90"] = f"{c90:.2%}" if c90 else "N/A"
    out["concentration_70"] = f"{c70:.2%}" if c70 else "N/A"
    status_chip = ChipDistribution(
        code=chip.code,
        date=chip.date,
        source=chip.source,
        profit_ratio=pr,
        avg_cost=ac,
        cost_90_low=chip.cost_90_low or 0.0,
        cost_90_high=chip.cost_90_high or 0.0,
        concentration_90=c90,
        cost_70_low=chip.cost_70_low or 0.0,
        cost_70_high=chip.cost_70_high or 0.0,
        concentration_70=c70,
    )
    out["chip_status"] = status_chip.get_chip_status(0.0)
    return out


def fetch_chip_distribution(code: str) -> dict | None:
    """Try AKShare (stock_cyq_em) first, then Tushare cyq_chips + compute_cyq_metrics."""
    stock_code = bare_code(code)

    chip = _get_chip_distribution_akshare(stock_code)
    if chip is not None:
        return _chip_to_payload(chip)

    token = str(get("tushare", "token") or "").strip()
    if not token:
        return None

    chip = _get_chip_distribution_tushare(stock_code, token)
    if chip is None:
        return None
    return _chip_to_payload(chip)
