"""
Factor expression definitions using Qlib expression language.

Organizes 514 additional factors beyond Alpha158 into 29 categories (672 total).
Each function returns a list of (expression_string, factor_name) tuples.

Qlib expression operators reference:
  Ref, Mean, Std, Max, Min, Med, Mad, Rank, Delta, Slope, Rsquare,
  Resi, Quantile, Count, Sum, Var, Skew, Kurt, WMA, EMA,
  Corr, Cov, Abs, Log, Sign, Power, If, Greater, Less
"""

from __future__ import annotations


def _ret(n: int) -> str:
    """Return expression: N-day return."""
    return f"$close/Ref($close,{n}) - 1"


def _log_ret(n: int) -> str:
    return f"Log($close/Ref($close,{n}))"


# ---------------------------------------------------------------------------
# Group 1: Momentum Factors (~45 factors)
# ---------------------------------------------------------------------------

def momentum_factors() -> list[tuple[str, str]]:
    factors = []

    # Price rate of change at multiple windows
    for w in [1, 2, 3, 5, 10, 20, 40, 60, 120, 250]:
        factors.append((_ret(w), f"MOM_ROC_{w}"))

    # Log returns
    for w in [1, 5, 10, 20, 60]:
        factors.append((_log_ret(w), f"MOM_LOGRET_{w}"))

    # Momentum acceleration: ROC of ROC
    for w in [5, 10, 20]:
        roc = f"$close/Ref($close,{w}) - 1"
        factors.append((
            f"({roc}) - Ref({roc},{w})",
            f"MOM_ACC_{w}",
        ))

    # Price relative to N-day high
    for w in [5, 10, 20, 60, 120, 250]:
        factors.append((
            f"$close / Max($high,{w}) - 1",
            f"MOM_HIGHREL_{w}",
        ))

    # Price relative to N-day low
    for w in [5, 10, 20, 60, 120, 250]:
        factors.append((
            f"$close / Min($low,{w}) - 1",
            f"MOM_LOWREL_{w}",
        ))

    # Price position within N-day range: (C - L_n) / (H_n - L_n)
    for w in [5, 10, 20, 60]:
        factors.append((
            f"($close - Min($low,{w})) / (Max($high,{w}) - Min($low,{w}) + 1e-8)",
            f"MOM_RANGEPOS_{w}",
        ))

    # EMA crossover signals
    for fast, slow in [(5, 20), (10, 30), (12, 26), (20, 60)]:
        factors.append((
            f"EMA($close,{fast}) / EMA($close,{slow}) - 1",
            f"MOM_EMAXOVER_{fast}_{slow}",
        ))

    # Price relative to SMA
    for w in [5, 10, 20, 60, 120]:
        factors.append((
            f"$close / Mean($close,{w}) - 1",
            f"MOM_SMADEV_{w}",
        ))

    return factors


# ---------------------------------------------------------------------------
# Group 2: Volatility Factors (~40 factors)
# ---------------------------------------------------------------------------

def volatility_factors() -> list[tuple[str, str]]:
    factors = []

    # Close-to-close realized volatility
    for w in [5, 10, 20, 60, 120]:
        factors.append((
            f"Std($close/Ref($close,1)-1, {w})",
            f"VOL_REALIZED_{w}",
        ))

    # Parkinson volatility: uses high-low range, more efficient than close-close
    for w in [5, 10, 20, 60]:
        factors.append((
            f"Std(Log($high/$low), {w}) / 1.67",
            f"VOL_PARKINSON_{w}",
        ))

    # Garman-Klass volatility proxy (simplified)
    for w in [5, 10, 20, 60]:
        factors.append((
            f"Mean(Power(Log($high/$low), 2), {w}) * 0.5 - Mean(Power(Log($close/Ref($close,1)), 2), {w}) * 0.3863",
            f"VOL_GK_{w}",
        ))

    # Volatility ratio: short-term vol / long-term vol (regime detection)
    for short, long in [(5, 20), (5, 60), (10, 60), (20, 120)]:
        factors.append((
            f"Std($close/Ref($close,1)-1, {short}) / (Std($close/Ref($close,1)-1, {long}) + 1e-8)",
            f"VOL_RATIO_{short}_{long}",
        ))

    # Return skewness
    for w in [20, 60, 120]:
        factors.append((
            f"Skew($close/Ref($close,1)-1, {w})",
            f"VOL_SKEW_{w}",
        ))

    # Return kurtosis
    for w in [20, 60, 120]:
        factors.append((
            f"Kurt($close/Ref($close,1)-1, {w})",
            f"VOL_KURT_{w}",
        ))

    # Maximum drawdown proxy: (max_high - current) / max_high
    for w in [10, 20, 60, 120]:
        factors.append((
            f"1 - $close / Max($high,{w})",
            f"VOL_MDD_{w}",
        ))

    # Intraday volatility: (high-low)/close
    factors.append((
        f"($high - $low) / ($close + 1e-8)",
        "VOL_INTRADAY",
    ))
    for w in [5, 10, 20]:
        factors.append((
            f"Mean(($high - $low) / ($close + 1e-8), {w})",
            f"VOL_INTRADAY_MA_{w}",
        ))

    # Volatility of volatility
    for w in [20, 60]:
        factors.append((
            f"Std(Std($close/Ref($close,1)-1, 5), {w})",
            f"VOL_VOLVOL_{w}",
        ))

    return factors


# ---------------------------------------------------------------------------
# Group 3: Volume & Liquidity Factors (~40 factors)
# ---------------------------------------------------------------------------

def volume_factors() -> list[tuple[str, str]]:
    factors = []

    # Volume ratio: short-term / long-term
    for short, long in [(1, 5), (5, 20), (5, 60), (10, 60), (20, 120)]:
        if short == 1:
            factors.append((
                f"$volume / (Mean($volume,{long}) + 1e-8)",
                f"VOL_VRATIO_{short}_{long}",
            ))
        else:
            factors.append((
                f"Mean($volume,{short}) / (Mean($volume,{long}) + 1e-8)",
                f"VOL_VRATIO_{short}_{long}",
            ))

    # Volume change rate
    for w in [1, 5, 10, 20]:
        if w == 1:
            factors.append((
                f"$volume / (Ref($volume,1) + 1e-8) - 1",
                f"VOL_VCHG_{w}",
            ))
        else:
            factors.append((
                f"Mean($volume,{w}) / (Mean(Ref($volume,{w}),{w}) + 1e-8) - 1",
                f"VOL_VCHG_{w}",
            ))

    # Abnormal volume: (vol - mean) / std
    for w in [20, 60]:
        factors.append((
            f"($volume - Mean($volume,{w})) / (Std($volume,{w}) + 1e-8)",
            f"VOL_ABNORMAL_{w}",
        ))

    # Price-volume correlation
    for w in [10, 20, 60]:
        factors.append((
            f"Corr($close/Ref($close,1)-1, $volume/Ref($volume,1)-1, {w})",
            f"VOL_PVCORR_{w}",
        ))

    # VWAP deviation proxy: using (high+low+close)/3 * volume
    for w in [5, 10, 20]:
        factors.append((
            f"$close - Sum(($high+$low+$close)/3 * $volume, {w}) / (Sum($volume,{w}) + 1e-8)",
            f"VOL_VWAPDEV_{w}",
        ))

    # Accumulation/Distribution proxy
    # AD = ((close-low) - (high-close)) / (high-low) * volume
    factors.append((
        "(2*$close - $low - $high) / ($high - $low + 1e-8) * $volume",
        "VOL_AD_LINE",
    ))
    for w in [5, 10, 20]:
        factors.append((
            f"Sum((2*$close - $low - $high) / ($high - $low + 1e-8) * $volume, {w})",
            f"VOL_AD_SUM_{w}",
        ))

    # Money Flow proxy: typical_price * volume
    for w in [5, 10, 20]:
        factors.append((
            f"Mean(($high+$low+$close)/3 * $volume, {w})",
            f"VOL_MF_{w}",
        ))

    # OBV rate of change proxy
    for w in [5, 10, 20]:
        factors.append((
            f"Sum(If($close>Ref($close,1), $volume, 0-$volume), {w})",
            f"VOL_OBV_{w}",
        ))

    # Amihud illiquidity: |return| / volume
    for w in [5, 20, 60]:
        factors.append((
            f"Mean(Abs($close/Ref($close,1)-1) / ($volume/1e6 + 1e-8), {w})",
            f"VOL_AMIHUD_{w}",
        ))

    # Turnover rate change (skip w=5, duplicate of VRATIO_1_5)
    for w in [10, 20]:
        factors.append((
            f"$volume / (Mean($volume,{w}) + 1e-8)",
            f"VOL_TURNREL_{w}",
        ))

    return factors


# ---------------------------------------------------------------------------
# Group 4: Advanced Technical Indicators (~50 factors)
# ---------------------------------------------------------------------------

def technical_factors() -> list[tuple[str, str]]:
    factors = []

    # --- Bollinger Bands ---
    for w in [10, 20, 60]:
        # Band width: (upper - lower) / middle
        factors.append((
            f"2 * Std($close,{w}) / (Mean($close,{w}) + 1e-8)",
            f"TECH_BBWIDTH_{w}",
        ))
        # Band position: (close - lower) / (upper - lower)
        factors.append((
            f"($close - Mean($close,{w}) + 2*Std($close,{w})) / (4*Std($close,{w}) + 1e-8)",
            f"TECH_BBPOS_{w}",
        ))
        # Distance from middle band
        factors.append((
            f"($close - Mean($close,{w})) / (Std($close,{w}) + 1e-8)",
            f"TECH_BBZSCORE_{w}",
        ))

    # --- MACD-like features ---
    # MACD line: EMA12 - EMA26
    factors.append((
        "EMA($close,12) - EMA($close,26)",
        "TECH_MACD_LINE",
    ))
    # MACD signal approximation using EMA of MACD line
    factors.append((
        "EMA($close,12) - EMA($close,26) - EMA(EMA($close,12) - EMA($close,26), 9)",
        "TECH_MACD_HIST",
    ))
    # MACD normalized
    factors.append((
        "(EMA($close,12) - EMA($close,26)) / ($close + 1e-8)",
        "TECH_MACD_NORM",
    ))

    # --- Williams %R ---
    for w in [10, 14, 20]:
        factors.append((
            f"(Max($high,{w}) - $close) / (Max($high,{w}) - Min($low,{w}) + 1e-8)",
            f"TECH_WILLR_{w}",
        ))

    # --- CCI (Commodity Channel Index) ---
    for w in [14, 20]:
        tp = "($high+$low+$close)/3"
        factors.append((
            f"({tp} - Mean({tp},{w})) / (Mad({tp},{w}) * 0.015 + 1e-8)",
            f"TECH_CCI_{w}",
        ))

    # --- ADX proxy: uses directional movement ---
    for w in [14, 20]:
        factors.append((
            f"Abs(Mean($high - Ref($high,1), {w}) - Mean(Ref($low,1) - $low, {w})) / (Mean($high - $low, {w}) + 1e-8)",
            f"TECH_ADX_PROXY_{w}",
        ))

    # --- Stochastic oscillator %K ---
    for w in [9, 14]:
        factors.append((
            f"($close - Min($low,{w})) / (Max($high,{w}) - Min($low,{w}) + 1e-8)",
            f"TECH_STOCH_K_{w}",
        ))
        # %D = SMA of %K
        factors.append((
            f"Mean(($close - Min($low,{w})) / (Max($high,{w}) - Min($low,{w}) + 1e-8), 3)",
            f"TECH_STOCH_D_{w}",
        ))

    # --- Donchian Channel ---
    for w in [10, 20, 60]:
        # Channel width relative to price
        factors.append((
            f"(Max($high,{w}) - Min($low,{w})) / ($close + 1e-8)",
            f"TECH_DONCHIAN_W_{w}",
        ))
        # Channel midpoint relative to price (distinct from MOM_RANGEPOS)
        factors.append((
            f"$close / ((Max($high,{w}) + Min($low,{w})) / 2 + 1e-8) - 1",
            f"TECH_DONCHIAN_MID_{w}",
        ))

    # --- RSI-like constructs ---
    for w in [6, 12, 24]:
        # Up ratio
        factors.append((
            f"Mean(If($close>Ref($close,1), $close-Ref($close,1), 0), {w}) / (Mean(If($close>Ref($close,1), $close-Ref($close,1), 0), {w}) + Mean(If($close<Ref($close,1), Ref($close,1)-$close, 0), {w}) + 1e-8)",
            f"TECH_RSI_{w}",
        ))

    # --- Elder Ray ---
    for w in [13, 26]:
        # Bull power: high - EMA
        factors.append((
            f"($high - EMA($close,{w})) / ($close + 1e-8)",
            f"TECH_BULLPOWER_{w}",
        ))
        # Bear power: low - EMA
        factors.append((
            f"($low - EMA($close,{w})) / ($close + 1e-8)",
            f"TECH_BEARPOWER_{w}",
        ))

    # --- Ichimoku-like features ---
    # Tenkan-Kijun spread
    factors.append((
        "(Max($high,9) + Min($low,9))/2 - (Max($high,26) + Min($low,26))/2",
        "TECH_ICHIMOKU_TK",
    ))
    # Price relative to cloud
    factors.append((
        "$close - (Max($high,26) + Min($low,26))/2",
        "TECH_ICHIMOKU_CLOUD",
    ))

    # --- Gap features ---
    factors.append((
        "$open / Ref($close,1) - 1",
        "TECH_GAP",
    ))
    for w in [5, 10, 20]:
        factors.append((
            f"Mean(Abs($open/Ref($close,1)-1), {w})",
            f"TECH_GAP_AVG_{w}",
        ))

    # --- Upper/Lower shadow ratios ---
    factors.append((
        "($high - If($close>$open, $close, $open)) / ($high - $low + 1e-8)",
        "TECH_UPPER_SHADOW",
    ))
    factors.append((
        "(If($close<$open, $close, $open) - $low) / ($high - $low + 1e-8)",
        "TECH_LOWER_SHADOW",
    ))
    # Body ratio
    factors.append((
        "Abs($close - $open) / ($high - $low + 1e-8)",
        "TECH_BODY_RATIO",
    ))

    return factors


# ---------------------------------------------------------------------------
# Group 5: Microstructure & Spread Factors (~20 factors)
# ---------------------------------------------------------------------------

def microstructure_factors() -> list[tuple[str, str]]:
    factors = []

    # Roll spread estimator: 2 * sqrt(-Cov(ret, ret_lag1))
    # simplified proxy using serial covariance of returns
    for w in [20, 60]:
        factors.append((
            f"Cov($close/Ref($close,1)-1, Ref($close,1)/Ref($close,2)-1, {w})",
            f"MICRO_ROLLCOV_{w}",
        ))

    # High-Low spread proxy (Corwin-Schultz)
    for w in [5, 10, 20]:
        factors.append((
            f"Mean(Log($high/$low), {w})",
            f"MICRO_HLSPREAD_{w}",
        ))

    # Kyle's Lambda proxy: price impact = |ret| / volume
    for w in [5, 20, 60]:
        factors.append((
            f"Slope(Abs($close/Ref($close,1)-1), {w}) / (Mean($volume,{w})/1e6 + 1e-8)",
            f"MICRO_KYLE_{w}",
        ))

    # Intraday intensity: (2*close - high - low) / (high - low)
    for w in [5, 10, 20]:
        factors.append((
            f"Mean((2*$close - $high - $low) / ($high - $low + 1e-8), {w})",
            f"MICRO_INTENSITY_{w}",
        ))

    # Close location value: (close - low) / (high - low)
    factors.append((
        "($close - $low) / ($high - $low + 1e-8)",
        "MICRO_CLV",
    ))
    for w in [5, 10, 20]:
        factors.append((
            f"Mean(($close - $low) / ($high - $low + 1e-8), {w})",
            f"MICRO_CLV_MA_{w}",
        ))

    return factors


# ---------------------------------------------------------------------------
# Group 6: Trend & Regression Factors (~30 factors)
# ---------------------------------------------------------------------------

def trend_factors() -> list[tuple[str, str]]:
    factors = []

    # Linear regression slope
    for w in [5, 10, 20, 60, 120]:
        factors.append((
            f"Slope($close, {w})",
            f"TREND_SLOPE_{w}",
        ))

    # R-squared of trend: higher = stronger trend
    for w in [10, 20, 60, 120]:
        factors.append((
            f"Rsquare($close, {w})",
            f"TREND_RSQUARE_{w}",
        ))

    # Regression residual: deviation from trend
    for w in [10, 20, 60]:
        factors.append((
            f"Resi($close, {w}) / ($close + 1e-8)",
            f"TREND_RESI_{w}",
        ))

    # Slope of returns: accelerating/decelerating
    for w in [10, 20, 60]:
        factors.append((
            f"Slope($close/Ref($close,1)-1, {w})",
            f"TREND_RETSLOPE_{w}",
        ))

    # Slope of volume: increasing/decreasing participation
    for w in [10, 20, 60]:
        factors.append((
            f"Slope($volume, {w})",
            f"TREND_VOLSLOPE_{w}",
        ))

    # Mean reversion: distance from rolling median
    for w in [10, 20, 60]:
        factors.append((
            f"$close / (Med($close,{w}) + 1e-8) - 1",
            f"TREND_MEDDEV_{w}",
        ))

    # Consecutive up/down days proxy
    factors.append((
        "Count($close>Ref($close,1), 10)",
        "TREND_UPCOUNT_10",
    ))
    factors.append((
        "Count($close<Ref($close,1), 10)",
        "TREND_DOWNCOUNT_10",
    ))

    return factors


# ---------------------------------------------------------------------------
# Group 7: Cross-Sectional & Rank Factors (~25 factors)
# ---------------------------------------------------------------------------

def cross_sectional_factors() -> list[tuple[str, str]]:
    """Rank-based factors for cross-sectional alpha."""
    factors = []

    # Rank of returns
    for w in [5, 10, 20, 60]:
        factors.append((
            f"Rank($close/Ref($close,{w})-1, {w})",
            f"CS_RANKRET_{w}",
        ))

    # Rank of volume change
    for w in [5, 20]:
        factors.append((
            f"Rank($volume / (Mean($volume,{w}) + 1e-8), {w})",
            f"CS_RANKVOL_{w}",
        ))

    # Rank of volatility
    for w in [10, 20, 60]:
        factors.append((
            f"Rank(Std($close/Ref($close,1)-1, {w}), {w})",
            f"CS_RANKVOLA_{w}",
        ))

    # Rank of Amihud
    for w in [20, 60]:
        factors.append((
            f"Rank(Mean(Abs($close/Ref($close,1)-1) / ($volume/1e6 + 1e-8), {w}), {w})",
            f"CS_RANKAMIHUD_{w}",
        ))

    # Rank of turnover
    factors.append((
        "Rank($volume, 20)",
        "CS_RANKTURNOVER",
    ))

    # Rank of price level
    factors.append((
        "Rank($close, 20)",
        "CS_RANKPRICE",
    ))

    # Rank of slope (trend strength)
    for w in [20, 60]:
        factors.append((
            f"Rank(Slope($close, {w}), {w})",
            f"CS_RANKSLOPE_{w}",
        ))

    # Rank of max drawdown
    for w in [20, 60]:
        factors.append((
            f"Rank(1 - $close / Max($high,{w}), {w})",
            f"CS_RANKMDD_{w}",
        ))

    return factors


# ---------------------------------------------------------------------------
# Group 8: Multi-Scale Features (~25 factors)
# ---------------------------------------------------------------------------

def multiscale_factors() -> list[tuple[str, str]]:
    """Features capturing interactions across different time scales."""
    factors = []

    # Short-term vs long-term return spread
    for short, long in [(5, 20), (5, 60), (10, 60), (20, 120), (60, 250)]:
        factors.append((
            f"($close/Ref($close,{short})-1) - ($close/Ref($close,{long})-1)",
            f"MS_RETSPREAD_{short}_{long}",
        ))

    # Short-term vs long-term volume correlation
    for short, long in [(5, 20), (10, 60)]:
        factors.append((
            f"Corr($close, $volume, {short}) - Corr($close, $volume, {long})",
            f"MS_CORRDIFF_{short}_{long}",
        ))

    # Slope difference at multiple windows
    for short, long in [(5, 20), (10, 60), (20, 120)]:
        factors.append((
            f"Slope($close,{short}) - Slope($close,{long})",
            f"MS_SLOPEDIFF_{short}_{long}",
        ))

    # Relative volatility regime
    for short, long in [(5, 20), (10, 60), (20, 120)]:
        factors.append((
            f"Std($close/Ref($close,1)-1, {short}) - Std($close/Ref($close,1)-1, {long})",
            f"MS_VOLDIFF_{short}_{long}",
        ))

    # EMA convergence/divergence across scales
    _seen_ema = set()
    for fast, mid, slow in [(5, 20, 60), (10, 30, 90), (20, 60, 120)]:
        key1 = (fast, mid)
        if key1 not in _seen_ema:
            factors.append((
                f"(EMA($close,{fast}) - EMA($close,{mid})) / ($close + 1e-8)",
                f"MS_EMACD_{fast}_{mid}",
            ))
            _seen_ema.add(key1)
        key2 = (mid, slow)
        if key2 not in _seen_ema:
            factors.append((
                f"(EMA($close,{mid}) - EMA($close,{slow})) / ($close + 1e-8)",
                f"MS_EMACD_{mid}_{slow}",
            ))
            _seen_ema.add(key2)

    return factors


# ---------------------------------------------------------------------------
# Group 9: Return Distribution Factors (~15 factors)
# ---------------------------------------------------------------------------

def distribution_factors() -> list[tuple[str, str]]:
    factors = []

    # Quantile of current price within rolling window
    for w in [20, 60, 120, 250]:
        factors.append((
            f"Quantile($close, {w}, 0.5)",
            f"DIST_QUANTILE50_{w}",
        ))

    # Proportion of up days
    for w in [10, 20, 60]:
        factors.append((
            f"Count($close>Ref($close,1), {w}) / {w}",
            f"DIST_UPFREQ_{w}",
        ))

    # Max daily gain/loss in window
    for w in [20, 60]:
        factors.append((
            f"Max($close/Ref($close,1)-1, {w})",
            f"DIST_MAXGAIN_{w}",
        ))
        factors.append((
            f"Min($close/Ref($close,1)-1, {w})",
            f"DIST_MAXLOSS_{w}",
        ))

    # Variance ratio (tests random walk)
    for short, long in [(5, 20), (10, 60)]:
        factors.append((
            f"Var($close/Ref($close,1)-1, {long}) / (Var($close/Ref($close,1)-1, {short}) * {long}/{short} + 1e-8)",
            f"DIST_VARRATIO_{short}_{long}",
        ))

    return factors


# ---------------------------------------------------------------------------
# Group 10: Reversal Factors (~22 factors)
# ---------------------------------------------------------------------------

def reversal_factors() -> list[tuple[str, str]]:
    """Short-term reversal and contrarian signals."""
    factors = []

    # Short-term reversal: negative of recent return
    for w in [1, 2, 3, 5]:
        factors.append((
            f"1 - $close/Ref($close,{w})",
            f"REV_SHORT_{w}",
        ))

    # Reversal adjusted by volatility: return / realized_vol
    for w in [5, 10, 20]:
        factors.append((
            f"($close/Ref($close,{w})-1) / (Std($close/Ref($close,1)-1, {w}) + 1e-8)",
            f"REV_VOLADJ_{w}",
        ))

    # Long-term reversal (overreaction proxy)
    for w in [60, 120, 250]:
        factors.append((
            f"1 - $close/Ref($close,{w})",
            f"REV_LONG_{w}",
        ))

    # Return deviation from mean return
    for w in [10, 20, 60]:
        factors.append((
            f"($close/Ref($close,1)-1) - Mean($close/Ref($close,1)-1, {w})",
            f"REV_DEMEAN_{w}",
        ))

    # Swing factor: max up vs max down in window
    for w in [10, 20]:
        factors.append((
            f"Max($close/Ref($close,1)-1, {w}) + Min($close/Ref($close,1)-1, {w})",
            f"REV_SWING_{w}",
        ))

    # Overnight vs intraday return divergence
    factors.append((
        "($open/Ref($close,1)-1) - ($close/$open-1)",
        "REV_OVERNIGHT_INTRADAY",
    ))

    return factors


# ---------------------------------------------------------------------------
# Group 11: Tail Risk Factors (~18 factors)
# ---------------------------------------------------------------------------

def tail_risk_factors() -> list[tuple[str, str]]:
    """Tail risk, extreme events, and downside risk measures."""
    factors = []

    # Lower partial moment (downside semi-variance)
    for w in [20, 60, 120]:
        factors.append((
            f"Mean(Power(If($close/Ref($close,1)-1<0, $close/Ref($close,1)-1, 0), 2), {w})",
            f"TAIL_DSV_{w}",
        ))

    # Upside semi-variance
    for w in [20, 60]:
        factors.append((
            f"Mean(Power(If($close/Ref($close,1)-1>0, $close/Ref($close,1)-1, 0), 2), {w})",
            f"TAIL_USV_{w}",
        ))

    # Tail ratio: upside std / downside std
    for w in [20, 60]:
        factors.append((
            f"Mean(Power(If($close/Ref($close,1)-1>0, $close/Ref($close,1)-1, 0), 2), {w}) / (Mean(Power(If($close/Ref($close,1)-1<0, $close/Ref($close,1)-1, 0), 2), {w}) + 1e-8)",
            f"TAIL_RATIO_{w}",
        ))

    # Frequency of extreme negative returns (< -2std)
    for w in [20, 60, 120]:
        factors.append((
            f"Count($close/Ref($close,1)-1 < -2*Std($close/Ref($close,1)-1,{w}), {w}) / {w}",
            f"TAIL_CRASHFREQ_{w}",
        ))

    # Frequency of extreme positive returns (> 2std)
    for w in [20, 60]:
        factors.append((
            f"Count($close/Ref($close,1)-1 > 2*Std($close/Ref($close,1)-1,{w}), {w}) / {w}",
            f"TAIL_SURGEFREQ_{w}",
        ))

    # Expected shortfall proxy: average of worst returns
    for w in [20, 60]:
        factors.append((
            f"Mean(If($close/Ref($close,1)-1 < Quantile($close/Ref($close,1)-1, {w}, 0.1), $close/Ref($close,1)-1, 0), {w})",
            f"TAIL_CVAR_{w}",
        ))

    return factors


# ---------------------------------------------------------------------------
# Group 12: Price Pattern Factors (~20 factors)
# ---------------------------------------------------------------------------

def price_pattern_factors() -> list[tuple[str, str]]:
    """Candlestick patterns and structural price features."""
    factors = []

    # Inside bar: today's range inside yesterday's range
    factors.append((
        "If(($high<Ref($high,1)) & ($low>Ref($low,1)), 1, 0)",
        "PAT_INSIDE_BAR",
    ))
    for w in [5, 10, 20]:
        factors.append((
            f"Count(($high<Ref($high,1)) & ($low>Ref($low,1)), {w})",
            f"PAT_INSIDE_COUNT_{w}",
        ))

    # Outside bar: today's range engulfs yesterday's range
    factors.append((
        "If(($high>Ref($high,1)) & ($low<Ref($low,1)), 1, 0)",
        "PAT_OUTSIDE_BAR",
    ))

    # Consecutive up closes (3, 5 only; 10/20 same as DIST_UPFREQ)
    for w in [3, 5]:
        factors.append((
            f"Count($close>Ref($close,1), {w}) / {w}",
            f"PAT_UPSEQ_{w}",
        ))

    # Consecutive down closes
    for w in [3, 5]:
        factors.append((
            f"Count($close<Ref($close,1), {w}) / {w}",
            f"PAT_DOWNSEQ_{w}",
        ))

    # New high breakout: close above N-day max
    for w in [20, 60, 120]:
        factors.append((
            f"If($close>=Max($high,{w}), 1, 0)",
            f"PAT_NEWHIGH_{w}",
        ))

    # New low breakdown: close below N-day min
    for w in [20, 60]:
        factors.append((
            f"If($close<=Min($low,{w}), 1, 0)",
            f"PAT_NEWLOW_{w}",
        ))

    # Doji candle: small body relative to range
    factors.append((
        "If(Abs($close-$open)/($high-$low+1e-8) < 0.1, 1, 0)",
        "PAT_DOJI",
    ))

    return factors


# ---------------------------------------------------------------------------
# Group 13: Volume-Price Interaction Factors (~22 factors)
# ---------------------------------------------------------------------------

def volume_price_factors() -> list[tuple[str, str]]:
    """Sophisticated volume-price divergence and confirmation signals."""
    factors = []

    # Smart money divergence: price up but volume down (or vice versa)
    for w in [5, 10, 20]:
        factors.append((
            f"Corr(Rank($close/Ref($close,1)-1, {w}), Rank($volume/Ref($volume,1)-1, {w}), {w})",
            f"VP_SMARTMONEY_{w}",
        ))

    # Volume-weighted return
    for w in [5, 10, 20]:
        factors.append((
            f"Sum(($close/Ref($close,1)-1)*$volume, {w}) / (Sum($volume,{w}) + 1e-8)",
            f"VP_VWRET_{w}",
        ))

    # Up-volume ratio: volume on up days / total volume
    for w in [10, 20, 60]:
        factors.append((
            f"Sum(If($close>Ref($close,1), $volume, 0), {w}) / (Sum($volume,{w}) + 1e-8)",
            f"VP_UPVOLRATIO_{w}",
        ))

    # Price-volume divergence: return ranks vs volume ranks
    for w in [10, 20]:
        factors.append((
            f"Sum($close/Ref($close,1)-1, {w}) * Sum($volume, {w})",
            f"VP_PVFORCE_{w}",
        ))

    # High-volume return vs low-volume return
    for w in [20, 60]:
        factors.append((
            f"Mean(If($volume>Mean($volume,{w}), $close/Ref($close,1)-1, 0), {w}) - Mean(If($volume<Mean($volume,{w}), $close/Ref($close,1)-1, 0), {w})",
            f"VP_HIVOL_RET_{w}",
        ))

    # Chaikin Money Flow: sum of CLV*volume / sum of volume
    for w in [10, 20, 60]:
        factors.append((
            f"Sum((2*$close-$low-$high)/($high-$low+1e-8)*$volume, {w}) / (Sum($volume,{w}) + 1e-8)",
            f"VP_CMF_{w}",
        ))

    # Volume surprise on return: |return| when volume is abnormal
    for w in [20, 60]:
        factors.append((
            f"Mean(If($volume>Mean($volume,{w})+Std($volume,{w}), Abs($close/Ref($close,1)-1), 0), {w})",
            f"VP_VOLSURPRISE_{w}",
        ))

    return factors


# ---------------------------------------------------------------------------
# Group 14: Mean Reversion Factors (~18 factors)
# ---------------------------------------------------------------------------

def mean_reversion_factors() -> list[tuple[str, str]]:
    """Z-scores, Ornstein-Uhlenbeck-inspired, and mean reversion signals."""
    factors = []

    # Z-score of price relative to rolling window
    for w in [10, 20, 60, 120]:
        factors.append((
            f"($close - Mean($close, {w})) / (Std($close, {w}) + 1e-8)",
            f"MR_ZSCORE_{w}",
        ))

    # Z-score of log price
    for w in [20, 60, 120]:
        factors.append((
            f"(Log($close) - Mean(Log($close), {w})) / (Std(Log($close), {w}) + 1e-8)",
            f"MR_LOGZSCORE_{w}",
        ))

    # Mean reversion speed proxy: autocorrelation of returns
    for w in [10, 20, 60]:
        factors.append((
            f"Corr($close/Ref($close,1)-1, Ref($close/Ref($close,1)-1,1), {w})",
            f"MR_AUTOCORR_{w}",
        ))

    # Distance from EMA (reversion target)
    for w in [10, 20, 60, 120]:
        factors.append((
            f"($close - EMA($close, {w})) / (Std($close, {w}) + 1e-8)",
            f"MR_EMAZSCORE_{w}",
        ))

    # Cumulative return deviation from trend
    for w in [20, 60]:
        factors.append((
            f"Sum($close/Ref($close,1)-1 - Mean($close/Ref($close,1)-1,{w}), {w})",
            f"MR_CUMDEV_{w}",
        ))

    return factors


# ---------------------------------------------------------------------------
# Group 15: Relative Strength Factors (~18 factors)
# ---------------------------------------------------------------------------

def relative_strength_factors() -> list[tuple[str, str]]:
    """Cross-sectional relative strength and rotation signals.
    (CS_RANKRET, CS_RANKVOLA already cover basic rank-returns/vol; this focuses on composites.)
    """
    factors = []

    # Rank of long-horizon return only (120/250 not in CS_RANKRET)
    factors.append(("Rank($close/Ref($close,120)-1, 120)", "RS_RANK_120"))
    factors.append(("Rank($close/Ref($close,250)-1, 250)", "RS_RANK_250"))

    # Relative volume rank (Mean vol, not raw vol like CS_RANKTURNOVER)
    for w in [5, 20, 60]:
        factors.append((
            f"Rank(Mean($volume, {w}), {w})",
            f"RS_VOLRANK_{w}",
        ))

    # Relative momentum spread: short rank - long rank (rotation signal)
    for short, long in [(5, 60), (10, 120), (20, 250)]:
        factors.append((
            f"Rank($close/Ref($close,{short})-1, {short}) - Rank($close/Ref($close,{long})-1, {long})",
            f"RS_MOMSPREAD_{short}_{long}",
        ))

    # Rank of Sharpe ratio proxy: mean_ret / std_ret
    for w in [20, 60, 120]:
        factors.append((
            f"Rank(Mean($close/Ref($close,1)-1, {w}) / (Std($close/Ref($close,1)-1, {w}) + 1e-8), {w})",
            f"RS_SHARPERANK_{w}",
        ))

    # Rank of Sortino-like: mean_ret / downside_std
    for w in [20, 60]:
        factors.append((
            f"Rank(Mean($close/Ref($close,1)-1, {w}) / (Std(If($close<Ref($close,1), $close/Ref($close,1)-1, 0), {w}) + 1e-8), {w})",
            f"RS_SORTRANK_{w}",
        ))

    return factors


# ---------------------------------------------------------------------------
# Group 16: Weighted & Adjusted Momentum (~20 factors)
# ---------------------------------------------------------------------------

def weighted_momentum_factors() -> list[tuple[str, str]]:
    """Volume-weighted, volatility-adjusted, and decay-weighted momentum."""
    factors = []

    # Volume-weighted momentum (60 only; 10/20 same as VP_VWRET)
    factors.append((
        "Sum(($close/Ref($close,1)-1)*$volume, 60) / (Sum($volume,60) + 1e-8)",
        "WM_VOLWEIGHTED_60",
    ))
    # Volume-weighted 5-day return (distinct from daily VP_VWRET)
    for w in [10, 20]:
        factors.append((
            f"Sum(($close/Ref($close,5)-1)*$volume, {w}) / (Sum($volume,{w}) + 1e-8)",
            f"WM_VOLWEIGHTED5D_{w}",
        ))

    # Volatility-adjusted momentum: return / volatility
    for w in [5, 10, 20, 60, 120]:
        factors.append((
            f"($close/Ref($close,{w})-1) / (Std($close/Ref($close,1)-1,{w}) + 1e-8)",
            f"WM_VOLADJ_{w}",
        ))

    # EMA-weighted momentum (recent returns weighted more)
    for w in [10, 20, 60]:
        factors.append((
            f"WMA($close/Ref($close,1)-1, {w})",
            f"WM_WMARET_{w}",
        ))

    # Exponentially weighted momentum
    for w in [10, 20, 60]:
        factors.append((
            f"EMA($close/Ref($close,1)-1, {w})",
            f"WM_EMARET_{w}",
        ))

    # Information ratio proxy: mean return / tracking error
    for w in [20, 60, 120]:
        factors.append((
            f"Mean($close/Ref($close,1)-1, {w}) / (Mad($close/Ref($close,1)-1, {w}) + 1e-8)",
            f"WM_IR_{w}",
        ))

    return factors


# ---------------------------------------------------------------------------
# Group 17: Higher-Order Price Dynamics (~18 factors)
# ---------------------------------------------------------------------------

def higher_order_factors() -> list[tuple[str, str]]:
    """Price acceleration, curvature, and higher-order derivative features."""
    factors = []

    # Price velocity: first difference normalized
    for w in [1, 5, 10]:
        factors.append((
            f"Delta($close, {w}) / ($close + 1e-8)",
            f"HO_VELOCITY_{w}",
        ))

    # Price acceleration: second difference
    for w in [1, 5, 10]:
        factors.append((
            f"(Delta($close,{w}) - Ref(Delta($close,{w}),{w})) / ($close + 1e-8)",
            f"HO_ACCEL_{w}",
        ))

    # Curvature: slope change
    for w in [10, 20, 60]:
        factors.append((
            f"Slope($close,{w}) - Ref(Slope($close,{w}),{w})",
            f"HO_CURVATURE_{w}",
        ))

    # Jerk: third derivative proxy
    for w in [5, 10]:
        accel = f"Delta($close,{w}) - Ref(Delta($close,{w}),{w})"
        factors.append((
            f"({accel}) - Ref({accel},{w})",
            f"HO_JERK_{w}",
        ))

    # Trend strength change: R-squared change
    for w in [20, 60]:
        factors.append((
            f"Rsquare($close,{w}) - Ref(Rsquare($close,{w}),{w})",
            f"HO_RSQDELTA_{w}",
        ))

    # Volatility acceleration
    for w in [10, 20]:
        factors.append((
            f"Std($close/Ref($close,1)-1, {w}) - Ref(Std($close/Ref($close,1)-1, {w}), {w})",
            f"HO_VOLACCEL_{w}",
        ))

    return factors


# ===========================================================================
# NEW GROUPS 18-29 — genuinely distinct concepts not covered above
# ===========================================================================


# ---------------------------------------------------------------------------
# Group 18: True Range & ATR (~18 factors)
# True Range accounts for overnight gaps; H-L alone does not.
# ---------------------------------------------------------------------------

def _tr_expr() -> str:
    """True Range: max(H-L, |H-prevC|, |L-prevC|) using nested If."""
    a = "$high-$low"
    b = "Abs($high-Ref($close,1))"
    c = "Abs($low-Ref($close,1))"
    return f"If({a}>{b}, If({a}>{c}, {a}, {c}), If({b}>{c}, {b}, {c}))"


def atr_factors() -> list[tuple[str, str]]:
    """Average True Range and derivatives."""
    tr = _tr_expr()
    factors = []

    # ATR at multiple windows
    for w in [5, 10, 14, 20, 60]:
        factors.append((f"Mean({tr}, {w})", f"ATR_{w}"))

    # ATR relative to price (normalized ATR)
    for w in [14, 20, 60]:
        factors.append((f"Mean({tr}, {w}) / ($close + 1e-8)", f"ATR_NORM_{w}"))

    # ATR ratio: short / long (volatility regime via true range)
    for s, l in [(5, 20), (14, 60)]:
        factors.append((
            f"Mean({tr}, {s}) / (Mean({tr}, {l}) + 1e-8)",
            f"ATR_RATIO_{s}_{l}",
        ))

    # Daily return normalized by ATR (how much of the "capacity" was used)
    for w in [14, 20]:
        factors.append((
            f"Abs($close - Ref($close,1)) / (Mean({tr}, {w}) + 1e-8)",
            f"ATR_RETCAP_{w}",
        ))

    # ATR expansion/contraction: current TR / ATR
    for w in [14, 20]:
        factors.append((
            f"({tr}) / (Mean({tr}, {w}) + 1e-8)",
            f"ATR_EXPANSION_{w}",
        ))

    # True Range skewness — asymmetry of range distribution
    factors.append((f"Skew({tr}, 20)", "ATR_SKEW_20"))

    return factors


# ---------------------------------------------------------------------------
# Group 19: Price Efficiency / Kaufman (~14 factors)
# Measures how "direct" price movement is vs total path traversed.
# ---------------------------------------------------------------------------

def efficiency_factors() -> list[tuple[str, str]]:
    """Kaufman efficiency ratio and path-based features."""
    factors = []

    # Efficiency ratio: |net displacement| / total path length
    for w in [5, 10, 20, 60, 120]:
        factors.append((
            f"Abs($close - Ref($close,{w})) / (Sum(Abs($close - Ref($close,1)), {w}) + 1e-8)",
            f"EFF_RATIO_{w}",
        ))

    # Signed efficiency: direction-preserving version
    for w in [10, 20, 60]:
        factors.append((
            f"($close - Ref($close,{w})) / (Sum(Abs($close - Ref($close,1)), {w}) + 1e-8)",
            f"EFF_SIGNED_{w}",
        ))

    # Path length relative to price (total "distance" traveled)
    for w in [10, 20, 60]:
        factors.append((
            f"Sum(Abs($close - Ref($close,1)), {w}) / ($close + 1e-8)",
            f"EFF_PATHLEN_{w}",
        ))

    # Efficiency change: is the trend becoming more/less efficient?
    for w in [10, 20]:
        eff = f"Abs($close - Ref($close,{w})) / (Sum(Abs($close - Ref($close,1)), {w}) + 1e-8)"
        factors.append((
            f"({eff}) - Ref({eff}, {w})",
            f"EFF_DELTA_{w}",
        ))

    return factors


# ---------------------------------------------------------------------------
# Group 20: Open Price Analysis (~16 factors)
# $open is underutilized. Captures overnight vs intraday dynamics.
# ---------------------------------------------------------------------------

def open_price_factors() -> list[tuple[str, str]]:
    """Features based on open price and intraday structure."""
    factors = []

    # Open position in today's range (distinct from CLV which uses close)
    factors.append((
        "($open - $low) / ($high - $low + 1e-8)",
        "OPEN_RANGEPOS",
    ))

    # Intraday return: close / open - 1
    factors.append(("$close / $open - 1", "OPEN_INTRADAY_RET"))
    for w in [5, 10, 20]:
        factors.append((
            f"Mean($close/$open - 1, {w})",
            f"OPEN_INTRADAY_MA_{w}",
        ))

    # Overnight return stability (volatility of gaps)
    for w in [10, 20, 60]:
        factors.append((
            f"Std($open/Ref($close,1)-1, {w})",
            f"OPEN_GAPVOL_{w}",
        ))

    # Intraday return volatility
    for w in [10, 20]:
        factors.append((
            f"Std($close/$open-1, {w})",
            f"OPEN_INTRAVOL_{w}",
        ))

    # Overnight-Intraday correlation: are they aligned or opposing?
    for w in [20, 60]:
        factors.append((
            f"Corr($open/Ref($close,1)-1, $close/$open-1, {w})",
            f"OPEN_OI_CORR_{w}",
        ))

    # Gap fill tendency: does intraday move reverse the gap?
    # Sign(gap) * Sign(intraday) < 0 means gap filled
    for w in [10, 20]:
        factors.append((
            f"Count(Sign($open/Ref($close,1)-1) * Sign($close/$open-1) < 0, {w}) / {w}",
            f"OPEN_GAPFILL_{w}",
        ))

    return factors


# ---------------------------------------------------------------------------
# Group 21: Higher-Lag Serial Dependence (~14 factors)
# MR_AUTOCORR covers lag-1. Multi-lag reveals richer time structure.
# ---------------------------------------------------------------------------

def serial_dependence_factors() -> list[tuple[str, str]]:
    """Multi-lag autocorrelation and cross-serial features."""
    factors = []
    ret = "$close/Ref($close,1)-1"

    # Return autocorrelation at lag 2, 3, 5
    for lag in [2, 3, 5]:
        for w in [20, 60]:
            factors.append((
                f"Corr({ret}, Ref({ret},{lag}), {w})",
                f"SERIAL_AC{lag}_{w}",
            ))

    # Absolute return autocorrelation (volatility clustering)
    for lag in [1, 5]:
        for w in [20, 60]:
            factors.append((
                f"Corr(Abs({ret}), Ref(Abs({ret}),{lag}), {w})",
                f"SERIAL_ABSAC{lag}_{w}",
            ))

    # Volume-return lead/lag: does volume predict next-day return?
    for w in [20, 60]:
        factors.append((
            f"Corr(Ref($volume,1), {ret}, {w})",
            f"SERIAL_VOLLEAD_{w}",
        ))

    return factors


# ---------------------------------------------------------------------------
# Group 22: Conditional Statistics (~18 factors)
# Statistics computed conditional on market state (up/down, high/low volume).
# ---------------------------------------------------------------------------

def conditional_factors() -> list[tuple[str, str]]:
    """Return/volume stats conditional on market regime."""
    factors = []

    # Average volume on up days / average volume on down days
    for w in [20, 60]:
        up_vol = f"Mean(If($close>Ref($close,1), $volume, 0), {w})"
        dn_vol = f"Mean(If($close<Ref($close,1), $volume, 0), {w})"
        factors.append((
            f"({up_vol}) / ({dn_vol} + 1e-8)",
            f"COND_VOLASYM_{w}",
        ))

    # Average range on up days vs down days
    for w in [20, 60]:
        factors.append((
            f"Mean(If($close>Ref($close,1), $high-$low, 0), {w}) / (Mean(If($close<Ref($close,1), $high-$low, 0), {w}) + 1e-8)",
            f"COND_RANGEASYM_{w}",
        ))

    # Upside volatility / downside volatility (distinct from TAIL_RATIO which uses semi-variance)
    for w in [20, 60]:
        factors.append((
            f"Std(If($close>Ref($close,1), $close/Ref($close,1)-1, 0), {w}) / (Std(If($close<Ref($close,1), $close/Ref($close,1)-1, 0), {w}) + 1e-8)",
            f"COND_VOLSYM_{w}",
        ))

    # Return contribution from large moves (>1 std)
    for w in [20, 60]:
        factors.append((
            f"Sum(If(Abs($close/Ref($close,1)-1) > Std($close/Ref($close,1)-1,{w}), $close/Ref($close,1)-1, 0), {w}) / (Sum(Abs($close/Ref($close,1)-1), {w}) + 1e-8)",
            f"COND_BIGMOVE_CONTRIB_{w}",
        ))

    # Average return when volume > mean+std (event return)
    for w in [20, 60]:
        factors.append((
            f"Mean(If($volume > Mean($volume,{w})+Std($volume,{w}), $close/Ref($close,1)-1, 0), {w})",
            f"COND_EVENT_RET_{w}",
        ))

    # Up-day average return / down-day average return magnitude
    for w in [20, 60]:
        factors.append((
            f"Mean(If($close>Ref($close,1), $close/Ref($close,1)-1, 0), {w}) / (Abs(Mean(If($close<Ref($close,1), $close/Ref($close,1)-1, 0), {w})) + 1e-8)",
            f"COND_GAINLOSS_{w}",
        ))

    return factors


# ---------------------------------------------------------------------------
# Group 23: Volume Shape & Distribution (~14 factors)
# Stats about the distribution of volume itself, not return.
# ---------------------------------------------------------------------------

def volume_shape_factors() -> list[tuple[str, str]]:
    """Distributional shape of volume."""
    factors = []

    # Volume skewness
    for w in [20, 60, 120]:
        factors.append((f"Skew($volume, {w})", f"VSHAPE_SKEW_{w}"))

    # Volume kurtosis
    for w in [20, 60]:
        factors.append((f"Kurt($volume, {w})", f"VSHAPE_KURT_{w}"))

    # Volume concentration: peak day / average
    for w in [10, 20, 60]:
        factors.append((
            f"Max($volume, {w}) / (Mean($volume, {w}) + 1e-8)",
            f"VSHAPE_PEAK_{w}",
        ))

    # Volume coefficient of variation
    for w in [20, 60]:
        factors.append((
            f"Std($volume, {w}) / (Mean($volume, {w}) + 1e-8)",
            f"VSHAPE_CV_{w}",
        ))

    # Volume trend via regression residual
    for w in [20, 60]:
        factors.append((
            f"Resi($volume, {w}) / (Mean($volume, {w}) + 1e-8)",
            f"VSHAPE_RESI_{w}",
        ))

    return factors


# ---------------------------------------------------------------------------
# Group 24: Pivot Point & Support/Resistance (~14 factors)
# Classic technical analysis levels, not covered above.
# ---------------------------------------------------------------------------

def pivot_factors() -> list[tuple[str, str]]:
    """Pivot point system and support/resistance features."""
    factors = []

    # Classic pivot: (H + L + C) / 3  (previous day)
    pivot = "(Ref($high,1) + Ref($low,1) + Ref($close,1)) / 3"
    r1 = f"2*{pivot} - Ref($low,1)"
    s1 = f"2*{pivot} - Ref($high,1)"
    r2 = f"{pivot} + Ref($high,1) - Ref($low,1)"
    s2 = f"{pivot} - Ref($high,1) + Ref($low,1)"

    # Price distance from pivot levels (normalized by price)
    factors.append((f"($close - ({pivot})) / ($close + 1e-8)", "PIVOT_DIST"))
    factors.append((f"($close - ({r1})) / ($close + 1e-8)", "PIVOT_R1_DIST"))
    factors.append((f"($close - ({s1})) / ($close + 1e-8)", "PIVOT_S1_DIST"))
    factors.append((f"($close - ({r2})) / ($close + 1e-8)", "PIVOT_R2_DIST"))
    factors.append((f"($close - ({s2})) / ($close + 1e-8)", "PIVOT_S2_DIST"))

    # Rolling pivot (multi-day)
    for w in [5, 10, 20]:
        mp = f"(Max($high,{w}) + Min($low,{w}) + $close) / 3"
        factors.append((
            f"($close - ({mp})) / ($close + 1e-8)",
            f"PIVOT_ROLLING_{w}",
        ))

    # Count of closes above/below pivot in window
    for w in [10, 20]:
        factors.append((
            f"Count($close > ({pivot}), {w}) / {w}",
            f"PIVOT_ABOVE_{w}",
        ))

    # Pivot range: R1 - S1 normalized
    factors.append((
        f"(({r1}) - ({s1})) / ($close + 1e-8)",
        "PIVOT_RANGE",
    ))

    return factors


# ---------------------------------------------------------------------------
# Group 25: Detrended Oscillators (~14 factors)
# Remove trend to isolate cycle component. Distinct from regression residuals.
# ---------------------------------------------------------------------------

def detrended_factors() -> list[tuple[str, str]]:
    """Detrended price oscillator and cycle features."""
    factors = []

    # DPO: price relative to lagged moving average
    # DPO(N) = C - MA(N, shifted N/2+1 periods back)
    for w in [10, 20, 60]:
        half = w // 2 + 1
        factors.append((
            f"($close - Ref(Mean($close,{w}),{half})) / ($close + 1e-8)",
            f"DPO_{w}",
        ))

    # Percentage Price Oscillator: (EMA_fast - EMA_slow) / EMA_slow
    for fast, slow in [(5, 20), (10, 60), (20, 120)]:
        factors.append((
            f"(EMA($close,{fast}) - EMA($close,{slow})) / (EMA($close,{slow}) + 1e-8)",
            f"PPO_{fast}_{slow}",
        ))

    # Detrended volume: volume deviation from its own trend
    for w in [20, 60]:
        half = w // 2 + 1
        factors.append((
            f"($volume - Ref(Mean($volume,{w}),{half})) / (Mean($volume,{w}) + 1e-8)",
            f"DVO_{w}",
        ))

    # Cycle amplitude proxy: range of detrended price
    for w in [20, 60]:
        resi = f"Resi($close, {w})"
        factors.append((
            f"(Max({resi},{w}) - Min({resi},{w})) / ($close + 1e-8)",
            f"CYCLE_AMP_{w}",
        ))

    # Detrended RSI: RSI deviation from 50
    for w in [14, 20]:
        rsi = f"Mean(If($close>Ref($close,1), $close-Ref($close,1), 0), {w}) / (Mean(If($close>Ref($close,1), $close-Ref($close,1), 0), {w}) + Mean(If($close<Ref($close,1), Ref($close,1)-$close, 0), {w}) + 1e-8)"
        factors.append((f"({rsi}) - 0.5", f"DRSI_{w}"))

    return factors


# ---------------------------------------------------------------------------
# Group 26: Sign Change & Regime (~14 factors)
# Measures how often direction flips — captures choppiness vs trending.
# ---------------------------------------------------------------------------

def sign_change_factors() -> list[tuple[str, str]]:
    """Direction change frequency and regime persistence."""
    factors = []
    ret = "$close/Ref($close,1)-1"
    ret_lag = "Ref($close,1)/Ref($close,2)-1"

    # Direction change count: how many days return flips sign
    for w in [5, 10, 20, 60]:
        factors.append((
            f"Count(({ret})*({ret_lag}) < 0, {w}) / {w}",
            f"SIGN_FLIPRATE_{w}",
        ))

    # Choppiness index proxy: higher = choppier (range-based)
    for w in [14, 20]:
        factors.append((
            f"Sum(Abs($close-Ref($close,1)), {w}) / (Max($high,{w}) - Min($low,{w}) + 1e-8)",
            f"SIGN_CHOP_{w}",
        ))

    # Direction bias: (count_up - count_down) / N
    for w in [10, 20, 60]:
        factors.append((
            f"(Count($close>Ref($close,1), {w}) - Count($close<Ref($close,1), {w})) / {w}",
            f"SIGN_BIAS_{w}",
        ))

    # Return sign persistence: are same-sign streaks common?
    # Approximated by: 1 - 2*fliprate
    # (redundant with fliprate algebraically, so use absolute-return-weighted variant)
    for w in [10, 20]:
        factors.append((
            f"Sum(If(({ret})*({ret_lag})>0, Abs({ret}), 0), {w}) / (Sum(Abs({ret}), {w}) + 1e-8)",
            f"SIGN_PERSIST_{w}",
        ))

    return factors


# ---------------------------------------------------------------------------
# Group 27: Feature Interaction (~18 factors)
# Non-linear combinations with known economic meaning.
# ---------------------------------------------------------------------------

def interaction_factors() -> list[tuple[str, str]]:
    """Economically motivated feature interactions."""
    factors = []

    # Return × Sign(volume change) — volume confirms/contradicts direction
    for w in [5, 10, 20]:
        factors.append((
            f"Mean(($close/Ref($close,1)-1) * Sign($volume - Ref($volume,1)), {w})",
            f"IX_RETVOLSIGN_{w}",
        ))

    # Close-to-range ratio: how much of the range did the return capture?
    # |close - open| / (high - low)  — distinct from BODY_RATIO which uses abs(c-o)/(h-l)
    # Use return version: |close - prev_close| / true_range
    tr = _tr_expr()
    factors.append((
        f"Abs($close - Ref($close,1)) / ({tr} + 1e-8)",
        "IX_RETEFF",
    ))
    for w in [5, 10, 20]:
        factors.append((
            f"Mean(Abs($close - Ref($close,1)) / ({tr} + 1e-8), {w})",
            f"IX_RETEFF_MA_{w}",
        ))

    # Range × Volume = dollar activity / "energy" measure
    for w in [5, 10, 20]:
        factors.append((
            f"Mean(($high-$low)*$volume, {w}) / (Mean(($high-$low)*$volume, 60) + 1e-8)",
            f"IX_ENERGY_{w}",
        ))

    # Volatility × Trend strength: strong trend + low vol = quality trend
    for w in [20, 60]:
        factors.append((
            f"Rsquare($close,{w}) / (Std($close/Ref($close,1)-1,{w}) + 1e-8)",
            f"IX_TRENDQUAL_{w}",
        ))

    # Momentum × Volume ratio: volume-confirmed momentum
    for w in [10, 20]:
        factors.append((
            f"($close/Ref($close,{w})-1) * Mean($volume,{w}) / (Mean($volume,60) + 1e-8)",
            f"IX_MOMVOL_{w}",
        ))

    return factors


# ---------------------------------------------------------------------------
# Group 28: Log-Space Features (~14 factors)
# Log transforms compress scale and normalize heavy-tailed distributions.
# ---------------------------------------------------------------------------

def log_features() -> list[tuple[str, str]]:
    """Log-domain volume and range features."""
    factors = []

    # Log volume z-score
    for w in [20, 60, 120]:
        factors.append((
            f"(Log($volume) - Mean(Log($volume), {w})) / (Std(Log($volume), {w}) + 1e-8)",
            f"LOG_VOLZ_{w}",
        ))

    # Log volume slope — growth rate of "log activity"
    for w in [10, 20, 60]:
        factors.append((f"Slope(Log($volume), {w})", f"LOG_VOLSLOPE_{w}"))

    # Log range z-score (MICRO_HLSPREAD already has Mean(Log(H/L),N); this adds the z-score)
    for w in [20, 60]:
        factors.append((
            f"(Log($high/$low) - Mean(Log($high/$low), {w})) / (Std(Log($high/$low), {w}) + 1e-8)",
            f"LOG_RANGEZ_{w}",
        ))

    # Log range slope (trend in intraday range)
    for w in [20, 60]:
        factors.append((f"Slope(Log($high/$low), {w})", f"LOG_RANGESLOPE_{w}"))

    # Log range skewness
    for w in [20, 60]:
        factors.append((f"Skew(Log($high/$low), {w})", f"LOG_RANGESKEW_{w}"))

    return factors


# ---------------------------------------------------------------------------
# Group 29: Dispersion & Asymmetry (~16 factors)
# Shape characteristics of returns that are NOT covered by skew/kurt.
# ---------------------------------------------------------------------------

def dispersion_factors() -> list[tuple[str, str]]:
    """Return dispersion, inter-quantile ranges, and asymmetry."""
    factors = []

    # Return range: max_daily_ret - min_daily_ret in window
    for w in [20, 60]:
        factors.append((
            f"Max($close/Ref($close,1)-1, {w}) - Min($close/Ref($close,1)-1, {w})",
            f"DISP_RETRANGE_{w}",
        ))

    # Inter-quartile range of returns
    for w in [20, 60]:
        factors.append((
            f"Quantile($close/Ref($close,1)-1, {w}, 0.75) - Quantile($close/Ref($close,1)-1, {w}, 0.25)",
            f"DISP_IQR_{w}",
        ))

    # Mean Absolute Deviation of returns (distinct from Std: more robust)
    for w in [10, 20, 60]:
        factors.append((
            f"Mad($close/Ref($close,1)-1, {w})",
            f"DISP_MAD_{w}",
        ))

    # Range asymmetry: (H - C) / (C - L) — upper range vs lower range
    factors.append((
        "($high - $close) / ($close - $low + 1e-8)",
        "DISP_RANGEASYM",
    ))
    for w in [5, 10, 20]:
        factors.append((
            f"Mean(($high - $close) / ($close - $low + 1e-8), {w})",
            f"DISP_RANGEASYM_MA_{w}",
        ))

    # Positive return / negative return count ratio (skew proxy that's different from skew)
    for w in [20, 60]:
        factors.append((
            f"Count($close/Ref($close,1)-1>0, {w}) / (Count($close/Ref($close,1)-1<0, {w}) + 1e-8)",
            f"DISP_UPDOWNRATIO_{w}",
        ))

    return factors


# ---------------------------------------------------------------------------
# Aggregate: collect all factor groups
# ---------------------------------------------------------------------------

def get_all_extended_factors() -> list[tuple[str, str]]:
    """Return all extended factor (expression, name) tuples."""
    all_factors = []
    for group_fn in _ALL_GROUPS.values():
        all_factors.extend(group_fn())
    return all_factors


def get_factor_groups() -> dict[str, list[tuple[str, str]]]:
    """Return factors organized by group name."""
    return {name: fn() for name, fn in _ALL_GROUPS.items()}


_ALL_GROUPS: dict[str, callable] = {
    "momentum": momentum_factors,
    "volatility": volatility_factors,
    "volume": volume_factors,
    "technical": technical_factors,
    "microstructure": microstructure_factors,
    "trend": trend_factors,
    "cross_sectional": cross_sectional_factors,
    "multiscale": multiscale_factors,
    "distribution": distribution_factors,
    "reversal": reversal_factors,
    "tail_risk": tail_risk_factors,
    "price_pattern": price_pattern_factors,
    "volume_price": volume_price_factors,
    "mean_reversion": mean_reversion_factors,
    "relative_strength": relative_strength_factors,
    "weighted_momentum": weighted_momentum_factors,
    "higher_order": higher_order_factors,
    "atr": atr_factors,
    "efficiency": efficiency_factors,
    "open_price": open_price_factors,
    "serial_dependence": serial_dependence_factors,
    "conditional": conditional_factors,
    "volume_shape": volume_shape_factors,
    "pivot": pivot_factors,
    "detrended": detrended_factors,
    "sign_change": sign_change_factors,
    "interaction": interaction_factors,
    "log_features": log_features,
    "dispersion": dispersion_factors,
}
