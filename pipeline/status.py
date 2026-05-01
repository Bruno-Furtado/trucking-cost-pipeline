# STATUS STRATEGY: distill the price_timeline into a single "live state" snapshot.
# Output is dashboard-shaped: regime classification, KPIs with deltas, 12-week sparkline,
# closest historical analogue, and a segmented regime history for chart bands.
# All computation runs in pandas/numpy (no Spark) so logic is testable in isolation.

from typing import Optional

import numpy as np
import pandas as pd

REGIME_CALM = "calm"
REGIME_ELEVATED = "elevated"
REGIME_EXTREME = "extreme"

ROLLING_VOL_DAYS = 30
PERCENTILE_5Y_DAYS = 252 * 5
SPARKLINE_WEEKS = 12
SPARKLINE_STEP_DAYS = 5
ANALOGUE_EXCLUSION_DAYS = 365 * 2
MIN_TIMELINE_DAYS = 60


def _classify_regime(percentile_5y: float, vol_pct_rank: float) -> str:
    if percentile_5y > 80 or vol_pct_rank > 75:
        return REGIME_EXTREME
    if percentile_5y < 50 and vol_pct_rank < 50:
        return REGIME_CALM
    return REGIME_ELEVATED


def _select_context_clause_key(regime: str, percentile_25y: float, delta_yoy_pct: float, vol_pct_rank: float) -> str:
    if regime == REGIME_EXTREME and vol_pct_rank > 90:
        return "extreme_volatility"
    if regime == REGIME_EXTREME and percentile_25y > 95:
        return "extreme_high"
    if regime == REGIME_EXTREME:
        return "extreme_general"
    if regime == REGIME_ELEVATED and delta_yoy_pct > 20:
        return "elevated_rising"
    if regime == REGIME_ELEVATED and delta_yoy_pct < -20:
        return "elevated_falling"
    if regime == REGIME_ELEVATED:
        return "elevated_general"
    if regime == REGIME_CALM and percentile_25y < 25:
        return "calm_low"
    return "calm_general"


def _date_to_narrative_key(d: str) -> str:
    year = d[:4]
    month_year = d[:7]
    if year == "2008":
        return "analogue_2008_crisis"
    if year == "2005":
        return "analogue_2005_katrina"
    if month_year in ("2022-02", "2022-03", "2022-04", "2022-05", "2022-06", "2022-07"):
        return "analogue_2022_invasion"
    if year == "2020":
        return "analogue_2020_covid"
    if year == "2009":
        return "analogue_2009_recovery"
    if year in ("2014", "2015", "2016"):
        return "analogue_2014_oil_crash"
    return "analogue_generic"


def _zscore(values: np.ndarray, target: float) -> tuple[np.ndarray, float]:
    valid = values[~np.isnan(values)]
    mean = valid.mean()
    std = valid.std()
    if std == 0:
        return values - mean, target - mean
    return (values - mean) / std, (target - mean) / std


def _find_historical_analogue(
    pdf: pd.DataFrame,
    diesel: np.ndarray,
    rolling_vol: np.ndarray,
    current_price: float,
    current_vol: float,
) -> Optional[dict]:
    n = len(diesel)
    cutoff_idx = n - ANALOGUE_EXCLUSION_DAYS
    if cutoff_idx <= 28:
        return None

    momentum = np.full(n, np.nan)
    momentum[28:] = (diesel[28:] - diesel[:-28]) / diesel[:-28] * 100
    current_momentum = momentum[-1]
    if np.isnan(current_momentum):
        return None

    candidate_price = diesel[:cutoff_idx]
    candidate_vol = rolling_vol[:cutoff_idx]
    candidate_momentum = momentum[:cutoff_idx]

    norm_price, target_price = _zscore(candidate_price, current_price)
    norm_vol, target_vol = _zscore(candidate_vol, current_vol)
    norm_momentum, target_momentum = _zscore(candidate_momentum, current_momentum)

    valid_mask = (~np.isnan(candidate_vol)) & (~np.isnan(candidate_momentum))
    if not valid_mask.any():
        return None

    distances = np.sqrt(
        (norm_price - target_price) ** 2
        + (norm_vol - target_vol) ** 2
        + (norm_momentum - target_momentum) ** 2
    )
    distances = np.where(valid_mask, distances, np.inf)

    best_idx = int(np.argmin(distances))
    best_distance = float(distances[best_idx])
    similarity = 1.0 / (1.0 + best_distance)

    best_date = pdf["date"].iloc[best_idx].strftime("%Y-%m-%d")

    return {
        "week": best_date,
        "price_then": round(float(diesel[best_idx]), 3),
        "similarity_score": round(similarity, 3),
        "narrative_key": _date_to_narrative_key(best_date),
    }


def _compute_regime_history(pdf: pd.DataFrame, diesel: np.ndarray, rolling_vol: np.ndarray) -> list:
    n = len(diesel)
    if n < MIN_TIMELINE_DAYS:
        return []

    rolling_pct_5y = np.zeros(n)
    for i in range(n):
        start = max(0, i - PERCENTILE_5Y_DAYS + 1)
        window = diesel[start:i + 1]
        rolling_pct_5y[i] = (window < diesel[i]).sum() / len(window) * 100

    vol_ranks = np.full(n, 50.0)
    seen_vols: list[float] = []
    for i in range(n):
        if not np.isnan(rolling_vol[i]):
            seen_vols.append(float(rolling_vol[i]))
            arr = np.asarray(seen_vols)
            vol_ranks[i] = (arr < rolling_vol[i]).sum() / len(arr) * 100

    regimes = [_classify_regime(rolling_pct_5y[i], vol_ranks[i]) for i in range(n)]

    segments = []
    seg_start = 0
    for i in range(1, n):
        if regimes[i] != regimes[seg_start]:
            segments.append({
                "start": pdf["date"].iloc[seg_start].strftime("%Y-%m-%d"),
                "end": pdf["date"].iloc[i - 1].strftime("%Y-%m-%d"),
                "regime": regimes[seg_start],
            })
            seg_start = i
    segments.append({
        "start": pdf["date"].iloc[seg_start].strftime("%Y-%m-%d"),
        "end": pdf["date"].iloc[n - 1].strftime("%Y-%m-%d"),
        "regime": regimes[seg_start],
    })
    return segments


def compute_diesel_status(pdf: pd.DataFrame) -> dict:
    """
    Args:
        pdf: pandas DataFrame with at minimum 'date' (datelike) and 'diesel_usd_gal' columns.
             Must be sorted ascending by date or sortable.
    Returns:
        dict matching the diesel_status.json schema.
    """
    if "date" not in pdf.columns or "diesel_usd_gal" not in pdf.columns:
        raise ValueError("pdf must include 'date' and 'diesel_usd_gal' columns")

    pdf = pdf.copy()
    pdf["date"] = pd.to_datetime(pdf["date"])
    pdf = pdf.sort_values("date").reset_index(drop=True)
    n = len(pdf)
    if n < MIN_TIMELINE_DAYS:
        raise ValueError(f"timeline has {n} rows, minimum {MIN_TIMELINE_DAYS} required")

    diesel = pdf["diesel_usd_gal"].astype(float).to_numpy()

    daily_returns = np.zeros(n)
    daily_returns[1:] = (diesel[1:] - diesel[:-1]) / diesel[:-1] * 100

    rolling_vol = pd.Series(daily_returns).rolling(ROLLING_VOL_DAYS, min_periods=10).std().to_numpy()
    current_vol = float(rolling_vol[-1])
    valid_vols = rolling_vol[~np.isnan(rolling_vol)]
    vol_pct_rank = float((valid_vols < current_vol).sum() / len(valid_vols) * 100) if len(valid_vols) else 50.0

    current_price = float(diesel[-1])
    as_of_date = pdf["date"].iloc[-1].strftime("%Y-%m-%d")

    percentile_25y = float((diesel < current_price).sum() / n * 100)

    last_5y = diesel[max(0, n - PERCENTILE_5Y_DAYS):]
    percentile_5y = float((last_5y < current_price).sum() / len(last_5y) * 100)

    delta_1w_pct = float((diesel[-1] - diesel[-6]) / diesel[-6] * 100) if n >= 6 else 0.0
    delta_yoy_pct = float((diesel[-1] - diesel[-253]) / diesel[-253] * 100) if n >= 253 else 0.0

    regime = _classify_regime(percentile_5y, vol_pct_rank)
    context_clause_key = _select_context_clause_key(regime, percentile_25y, delta_yoy_pct, vol_pct_rank)

    sparkline_indices = []
    for i in range(SPARKLINE_WEEKS):
        idx = n - 1 - i * SPARKLINE_STEP_DAYS
        if idx >= 0:
            sparkline_indices.append(idx)
    sparkline_indices.reverse()
    sparkline_12w = [round(float(diesel[i]), 3) for i in sparkline_indices]

    analogue = _find_historical_analogue(pdf, diesel, rolling_vol, current_price, current_vol)
    regime_history = _compute_regime_history(pdf, diesel, rolling_vol)

    headline_vars = {
        "price": f"{current_price:.2f}",
        "pct": f"{percentile_25y:.1f}",
        "yoy": f"{delta_yoy_pct:+.0f}",
        "context_clause_key": context_clause_key,
    }

    return {
        "as_of_date": as_of_date,
        "regime": regime,
        "kpis": {
            "current_price_usd_gal": round(current_price, 3),
            "delta_1w_pct": round(delta_1w_pct, 2),
            "delta_yoy_pct": round(delta_yoy_pct, 1),
            "percentile_25y": round(percentile_25y, 1),
            "percentile_5y": round(percentile_5y, 1),
        },
        "headline_vars": headline_vars,
        "sparkline_12w": sparkline_12w,
        "historical_analogue": analogue,
        "regime_history": regime_history,
    }
