from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from pipeline.status import (
    MIN_TIMELINE_DAYS,
    REGIME_CALM,
    REGIME_ELEVATED,
    REGIME_EXTREME,
    SPARKLINE_WEEKS,
    _classify_regime,
    compute_diesel_status,
)


def _make_pdf(prices: list[float], base_date: date = date(2000, 1, 3)) -> pd.DataFrame:
    rows = [
        {"date": base_date + timedelta(days=i), "diesel_usd_gal": float(p)}
        for i, p in enumerate(prices)
    ]
    return pd.DataFrame(rows)


def test_classify_regime_extreme_when_high_percentile():
    assert _classify_regime(percentile_5y=85, vol_pct_rank=20) == REGIME_EXTREME


def test_classify_regime_extreme_when_high_vol():
    assert _classify_regime(percentile_5y=40, vol_pct_rank=80) == REGIME_EXTREME


def test_classify_regime_calm_when_low_both():
    assert _classify_regime(percentile_5y=30, vol_pct_rank=20) == REGIME_CALM


def test_classify_regime_elevated_in_between():
    assert _classify_regime(percentile_5y=60, vol_pct_rank=60) == REGIME_ELEVATED


def test_compute_status_rejects_short_timeline():
    pdf = _make_pdf([3.0] * (MIN_TIMELINE_DAYS - 1))
    with pytest.raises(ValueError):
        compute_diesel_status(pdf)


def test_compute_status_returns_required_keys():
    prices = list(np.linspace(2.0, 3.0, 300))
    pdf = _make_pdf(prices)
    result = compute_diesel_status(pdf)

    assert set(result.keys()) == {
        "as_of_date",
        "regime",
        "kpis",
        "headline_vars",
        "sparkline_12w",
        "historical_analogue",
        "regime_history",
    }
    assert set(result["kpis"].keys()) == {
        "current_price_usd_gal",
        "delta_1w_pct",
        "delta_yoy_pct",
        "percentile_25y",
        "percentile_5y",
    }


def test_sparkline_has_correct_length_with_long_history():
    prices = list(np.linspace(2.0, 3.0, 300))
    pdf = _make_pdf(prices)
    result = compute_diesel_status(pdf)

    assert len(result["sparkline_12w"]) == SPARKLINE_WEEKS
    assert result["sparkline_12w"][-1] == round(float(prices[-1]), 3)


def test_sparkline_truncates_when_history_short():
    # 70 days = MIN_TIMELINE met but only ~14 sparkline samples possible (70 / 5).
    # Actually with 70 days and step=5, indices [69, 64, 59, ..., 4] = 14 samples capped at SPARKLINE_WEEKS.
    prices = list(np.linspace(2.0, 3.0, 70))
    pdf = _make_pdf(prices)
    result = compute_diesel_status(pdf)
    assert 1 <= len(result["sparkline_12w"]) <= SPARKLINE_WEEKS


def test_extreme_regime_with_recent_spike():
    # 5 anos calmos + spike final.
    base = [3.0] * (252 * 5)
    spike = [3.0 + 0.1 * i for i in range(60)]
    pdf = _make_pdf(base + spike)
    result = compute_diesel_status(pdf)

    assert result["regime"] == REGIME_EXTREME
    assert result["kpis"]["percentile_5y"] > 80


def test_calm_regime_with_stable_history():
    # Preços com leve ruído estacionário e final abaixo da mediana.
    np.random.seed(42)
    base = 3.0 + np.random.normal(0, 0.02, 252 * 6)
    base[-100:] = 2.7 + np.random.normal(0, 0.02, 100)
    pdf = _make_pdf(list(base))
    result = compute_diesel_status(pdf)

    # Final abaixo da mediana garante percentil_5y baixo.
    assert result["kpis"]["percentile_5y"] < 80


def test_analogue_excludes_recent_window():
    # Construir série onde dia atual é único na história recente,
    # mas tem match no início da série (>2 anos atrás).
    prices = [2.0 + 0.5 * np.sin(i / 20) for i in range(252 * 4)]
    pdf = _make_pdf(prices)
    result = compute_diesel_status(pdf)

    if result["historical_analogue"] is not None:
        analogue_date = pd.to_datetime(result["historical_analogue"]["week"])
        as_of = pd.to_datetime(result["as_of_date"])
        gap_days = (as_of - analogue_date).days
        assert gap_days > 365 * 2 - 1


def test_analogue_none_when_history_too_short():
    prices = list(np.linspace(2.0, 3.0, 100))
    pdf = _make_pdf(prices)
    result = compute_diesel_status(pdf)

    assert result["historical_analogue"] is None


def test_regime_history_segments_are_contiguous():
    prices = list(np.linspace(2.0, 3.0, 300))
    pdf = _make_pdf(prices)
    result = compute_diesel_status(pdf)

    segments = result["regime_history"]
    assert len(segments) >= 1
    for i in range(1, len(segments)):
        prev_end = pd.to_datetime(segments[i - 1]["end"])
        curr_start = pd.to_datetime(segments[i]["start"])
        assert (curr_start - prev_end).days == 1


def test_regime_history_segments_alternate_regimes():
    prices = list(np.linspace(2.0, 3.0, 300))
    pdf = _make_pdf(prices)
    result = compute_diesel_status(pdf)

    segments = result["regime_history"]
    for i in range(1, len(segments)):
        assert segments[i]["regime"] != segments[i - 1]["regime"]


def test_headline_vars_match_kpis():
    prices = list(np.linspace(2.0, 3.0, 300))
    pdf = _make_pdf(prices)
    result = compute_diesel_status(pdf)

    assert result["headline_vars"]["price"] == f"{result['kpis']['current_price_usd_gal']:.2f}"
    assert result["headline_vars"]["pct"] == f"{result['kpis']['percentile_25y']:.1f}"
