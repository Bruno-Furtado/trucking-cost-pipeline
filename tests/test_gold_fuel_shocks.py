from datetime import date, timedelta

from pyspark.sql.types import DateType, DoubleType, StructField, StructType

from pipeline.gold import _build_fuel_shocks_logic


TIMELINE_SCHEMA = StructType([
    StructField("date", DateType(), False),
    StructField("wti_usd_bbl", DoubleType(), True),
    StructField("diesel_usd_gal", DoubleType(), True),
    StructField("diesel_week_ending", DateType(), True),
])


def _make_timeline(spark, prices, base_date=date(2024, 1, 1)):
    """prices: list of (wti, diesel) tuples, one per day."""
    rows = []
    for i, (wti, diesel) in enumerate(prices):
        d = base_date + timedelta(days=i)
        rows.append((d, float(wti), float(diesel), d))
    return spark.createDataFrame(rows, TIMELINE_SCHEMA)


def test_nms_suppresses_neighbors(spark):
    # Pico forte no dia 50 (+30%) com vizinho menor no dia 53 (+28%).
    # NMS dentro de +/-14 dias deve preservar apenas o dia 50.
    prices = []
    for i in range(100):
        if i < 50:
            diesel = 1.0
        elif i <= 52:
            diesel = 1.30
        elif i == 53:
            diesel = 1.28
        else:
            diesel = 1.0
        prices.append((80.0, diesel))

    df = _make_timeline(spark, prices)
    result = _build_fuel_shocks_logic(df).orderBy("rank").collect()

    assert len(result) == 1
    assert result[0]["rank"] == 1
    assert result[0]["peak_date"] == date(2024, 1, 1) + timedelta(days=50)
    peak_dates = {r["peak_date"] for r in result}
    assert (date(2024, 1, 1) + timedelta(days=53)) not in peak_dates


def test_returns_distinct_events(spark):
    # Dois picos reais: +20% no dia 50 e +25% no dia 110 (separados por 60 dias).
    prices = []
    for i in range(150):
        if i < 50:
            diesel = 1.0
        elif i < 110:
            diesel = 1.20
        else:
            diesel = 1.50
        prices.append((80.0, diesel))

    df = _make_timeline(spark, prices)
    result = _build_fuel_shocks_logic(df).orderBy("rank").collect()

    assert len(result) == 2
    pct_values = sorted(round(r["pct_change_28d"], 4) for r in result)
    assert len(set(pct_values)) == 2

    peak_dates = {r["peak_date"] for r in result}
    assert (date(2024, 1, 1) + timedelta(days=50)) in peak_dates
    assert (date(2024, 1, 1) + timedelta(days=110)) in peak_dates


def test_handles_tied_pct_change(spark):
    # Forward fill semanal cria 4 dias consecutivos com pct_change identico.
    # Dedup deve manter apenas 1 entry pra esse evento.
    prices = []
    for i in range(90):
        if i < 50:
            diesel = 1.0
        elif i <= 53:
            diesel = 1.20
        else:
            diesel = 1.0
        prices.append((80.0, diesel))

    df = _make_timeline(spark, prices)
    result = _build_fuel_shocks_logic(df).collect()

    assert len(result) == 1
    assert result[0]["peak_date"] == date(2024, 1, 1) + timedelta(days=50)
