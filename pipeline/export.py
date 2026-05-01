# EXPORT STRATEGY: serialize gold Delta tables as JSON Array files for static consumption.
# Output is consumer-shaped: dates as ISO strings, single file per table, minified.

import json
from datetime import datetime, timezone
from pathlib import Path

from pipeline.config import (
    EXPORTS_PATH,
    PRICE_TIMELINE_GOLD_PATH,
    WTI_DIESEL_LAG_GOLD_PATH,
    FUEL_SHOCKS_GOLD_PATH,
)
from pipeline.spark_session import get_spark


def _write_json_array(records: list, output_file: Path) -> None:
    """Write a list of dicts as minified JSON Array."""
    output_file.write_text(json.dumps(records, separators=(",", ":")))


def export_price_timeline(spark) -> dict:
    df = spark.read.format("delta").load(PRICE_TIMELINE_GOLD_PATH).orderBy("date")
    pdf = df.toPandas()

    pdf["date"] = pdf["date"].astype(str)
    pdf["diesel_week_ending"] = pdf["diesel_week_ending"].astype(str)

    records = pdf.to_dict(orient="records")
    output_file = Path(EXPORTS_PATH) / "price_timeline.json"
    _write_json_array(records, output_file)

    stats = {
        "rows": len(records),
        "min_date": records[0]["date"],
        "max_date": records[-1]["date"],
    }
    print(f"Exported price_timeline: {stats}")
    return stats


def export_wti_diesel_lag(spark) -> dict:
    df = spark.read.format("delta").load(WTI_DIESEL_LAG_GOLD_PATH).orderBy("lag_days")
    pdf = df.toPandas()

    records = pdf.to_dict(orient="records")
    output_file = Path(EXPORTS_PATH) / "wti_diesel_lag.json"
    _write_json_array(records, output_file)

    optimal = next(r for r in records if r["is_optimal"])
    stats = {
        "rows": len(records),
        "optimal_lag_days": int(optimal["lag_days"]),
        "optimal_correlation": round(float(optimal["correlation"]), 4),
    }
    print(f"Exported wti_diesel_lag: {stats}")
    return stats


def export_fuel_shocks(spark) -> dict:
    df = spark.read.format("delta").load(FUEL_SHOCKS_GOLD_PATH).orderBy("rank")
    pdf = df.toPandas()

    pdf["peak_date"] = pdf["peak_date"].astype(str)
    pdf["period_start_date"] = pdf["period_start_date"].astype(str)

    records = pdf.to_dict(orient="records")
    output_file = Path(EXPORTS_PATH) / "fuel_shocks.json"
    _write_json_array(records, output_file)

    top = records[0]
    stats = {
        "rows": len(records),
        "top_event_pct_change": round(float(top["pct_change_28d"]), 2),
        "top_event_date": top["peak_date"],
    }
    print(f"Exported fuel_shocks: {stats}")
    return stats


def write_manifest(stats: dict, data_through: str, output_dir: str = EXPORTS_PATH) -> Path:
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "data_through": data_through,
        "tables": stats,
    }
    output_file = Path(output_dir) / "manifest.json"
    output_file.write_text(json.dumps(manifest, indent=2))
    print(f"Wrote manifest: {output_file}")
    return output_file


if __name__ == "__main__":
    Path(EXPORTS_PATH).mkdir(parents=True, exist_ok=True)
    spark = get_spark()

    stats = {
        "price_timeline": export_price_timeline(spark),
        "wti_diesel_lag": export_wti_diesel_lag(spark),
        "fuel_shocks": export_fuel_shocks(spark),
    }

    data_through = stats["price_timeline"]["max_date"]
    write_manifest(stats, data_through)

    spark.stop()
    print(f"All exports written to {EXPORTS_PATH}")
