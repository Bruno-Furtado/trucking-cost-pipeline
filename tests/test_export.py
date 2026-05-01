import json
from datetime import datetime

from pipeline.export import write_manifest


def test_manifest_has_required_keys(tmp_path):
    stats = {
        "price_timeline": {"rows": 10, "min_date": "2024-01-01", "max_date": "2024-12-31"},
        "wti_diesel_lag": {"rows": 91, "optimal_lag_days": 14, "optimal_correlation": 0.95},
        "fuel_shocks": {"rows": 5, "top_event_pct_change": 30.0, "top_event_date": "2024-06-01"},
    }
    output_file = write_manifest(stats, data_through="2024-12-31", output_dir=str(tmp_path))

    manifest = json.loads(output_file.read_text())

    assert set(manifest.keys()) == {"generated_at", "data_through", "tables"}
    assert manifest["data_through"] == "2024-12-31"
    assert manifest["tables"] == stats


def test_manifest_generated_at_is_iso_utc(tmp_path):
    output_file = write_manifest({}, data_through="2024-12-31", output_dir=str(tmp_path))

    manifest = json.loads(output_file.read_text())
    generated_at = manifest["generated_at"]

    assert generated_at.endswith("Z")
    # Sem o sufixo Z deve ser parseavel como ISO 8601.
    datetime.fromisoformat(generated_at[:-1])
