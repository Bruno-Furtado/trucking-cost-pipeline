# trucking-cost-pipeline

PySpark + Delta Lake cost intelligence pipeline for the US trucking industry.
Ingests public sources (Yahoo Finance WTI, EIA Diesel, FMCSA Census) through a medallion architecture (bronze/silver/gold).

## Quick start

```bash
docker compose build
docker compose run --rm pipeline python -m pipeline.spark_session
```
