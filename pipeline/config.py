"""Centralized config for paths and constants."""

import os

DATA_ROOT = os.environ.get("DATA_ROOT", "/data")

BRONZE_PATH = f"{DATA_ROOT}/bronze"
SILVER_PATH = f"{DATA_ROOT}/silver"
GOLD_PATH = f"{DATA_ROOT}/gold"
EXPORTS_PATH = f"{DATA_ROOT}/exports"

WTI_BRONZE_PATH = f"{BRONZE_PATH}/wti_daily"
EIA_DIESEL_BRONZE_PATH = f"{BRONZE_PATH}/diesel_weekly"

WTI_SILVER_PATH = f"{SILVER_PATH}/wti_daily"
EIA_DIESEL_SILVER_PATH = f"{SILVER_PATH}/diesel_weekly"

PRICE_TIMELINE_GOLD_PATH = f"{GOLD_PATH}/price_timeline"
WTI_DIESEL_LAG_GOLD_PATH = f"{GOLD_PATH}/wti_diesel_lag"
FUEL_SHOCKS_GOLD_PATH = f"{GOLD_PATH}/fuel_shocks"

CORRELATION_LAG_MIN = 0
CORRELATION_LAG_MAX = 90

SHOCK_WINDOW_DAYS = 28
SHOCK_SUPPRESSION_DAYS = 14
SHOCK_TOP_N = 10

WTI_TICKER = "CL=F"

# EIA API config
EIA_API_BASE_URL = "https://api.eia.gov/v2/petroleum/pri/gnd/data/"
EIA_DIESEL_PRODUCT = "EPD2D"  # No. 2 Ultra-Low Sulfur Diesel
EIA_DIESEL_AREA = "NUS"  # US national average
EIA_PAGE_SIZE = 5000
