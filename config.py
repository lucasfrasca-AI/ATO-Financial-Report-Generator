"""
config.py — All settings for the ATO Year-End Report Generator.
Values are loaded from .env where applicable; everything else is a module-level constant.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------
GEMINI_API_KEY: str | None = os.getenv("GEMINI_API_KEY")

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------
GENERATION_MODEL = "gemini-2.5-flash"   # narrative generation (Node 5)
QUALITY_MODEL = "gemini-2.5-flash"      # quality gate (Node 6)

# ---------------------------------------------------------------------------
# Entity & Report
# ---------------------------------------------------------------------------
ENTITY_TYPE = "pty_ltd"          # pty_ltd | trust | partnership | sole_trader
TAX_YEAR_END = "30_june"
REPORTING_CURRENCY = "AUD"
ROUNDING = "nearest_dollar"      # nearest_dollar | two_decimal_places

# ---------------------------------------------------------------------------
# Sections
# ---------------------------------------------------------------------------
INCLUDE_INCOME_STATEMENT = True
INCLUDE_BALANCE_SHEET = True
INCLUDE_CASH_FLOW = True
INCLUDE_NOTES = True
INCLUDE_RATIOS = True
INCLUDE_ATO_BENCHMARKS = False   # Requires INDUSTRY_CODE
INCLUDE_COMPARATIVE_YEAR = True
INCLUDE_VARIANCE_ANALYSIS = True

# ---------------------------------------------------------------------------
# Tax Rates
# ---------------------------------------------------------------------------
COMPANY_TAX_RATE_BASE = 0.30
COMPANY_TAX_RATE_SBE = 0.25
SBE_TURNOVER_THRESHOLD = 50_000_000
GST_RATE = 0.10

# ---------------------------------------------------------------------------
# ATO Benchmarks (optional)
# ---------------------------------------------------------------------------
INDUSTRY_CODE: str | None = None
INDUSTRY_NAME: str | None = None

# ---------------------------------------------------------------------------
# Presidio
# ---------------------------------------------------------------------------
REDACTION_CONFIDENCE_THRESHOLD = 0.75
SHOW_FULL_TOKEN_MAP_IN_TERMINAL = False
PRESERVE_REGULATORY_ORGS = True

# ---------------------------------------------------------------------------
# Custom account mapping overrides (take priority over XERO_TO_ATO_MAP)
# ---------------------------------------------------------------------------
CUSTOM_ACCOUNT_MAPPINGS: dict[str, str] = {}

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
INPUT_FOLDER = "./input"
TEMPLATES_FOLDER = "./templates"
EXAMPLES_FOLDER = "./examples"
OUTPUT_FOLDER = "./outputs"
DATA_FOLDER = "./data"
