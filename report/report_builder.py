"""
report_builder.py — Builds the numerical report structure from mapped accounts.

NO LLM. Deterministic Python only.

Responsibilities:
  - Parse account names and amounts from parsed tables
  - Apply ATO account mapping
  - Build Income Statement, Balance Sheet, and Key Ratios structures
  - Reconciliation check: net profit in P&L must equal retained earnings movement in BS
  - Fail loudly (raise ValueError) on reconciliation mismatch
  - Log every figure to sources.json with source document traceability

Output structure (dict):
  {
    "income_statement": {...},
    "balance_sheet": {...},
    "ratios": {...},
    "sources": [...],
    "entity": {...},
    "period": {...},
  }
"""

import json
import logging
import re
from pathlib import Path

from report.account_mapper import map_account

import config

logger = logging.getLogger(__name__)

# Amount parsing: handles $1,234.56 and (1,234.56) for negatives
_AMOUNT_RE = re.compile(r"^\(?([\d,]+(?:\.\d{0,2})?)\)?$")

# Aggregate/subtotal lines that appear in financial PDFs but should not be
# mapped as individual accounts (they double-count already-extracted line items)
_AGGREGATE_ROW_RE = re.compile(
    r'^\s*(?:total|sub.?total|grand total|'
    r'net\s+(?:assets|profit|loss|position|revenue)|'
    r'ebitda|ebit\b|'
    r'less\s*:\s*(?:total|operating|cost|expense)|'
    r'gross\s+profit\s*$|operating\s+profit|'
    r'profit\s+before|profit\s+after|loss\s+before|loss\s+after|'
    r'earnings\s+before)',
    re.IGNORECASE,
)


def _parse_amount(raw: str) -> float | None:
    """Parse a formatted amount string to float. Returns None if unparseable."""
    if not raw:
        return None
    raw = raw.strip().replace("$", "").replace(",", "")
    negative = raw.startswith("(") and raw.endswith(")")
    raw = raw.strip("()")
    try:
        value = float(raw)
        return -value if negative else value
    except ValueError:
        return None


def _round_amount(value: float) -> float | int:
    """Apply configured rounding."""
    if config.ROUNDING == "nearest_dollar":
        return round(value)
    return round(value, 2)


class ReportBuilder:
    """
    Builds the report structure from parsed documents.
    """

    def __init__(self, documents: list, run_log_path: str = None):
        """
        Args:
            documents:    List of parsed document dicts from ingestion layer.
            run_log_path: Path to run_log.txt for audit trail.
        """
        self.documents = documents
        self.run_log_path = run_log_path
        self.sources = []  # Traceability records
        self._mapping_cache = {}

    def build(self) -> dict:
        """
        Main build entry point. Returns the full report structure dict.
        Raises ValueError on reconciliation failure.
        """
        pl_docs = [d for d in self.documents if d["doc_type"] == "pl"]
        bs_docs = [d for d in self.documents if d["doc_type"] == "bs"]

        income_statement = self._build_income_statement(pl_docs)
        balance_sheet = self._build_balance_sheet(bs_docs)

        # Reconciliation check
        if income_statement and balance_sheet:
            self._reconcile(income_statement, balance_sheet)

        ratios = self._calculate_ratios(income_statement, balance_sheet)

        return {
            "income_statement": income_statement,
            "balance_sheet": balance_sheet,
            "ratios": ratios,
            "sources": self.sources,
        }

    # ------------------------------------------------------------------
    # Income Statement
    # ------------------------------------------------------------------

    def _build_income_statement(self, pl_docs: list) -> dict:
        if not pl_docs:
            logger.warning("No P&L document found — income statement will be empty")
            return {}

        revenue = {}
        cost_of_sales = {}
        expenses = {}

        for doc in pl_docs:
            for table in doc.get("tables", []):
                for row in table.get("rows", []):
                    account_name, amount = self._extract_account_row(row)
                    if account_name is None or amount is None:
                        continue

                    ato_category = self._map(account_name)
                    rounded = _round_amount(amount)
                    self._record_source(account_name, ato_category, rounded, doc["name"])

                    if ato_category.startswith("Revenue"):
                        revenue[ato_category] = revenue.get(ato_category, 0) + rounded
                    elif ato_category.startswith("Cost of Sales"):
                        cost_of_sales[ato_category] = cost_of_sales.get(ato_category, 0) + rounded
                    else:
                        expenses[ato_category] = expenses.get(ato_category, 0) + rounded

        total_revenue = sum(revenue.values())
        total_cogs = sum(cost_of_sales.values())
        gross_profit = total_revenue - total_cogs
        total_expenses = sum(expenses.values())
        net_profit = gross_profit - total_expenses

        return {
            "revenue": revenue,
            "total_revenue": total_revenue,
            "cost_of_sales": cost_of_sales,
            "total_cost_of_sales": total_cogs,
            "gross_profit": gross_profit,
            "expenses": expenses,
            "total_expenses": total_expenses,
            "net_profit": net_profit,
        }

    # ------------------------------------------------------------------
    # Balance Sheet
    # ------------------------------------------------------------------

    def _build_balance_sheet(self, bs_docs: list) -> dict:
        if not bs_docs:
            logger.warning("No Balance Sheet document found — balance sheet will be empty")
            return {}

        current_assets = {}
        non_current_assets = {}
        current_liabilities = {}
        non_current_liabilities = {}
        equity = {}

        for doc in bs_docs:
            for table in doc.get("tables", []):
                for row in table.get("rows", []):
                    account_name, amount = self._extract_account_row(row)
                    if account_name is None or amount is None:
                        continue

                    ato_category = self._map(account_name)
                    rounded = _round_amount(amount)
                    self._record_source(account_name, ato_category, rounded, doc["name"])

                    if "Current Assets" in ato_category and "Non-Current" not in ato_category:
                        current_assets[ato_category] = current_assets.get(ato_category, 0) + rounded
                    elif "Non-Current Assets" in ato_category:
                        non_current_assets[ato_category] = non_current_assets.get(ato_category, 0) + rounded
                    elif "Current Liabilities" in ato_category and "Non-Current" not in ato_category:
                        current_liabilities[ato_category] = current_liabilities.get(ato_category, 0) + rounded
                    elif "Non-Current Liabilities" in ato_category:
                        non_current_liabilities[ato_category] = non_current_liabilities.get(ato_category, 0) + rounded
                    elif "Equity" in ato_category:
                        equity[ato_category] = equity.get(ato_category, 0) + rounded

        total_current_assets = sum(current_assets.values())
        total_non_current_assets = sum(non_current_assets.values())
        total_assets = total_current_assets + total_non_current_assets

        total_current_liabilities = sum(current_liabilities.values())
        total_non_current_liabilities = sum(non_current_liabilities.values())
        total_liabilities = total_current_liabilities + total_non_current_liabilities

        total_equity = sum(equity.values())

        return {
            "current_assets": current_assets,
            "total_current_assets": total_current_assets,
            "non_current_assets": non_current_assets,
            "total_non_current_assets": total_non_current_assets,
            "total_assets": total_assets,
            "current_liabilities": current_liabilities,
            "total_current_liabilities": total_current_liabilities,
            "non_current_liabilities": non_current_liabilities,
            "total_non_current_liabilities": total_non_current_liabilities,
            "total_liabilities": total_liabilities,
            "equity": equity,
            "total_equity": total_equity,
        }

    # ------------------------------------------------------------------
    # Reconciliation
    # ------------------------------------------------------------------

    def _reconcile(self, income_statement: dict, balance_sheet: dict):
        """
        Net profit in Income Statement must equal Current Year Earnings in Equity.
        Raises ValueError with details on mismatch.
        """
        net_profit = income_statement.get("net_profit", 0)
        current_year_earnings = balance_sheet.get("equity", {}).get(
            "Equity - Current Year Profit", None
        )

        if current_year_earnings is None:
            logger.warning(
                "Reconciliation skipped: 'Equity - Current Year Profit' not found in balance sheet"
            )
            return

        diff = abs(net_profit - current_year_earnings)
        # Allow $1 rounding difference
        if diff > 1:
            raise ValueError(
                f"RECONCILIATION FAILURE: Net profit in Income Statement "
                f"({net_profit:,.2f}) does not match Current Year Earnings in Balance Sheet "
                f"({current_year_earnings:,.2f}). Difference: {diff:,.2f}. "
                "Check source documents for errors."
            )

        logger.info("Reconciliation passed: net profit = %s", net_profit)

    # ------------------------------------------------------------------
    # Ratios
    # ------------------------------------------------------------------

    def _calculate_ratios(self, income_statement: dict, balance_sheet: dict) -> dict:
        ratios = {}

        total_revenue = income_statement.get("total_revenue", 0)
        gross_profit = income_statement.get("gross_profit", 0)
        net_profit = income_statement.get("net_profit", 0)
        total_assets = balance_sheet.get("total_assets", 0)
        total_liabilities = balance_sheet.get("total_liabilities", 0)
        total_equity = balance_sheet.get("total_equity", 0)
        current_assets = balance_sheet.get("total_current_assets", 0)
        current_liabilities = balance_sheet.get("total_current_liabilities", 0)

        if total_revenue:
            ratios["gross_margin_pct"] = round(gross_profit / total_revenue * 100, 2)
            ratios["net_margin_pct"] = round(net_profit / total_revenue * 100, 2)

        if total_assets:
            ratios["return_on_assets_pct"] = round(net_profit / total_assets * 100, 2)

        if total_equity:
            ratios["return_on_equity_pct"] = round(net_profit / total_equity * 100, 2)

        if total_equity:
            ratios["debt_to_equity"] = round(total_liabilities / total_equity, 2)

        if current_liabilities:
            ratios["current_ratio"] = round(current_assets / current_liabilities, 2)

        return ratios

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _map(self, account_name: str) -> str:
        if account_name not in self._mapping_cache:
            ato_cat, method = map_account(account_name, run_log_path=self.run_log_path)
            self._mapping_cache[account_name] = ato_cat
        return self._mapping_cache[account_name]

    def _record_source(self, account_name: str, ato_category: str, amount: float, doc_name: str):
        self.sources.append({
            "account_name": account_name,
            "ato_category": ato_category,
            "amount": amount,
            "source_document": doc_name,
        })

    def _extract_account_row(self, row: dict) -> tuple:
        """
        Extract (account_name, amount) from a table row dict.
        Handles two formats:
          - Multi-column: {'Account': 'Cash', 'CY': '428,640'}
          - Single-column: {'HEADER': 'Cash at bank 312,840 428,640'}
            (common when pdfplumber collapses financial PDF columns into one cell)
        Returns (None, None) if extraction fails.
        """
        values = list(row.values())

        if len(values) == 1:
            # Single-value row: the cell contains account name + amounts in one string.
            # Parse it as text: find all formatted numbers, account = everything before
            # the first number, take the last number as current-year amount.
            cell = str(values[0]).strip()
            nums = re.findall(r'\(?\d{1,3}(?:,\d{3})+(?:\.\d{0,2})?\)?', cell)
            if not nums:
                return None, None
            first_match = re.search(r'\(?\d{1,3}(?:,\d{3})+(?:\.\d{0,2})?\)?', cell)
            account_name = cell[:first_match.start()].strip().rstrip('—–-. \t')
            if not account_name or len(account_name) < 3:
                return None, None
            if _AGGREGATE_ROW_RE.match(account_name):
                return None, None
            amount = _parse_amount(nums[-1])
            return account_name, amount

        if len(values) < 2:
            return None, None

        # Multi-column row: account name = first non-numeric string value
        account_name = None
        for v in values:
            v = str(v).strip()
            if v and not _parse_amount(v):
                account_name = v
                break

        if not account_name:
            return None, None

        # Amount = last parseable numeric value (rightmost = current year)
        amount = None
        for v in reversed(values):
            parsed = _parse_amount(str(v))
            if parsed is not None:
                amount = parsed
                break

        return account_name, amount


def build_report(documents: list, run_log_path: str = None) -> dict:
    """Convenience function: build the report from a list of parsed documents."""
    builder = ReportBuilder(documents, run_log_path=run_log_path)
    return builder.build()
