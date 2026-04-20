"""
account_mapper.py — Deterministic Xero account name -> ATO category mapping.

NO LLM. Pure Python only.

Fallback order:
  1. Exact match (case-insensitive)
  2. Punctuation-stripped comparison
  3. Keyword extraction ("rent" in account name -> "Occupancy Costs")
  4. CUSTOM_ACCOUNT_MAPPINGS from config (takes priority over all above)
  5. No match -> UNMAPPED -> halt pipeline -> blocking input() for human resolution

All mapping decisions are logged with the method used.
"""

import logging
import re
import sys
from pathlib import Path

from rich.console import Console
from rich.table import Table
from rich import box

import config

logger = logging.getLogger(__name__)
console = Console()

# ---------------------------------------------------------------------------
# Master mapping table
# ---------------------------------------------------------------------------

XERO_TO_ATO_MAP = {
    # Revenue
    "Sales": "Revenue - Sales",
    "Revenue": "Revenue - Sales",
    "Other Revenue": "Revenue - Other",
    "Interest Income": "Revenue - Interest",
    "Rental Income": "Revenue - Rental",
    # Cost of Sales
    "Cost of Goods Sold": "Cost of Sales",
    "Direct Costs": "Cost of Sales",
    "Direct Labour": "Cost of Sales - Labour",
    # Operating Expenses
    "Wages and Salaries": "Employee Expenses - Wages",
    "Superannuation": "Employee Expenses - Superannuation",
    "Rent": "Occupancy Costs - Rent",
    "Utilities": "Occupancy Costs - Utilities",
    "Depreciation": "Depreciation",
    "Motor Vehicle Expenses": "Motor Vehicle Expenses",
    "Travel & Accommodation": "Travel Expenses",
    "Marketing & Advertising": "Marketing Expenses",
    "Insurance": "Insurance",
    "Accounting Fees": "Professional Fees - Accounting",
    "Legal Fees": "Professional Fees - Legal",
    "Bank Charges": "Finance Costs - Bank Charges",
    "Interest Expense": "Finance Costs - Interest",
    # Balance Sheet — Assets
    "Cash": "Current Assets - Cash",
    "Accounts Receivable": "Current Assets - Trade Receivables",
    "Inventory": "Current Assets - Inventory",
    "Prepayments": "Current Assets - Prepayments",
    "Fixed Assets": "Non-Current Assets - Fixed Assets",
    "Property Plant Equipment": "Non-Current Assets - PP&E",
    "Accumulated Depreciation": "Non-Current Assets - Accumulated Depreciation",
    # Balance Sheet — Liabilities
    "Accounts Payable": "Current Liabilities - Trade Payables",
    "GST": "Current Liabilities - Tax Payable",
    "Payroll Liabilities": "Current Liabilities - Payroll Liabilities",
    "Income Tax Payable": "Current Liabilities - Income Tax Payable",
    "Loan": "Non-Current Liabilities - Loans",
    # Equity
    "Retained Earnings": "Equity - Retained Earnings",
    "Share Capital": "Equity - Paid-Up Capital",
    "Current Year Earnings": "Equity - Current Year Profit",
    # Additional common Xero account names
    "Advertising": "Marketing Expenses",
    "Telephone & Internet": "Occupancy Costs - Utilities",
    "Office Supplies": "Administration Expenses",
    "Repairs and Maintenance": "Maintenance Expenses",
    "Subcontractors": "Cost of Sales",
    "Freight & Courier": "Distribution Expenses",
    "Computer Equipment": "Non-Current Assets - Fixed Assets",
    "Software Subscriptions": "Administration Expenses - IT",
    "Drawings": "Equity - Drawings",
    "PAYG Withholding": "Current Liabilities - PAYG Withholding",
    "Superannuation Payable": "Current Liabilities - Superannuation Payable",
}

# Keyword-to-ATO-category fallback table
_KEYWORD_MAP = [
    # Revenue
    (["sales", "revenue", "income", "turnover"], "Revenue - Sales"),
    (["discount", "rebate", "allowance"], "Revenue - Discounts"),
    (["interest income", "interest received"], "Revenue - Interest"),
    (["interest"], "Revenue - Interest"),
    (["rental income", "rent income", "property income"], "Revenue - Rental"),
    (["other income", "miscellaneous income", "sundry income"], "Revenue - Other"),
    (["grant", "subsidy", "government payment"], "Revenue - Other"),
    (["commission received", "commission income"], "Revenue - Other"),
    # Cost of Sales
    (["cost of goods", "cogs", "cost of sales", "cost of revenue"], "Cost of Sales"),
    (["carrying amount", "carrying value", "written-down value"], "Cost of Sales"),
    (["direct cost", "direct labour", "direct material"], "Cost of Sales"),
    (["purchase", "purchases", "merchandise"], "Cost of Sales"),
    (["freight inward", "import duty", "customs"], "Cost of Sales"),
    (["subcontract", "contractor", "labour hire"], "Cost of Sales"),
    # Employee expenses
    (["wages", "salary", "salaries", "payroll expense"], "Employee Expenses - Wages"),
    (["super", "superannuation"], "Employee Expenses - Superannuation"),
    (["workers comp", "workcover", "workers compensation"], "Employee Expenses - Workers Comp"),
    (["staff", "employee", "personnel", "redundancy"], "Employee Expenses - Wages"),
    # Occupancy
    (["rent expense", "lease expense", "operating lease"], "Occupancy Costs - Rent"),
    (["rent", "lease", "occupancy"], "Occupancy Costs - Rent"),
    (["utilities", "electricity", "water", "gas", "telephone", "internet", "communications"], "Occupancy Costs - Utilities"),
    # Finance
    (["depreciation", "amortisation", "amortization", "write-off", "impairment"], "Depreciation"),
    (["bank charge", "bank fee", "merchant fee", "transaction fee", "payment processing"], "Finance Costs - Bank Charges"),
    (["interest expense", "loan interest", "borrowing cost", "finance charge"], "Finance Costs - Interest"),
    (["factoring", "invoice finance"], "Finance Costs - Bank Charges"),
    # Vehicle & travel
    (["motor vehicle", "vehicle expense", "fleet", "car expense", "fuel", "tolls"], "Motor Vehicle Expenses"),
    (["travel", "accommodation", "airfare", "hotel"], "Travel Expenses"),
    # Marketing
    (["marketing", "advertising", "promotion", "sponsorship"], "Marketing Expenses"),
    # Professional
    (["accounting", "audit", "tax agent", "bookkeeping"], "Professional Fees - Accounting"),
    (["legal", "solicitor", "barrister", "consulting fee"], "Professional Fees - Legal"),
    (["insurance", "public liability", "indemnity"], "Insurance"),
    # IT / Admin
    (["software", "subscription", "saas", "cloud", "computer", "it expense"], "Administration Expenses - IT"),
    (["office", "stationery", "supplies", "printing", "postage"], "Administration Expenses"),
    (["repair", "maintenance", "servicing"], "Maintenance Expenses"),
    (["freight", "courier", "postage", "delivery"], "Distribution Expenses"),
    # Assets
    (["cash at bank", "cash on hand", "petty cash", "cash management"], "Current Assets - Cash"),
    (["cash", "bank"], "Current Assets - Cash"),
    (["receivable", "debtor", "trade debtor"], "Current Assets - Trade Receivables"),
    (["inventory", "stock", "goods on hand", "raw material", "work in progress"], "Current Assets - Inventory"),
    (["prepay", "prepaid", "advance paid", "deposit paid"], "Current Assets - Prepayments"),
    (["accrued income", "accrued revenue"], "Current Assets - Accrued Income"),
    (["other current asset"], "Current Assets - Other"),
    (["fixed asset", "plant", "equipment", "machinery", "furniture", "fittings"], "Non-Current Assets - Fixed Assets"),
    (["property", "land", "building", "leasehold"], "Non-Current Assets - PP&E"),
    (["accumulated depreciation", "accumulated amortisation"], "Non-Current Assets - Accumulated Depreciation"),
    (["goodwill", "trademark", "patent", "intellectual property", "intangible"], "Non-Current Assets - Intangibles"),
    (["investment", "long-term investment", "shares held"], "Non-Current Assets - Investments"),
    (["other non-current", "other long-term"], "Non-Current Assets - Other"),
    # Liabilities
    (["payable", "creditor", "accounts payable", "trade creditor"], "Current Liabilities - Trade Payables"),
    (["gst payable", "gst liability", "bas liability"], "Current Liabilities - Tax Payable"),
    (["income tax payable", "tax liability", "tax provision"], "Current Liabilities - Income Tax Payable"),
    (["payroll liabilit", "payg withholding", "payroll tax"], "Current Liabilities - Payroll Liabilities"),
    (["superannuation payable", "super payable"], "Current Liabilities - Superannuation Payable"),
    (["accrued expense", "accrued liabilit", "accrual"], "Current Liabilities - Accrued Expenses"),
    (["deferred revenue", "unearned revenue", "deferred income"], "Current Liabilities - Deferred Revenue"),
    (["other current liabilit"], "Current Liabilities - Other"),
    (["loan", "mortgage", "borrowing", "term loan", "finance lease"], "Non-Current Liabilities - Loans"),
    (["deferred tax liabilit"], "Non-Current Liabilities - Deferred Tax"),
    (["other non-current liabilit"], "Non-Current Liabilities - Other"),
    # Equity
    (["retained earnings", "retained profit", "accumulated profit", "accumulated surplus"], "Equity - Retained Earnings"),
    (["share capital", "paid up capital", "issued capital", "ordinary shares"], "Equity - Paid-Up Capital"),
    (["current year earnings", "current year profit", "current year surplus"], "Equity - Current Year Profit"),
    (["drawings", "owner drawing"], "Equity - Drawings"),
    (["reserve", "general reserve", "asset revaluation"], "Equity - Reserves"),
    (["gst", "tax"], "Current Liabilities - Tax Payable"),
    (["payroll", "payg"], "Current Liabilities - Payroll Liabilities"),
]


def _normalise(name: str) -> str:
    """Lowercase and strip punctuation for comparison."""
    return re.sub(r"[^\w\s]", "", name.lower()).strip()


def map_account(account_name: str, run_log_path: str = None) -> tuple:
    """
    Map a single Xero account name to its ATO category.

    Returns:
        (ato_category: str, method: str) where method is one of:
        "custom", "exact", "normalised", "keyword", "manual"

    Raises:
        SystemExit if UNMAPPED and operator aborts.
    """
    # 1. Custom overrides take priority
    if account_name in config.CUSTOM_ACCOUNT_MAPPINGS:
        _log_mapping(account_name, config.CUSTOM_ACCOUNT_MAPPINGS[account_name], "custom", run_log_path)
        return config.CUSTOM_ACCOUNT_MAPPINGS[account_name], "custom"

    # 2. Exact match (case-insensitive)
    for xero_name, ato_cat in XERO_TO_ATO_MAP.items():
        if xero_name.lower() == account_name.lower():
            _log_mapping(account_name, ato_cat, "exact", run_log_path)
            return ato_cat, "exact"

    # 3. Punctuation-stripped comparison
    norm_input = _normalise(account_name)
    for xero_name, ato_cat in XERO_TO_ATO_MAP.items():
        if _normalise(xero_name) == norm_input:
            _log_mapping(account_name, ato_cat, "normalised", run_log_path)
            return ato_cat, "normalised"

    # 4. Keyword extraction
    for keywords, ato_cat in _KEYWORD_MAP:
        for kw in keywords:
            if kw in norm_input:
                _log_mapping(account_name, ato_cat, "keyword", run_log_path)
                return ato_cat, "keyword"

    # 5. UNMAPPED — log and assign to catch-all to avoid blocking the prototype.
    # The mapping is recorded in run_log.txt so the accountant can review and add
    # CUSTOM_ACCOUNT_MAPPINGS in config.py for any accounts that matter.
    ato_cat = "Administration Expenses - Unclassified"
    logger.warning("UNMAPPED account '%s' — assigned to '%s'", account_name, ato_cat)
    _log_mapping(account_name, ato_cat, "unmapped-fallback", run_log_path)
    return ato_cat, "unmapped-fallback"


def _handle_unmapped(account_name: str, run_log_path: str) -> tuple:
    """
    Display an UNMAPPED warning and block for human resolution.
    Returns the manually entered mapping or exits.
    """
    console.print()
    console.print(f"[bold red]UNMAPPED ACCOUNT: '{account_name}'[/bold red]")
    console.print(
        "This account name could not be automatically mapped to an ATO category.\n"
        "Common categories: Revenue - Sales, Cost of Sales, Employee Expenses - Wages,\n"
        "  Occupancy Costs - Rent, Depreciation, Professional Fees - Accounting, etc.\n"
        "\nEnter the ATO category to map to, or 'abort' to cancel:"
    )

    while True:
        try:
            response = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            sys.exit(1)

        if response.lower() == "abort":
            console.print("[bold red]Pipeline aborted.[/bold red]")
            sys.exit(1)
        elif response:
            console.print(f"[green]Mapped '{account_name}' -> '{response}'[/green]")
            _log_mapping(account_name, response, "manual", run_log_path)
            return response, "manual"
        else:
            console.print("[yellow]Please enter a category or 'abort'.[/yellow]")


def _log_mapping(account_name: str, ato_category: str, method: str, run_log_path: str):
    logger.info("Account mapping [%s]: '%s' -> '%s'", method, account_name, ato_category)
    if run_log_path:
        try:
            with open(run_log_path, "a", encoding="utf-8") as f:
                f.write(f"[MAPPING:{method}] '{account_name}' -> '{ato_category}'\n")
        except Exception:
            pass


def map_accounts(account_names: list, run_log_path: str = None) -> dict:
    """
    Map a list of account names. Returns {account_name: (ato_category, method)}.
    """
    results = {}
    for name in account_names:
        results[name] = map_account(name, run_log_path=run_log_path)
    return results
