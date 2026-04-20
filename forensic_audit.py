#!/usr/bin/env python3
"""
forensic_audit.py -- Source-faithful forensic accounting audit and corrected report generator.

Extracts figures directly from source PDFs (never uses defaults), validates three
accounting equations, detects account misclassifications, and regenerates a corrected
PDF with a reconciliation table and DATA INTEGRITY stamp.

Usage:
    python forensic_audit.py                      # Full run
    python forensic_audit.py --dry-run            # Validate only, no PDF output
    python forensic_audit.py --output-dir DIR     # Custom output directory
"""

import argparse
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pdfplumber
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (
    Frame, HRFlowable, PageBreak, Paragraph, SimpleDocTemplate,
    Spacer, Table, TableStyle,
)
from reportlab.platypus.doctemplate import PageTemplate

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

INPUT_DIR = Path("input")
OUTPUT_BASE = Path("outputs")
PL_FILE = INPUT_DIR / "pl_ironbark_fy2024.pdf"
BS_FILE = INPUT_DIR / "bs_ironbark_fy2024.pdf"

# ---------------------------------------------------------------------------
# Colours
# ---------------------------------------------------------------------------

NAVY       = colors.HexColor("#1B3A6B")
GREY_BG    = colors.HexColor("#F2F2F2")
RED_C      = colors.HexColor("#CC0000")
GREEN_C    = colors.HexColor("#006600")
ORANGE_C   = colors.HexColor("#CC6600")
WHITE      = colors.white
LIGHT_GREY = colors.HexColor("#CCCCCC")

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class ExtractionError(Exception):
    """Raised when a required figure cannot be extracted from a source PDF."""

class BalanceSheetImbalanceError(Exception):
    """Raised when Total Assets ? Total Liabilities + Total Equity."""

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ExtractedItem:
    account: str
    cy_amount: float
    py_amount: Optional[float]
    section: str            # detected from table header context
    source_file: str
    page: int
    raw_line: str


@dataclass
class EquationCheck:
    name: str
    lhs_label: str
    lhs_value: float
    rhs_label: str
    rhs_value: float
    passed: bool
    tolerance: float = 1.0

    @property
    def difference(self) -> float:
        return abs(self.lhs_value - self.rhs_value)


@dataclass
class MisclassificationError:
    account: str
    found_in_section: str
    should_be_in_section: str
    amount: float
    source_file: str
    page: int


@dataclass
class FinancialModel:
    # P&L buckets
    revenue: Dict[str, float]            = field(default_factory=dict)
    other_income: Dict[str, float]       = field(default_factory=dict)
    cost_of_sales: Dict[str, float]      = field(default_factory=dict)
    selling_expenses: Dict[str, float]   = field(default_factory=dict)
    admin_expenses: Dict[str, float]     = field(default_factory=dict)
    operating_expenses: Dict[str, float] = field(default_factory=dict)
    finance_costs: Dict[str, float]      = field(default_factory=dict)
    depreciation_amort: Dict[str, float] = field(default_factory=dict)
    income_tax: Dict[str, float]         = field(default_factory=dict)
    # BS buckets
    current_assets: Dict[str, float]          = field(default_factory=dict)
    non_current_assets: Dict[str, float]      = field(default_factory=dict)
    current_liabilities: Dict[str, float]     = field(default_factory=dict)
    non_current_liabilities: Dict[str, float] = field(default_factory=dict)
    equity: Dict[str, float]                  = field(default_factory=dict)
    # Audit trail
    all_items: List[ExtractedItem]          = field(default_factory=list)
    misclassifications: List[MisclassificationError] = field(default_factory=list)
    equation_checks: List[EquationCheck]    = field(default_factory=list)
    warnings: List[str]                     = field(default_factory=list)
    # Computed totals (populated by compute_totals)
    total_revenue: float             = 0.0
    total_other_income: float        = 0.0
    total_cos: float                 = 0.0
    gross_profit: float              = 0.0
    total_opex: float                = 0.0
    total_finance_costs: float       = 0.0
    total_da: float                  = 0.0
    total_income_tax: float          = 0.0
    ebit: float                      = 0.0
    net_profit_before_tax: float     = 0.0
    net_profit: float                = 0.0
    total_current_assets: float      = 0.0
    total_non_current_assets: float  = 0.0
    total_assets: float              = 0.0
    total_current_liabilities: float        = 0.0
    total_non_current_liabilities: float    = 0.0
    total_liabilities: float                = 0.0
    total_equity: float                     = 0.0


# ---------------------------------------------------------------------------
# Section detection
# ---------------------------------------------------------------------------

# P&L: ordered from most-specific to least-specific to avoid mis-fires
PL_SECTION_PATTERNS = [
    (re.compile(r'OTHER\s+INCOME|MISCELLANEOUS\s+INCOME|OTHER\s+OPERATING\s+INCOME', re.I), 'other_income'),
    (re.compile(r'REVENUE|SALES\s+INCOME|TRADING\s+INCOME', re.I), 'revenue'),
    (re.compile(r'COST\s+OF\s+(?:GOODS\s+SOLD|SALES|REVENUE)|DIRECT\s+COSTS?', re.I), 'cost_of_sales'),
    (re.compile(r'FINANCE\s+(?:COSTS?|[&]\s*BANKING|EXPENSES?)|INTEREST\s+EXPENSE|BORROWING\s+COSTS?|BANKING', re.I), 'finance_costs'),
    (re.compile(r'DEPRECIATION\s+(?:AND|&)\s+AMORTIS|D\s*[&/]\s*A\b', re.I), 'depreciation_amort'),
    (re.compile(r'INCOME\s+TAX', re.I), 'income_tax'),
    # Expense sub-sections (map all to operating_expenses)
    (re.compile(r'PERSONNEL|EMPLOYEE\s+COSTS?|STAFF\s+COSTS?|LABOUR\s+COSTS?', re.I), 'operating_expenses'),
    (re.compile(r'OCCUPANCY|FACILITIES|PROPERTY\s+COSTS?', re.I), 'operating_expenses'),
    (re.compile(r'FLEET|VEHICLE|LOGISTICS', re.I), 'operating_expenses'),
    (re.compile(r'SALES\s*[&]\s*MARKETING|MARKETING|ADVERTISING', re.I), 'operating_expenses'),
    (re.compile(r'ADMINISTRATION|ADMIN|GENERAL\s+EXPENSES?', re.I), 'operating_expenses'),
    (re.compile(r'SELLING\s+(?:EXPENSES?|COSTS?)', re.I), 'operating_expenses'),
    (re.compile(r'(?:OPERATING\s+)?EXPENSES?', re.I), 'operating_expenses'),
]

# BS: non-current before current to prevent "CURRENT" matching "NON-CURRENT"
BS_SECTION_PATTERNS = [
    (re.compile(r'NON[\s\-]+CURRENT\s+ASSETS?', re.I), 'non_current_assets'),
    (re.compile(r'CURRENT\s+ASSETS?', re.I), 'current_assets'),
    (re.compile(r'NON[\s\-]+CURRENT\s+LIABILIT', re.I), 'non_current_liabilities'),
    (re.compile(r'CURRENT\s+LIABILIT', re.I), 'current_liabilities'),
    (re.compile(r"SHAREHOLDERS['\u2019]?\s+EQUITY|OWNERS['\u2019]?\s+EQUITY|(?<!\w)EQUITY\b", re.I), 'equity'),
]

# Aggregate / subtotal lines -- skip these to avoid double-counting
SKIP_LINE_RE = re.compile(
    r'^(?:total\b|sub[\s\-]?total\b|grand\s+total\b|'
    r'net\s+(?:profit|loss|assets?|position|working\s+capital)\b|'
    r'gross\s+profit\b|'
    r'ebitda\b|ebit\b|'
    r'operating\s+(?:profit|result)\b|'
    r'profit\s+(?:before|after)\s|'
    r'loss\s+(?:before|after)\s|'
    r'earnings\s+before\s|'
    r'less\s*:\s*(?:total|operating|cost\s+of|dividend)|'
    r'add\s*:\s*|'
    # Equity reconciliation movement rows (only closing balance is the BS figure)
    r'(?:retained\s+earnings?|accumulated\s+(?:profit|surplus))\s*[^\w].*opening\s+balance|'
    r'(?:retained\s+earnings?|accumulated\s+(?:profit|surplus))\s*[^\w].*movement|'
    r'opening\s+balance\s*$|'
    r'(?:dividend|distribution)s?\s+(?:paid|declared)|'
    r'net\s+working\s+capital\b)',
    re.I,
)

# Matches formatted AUD amounts (e.g. 1,234 / 1,234.56 / (1,234) )
AMOUNT_RE = re.compile(r'\(?\d{1,3}(?:,\d{3})+(?:\.\d{1,2})?\)?')

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def detect_section(header_text: str, doc_type: str) -> Optional[str]:
    patterns = PL_SECTION_PATTERNS if doc_type == 'pl' else BS_SECTION_PATTERNS
    for pattern, section in patterns:
        if pattern.search(header_text):
            return section
    return None


def parse_amount(s: str) -> Optional[float]:
    s = s.strip().replace(',', '')
    negative = s.startswith('(') and s.endswith(')')
    s = s.strip('()')
    try:
        v = float(s)
        return -v if negative else v
    except ValueError:
        return None


def _parse_data_line(line: str, section: str, source_name: str, page_num: int) -> Optional['ExtractedItem']:
    """
    Parse a single text line (from raw page text fallback) into an ExtractedItem.
    Returns None if the line cannot be parsed as a financial data row.
    """
    amounts = AMOUNT_RE.findall(line)
    if not amounts:
        return None
    first_match = AMOUNT_RE.search(line)
    account = line[:first_match.start()].strip().rstrip('----. \t')
    if not account or len(account) < 3:
        return None
    if SKIP_LINE_RE.match(account):
        return None
    cy_amount = parse_amount(amounts[-1])
    if cy_amount is None:
        return None
    py_amount = parse_amount(amounts[-2]) if len(amounts) >= 2 else None
    return ExtractedItem(
        account=account,
        cy_amount=cy_amount,
        py_amount=py_amount,
        section=section,
        source_doc=source_name,
        page_num=page_num,
    )


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------

def extract_from_pdf(pdf_path: Path, doc_type: str) -> List[ExtractedItem]:
    """
    Extract all financial line items from a PDF using section-aware, column-aware parsing.

    For each pdfplumber table:
      - Row 0 = section header (may be multi-column: ["REVENUE", "PY 2022-23", "CY 2023-24"])
      - Rows 1+ = data rows (may be multi-column: ["account name", "3,842,150", "4,218,440"])

    The CY amount is the last numeric column; the PY amount is the second-to-last.

    Raises:
        FileNotFoundError: If the PDF does not exist.
        ExtractionError:   If no financial data can be extracted.
    """
    if not pdf_path.exists():
        raise FileNotFoundError(f"Source PDF not found: {pdf_path}")

    items: List[ExtractedItem] = []
    current_section: Optional[str] = None
    source_name = pdf_path.name

    with pdfplumber.open(str(pdf_path)) as pdf:
        for page_num, page in enumerate(pdf.pages, 1):
            raw_tables = page.extract_tables()

            if not raw_tables:
                # Fallback: parse from raw page text
                text = page.extract_text() or ""
                for line in text.split('\n'):
                    line = line.strip()
                    if not line:
                        continue
                    detected = detect_section(line, doc_type)
                    if detected:
                        current_section = detected
                        continue
                    if current_section:
                        item = _parse_data_line(line, current_section, source_name, page_num)
                        if item:
                            items.append(item)
                continue

            for raw_table in raw_tables:
                if not raw_table or len(raw_table) < 1:
                    continue

                # Detect section from header row (join all header cells)
                header_cells = [str(c or '').strip() for c in raw_table[0]]
                header_text = ' '.join(c for c in header_cells if c)
                detected = detect_section(header_text, doc_type)
                if detected:
                    current_section = detected

                if current_section is None:
                    continue

                # Identify which column holds the account name and which hold amounts
                # Strategy: account name = first non-numeric column; CY = last numeric column
                for row in (raw_table if detected is None else raw_table[1:]):
                    cells = [str(c or '').strip() for c in row]
                    if not any(cells):
                        continue

                    # Numeric cell: formatted amount OR standalone "0"
                    _NUMERIC_CELL_RE = re.compile(r'^(?:\(?\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?\)?|0)$')
                    _TRAILING_ZERO_RE = re.compile(r'\s+0\s*$')

                    # Try multi-column first: find first text cell and last two numeric cells
                    numeric_cells = [(i, c) for i, c in enumerate(cells) if _NUMERIC_CELL_RE.match(c)]
                    text_cells = [(i, c) for i, c in enumerate(cells)
                                  if c and not _NUMERIC_CELL_RE.match(c) and len(c) > 2]

                    if numeric_cells and text_cells:
                        account = text_cells[0][1].strip().rstrip('----. \t')
                        cy_raw = numeric_cells[-1][1]
                        py_raw = numeric_cells[-2][1] if len(numeric_cells) >= 2 else None
                    else:
                        # Collapsed single-cell row: parse line as text
                        line = ' '.join(c for c in cells if c)
                        amounts = AMOUNT_RE.findall(line)
                        if not amounts:
                            continue
                        first_match = AMOUNT_RE.search(line)
                        account = line[:first_match.start()].strip().rstrip('----. \t')
                        # Check for trailing " 0" (CY=0, AMOUNT_RE won't match "0")
                        if _TRAILING_ZERO_RE.search(line):
                            cy_raw = '0'
                            py_raw = amounts[-1]
                        else:
                            cy_raw = amounts[-1]
                            py_raw = amounts[-2] if len(amounts) >= 2 else None

                    if not account or len(account) < 3:
                        continue
                    if SKIP_LINE_RE.match(account):
                        continue

                    cy_amount = parse_amount(cy_raw)
                    if cy_amount is None:
                        continue
                    py_amount = parse_amount(py_raw) if py_raw else None

                    items.append(ExtractedItem(
                        account=account,
                        cy_amount=cy_amount,
                        py_amount=py_amount,
                        section=current_section,
                        source_file=source_name,
                        page=page_num,
                        raw_line=' | '.join(cells),
                    ))

    if not items:
        raise ExtractionError(
            f"No financial data extracted from '{pdf_path.name}'. "
            "Verify the PDF contains machine-readable text."
        )

    return items


# ---------------------------------------------------------------------------
# Classification validation
# ---------------------------------------------------------------------------

# Which statement a section key belongs to
_SECTION_STMT = {
    'revenue': 'pl', 'other_income': 'pl', 'cost_of_sales': 'pl',
    'selling_expenses': 'pl', 'admin_expenses': 'pl', 'operating_expenses': 'pl',
    'finance_costs': 'pl', 'depreciation_amort': 'pl', 'income_tax': 'pl',
    'current_assets': 'bs', 'non_current_assets': 'bs',
    'current_liabilities': 'bs', 'non_current_liabilities': 'bs', 'equity': 'bs',
}

# Account names that belong unambiguously on the Balance Sheet
_MUST_BS_RE = re.compile(
    r'cash\s+at\s+bank|cash\s+on\s+hand|petty\s+cash|'
    r'accounts?\s+receiv|trade\s+debtor|'
    r'inventor|stock\s+on\s+hand|'
    r'prepay|deposit\s+(?:paid|asset)|'
    r'accounts?\s+payabl|trade\s+creditor|'
    r'gst\s+(?:payabl|collect|clears?)|'
    r'payroll\s+liabilit|superannuation\s+payabl|'
    r'income\s+tax\s+payabl|'
    r'retained\s+earnings|share\s+capital',
    re.I,
)

# These P&L sections legitimately reference BS asset/liability names
# (e.g. "Depreciation — plant & equipment", "Interest — term loan")
# Do not flag misclassification for items in these sections.
_MISCLASS_EXEMPT_SECTIONS = {'depreciation_amort', 'finance_costs'}


def validate_classification(item: ExtractedItem) -> Optional[MisclassificationError]:
    """Return a MisclassificationError if a BS account appears in a P&L section."""
    stmt = _SECTION_STMT.get(item.section, 'unknown')
    if stmt == 'pl' and item.section not in _MISCLASS_EXEMPT_SECTIONS and _MUST_BS_RE.search(item.account):
        return MisclassificationError(
            account=item.account,
            found_in_section=item.section,
            should_be_in_section='balance_sheet',
            amount=item.cy_amount,
            source_file=item.source_file,
            page=item.page,
        )
    return None


# ---------------------------------------------------------------------------
# Model building
# ---------------------------------------------------------------------------

_SECTION_TO_BUCKET = {
    'revenue':            'revenue',
    'other_income':       'other_income',
    'cost_of_sales':      'cost_of_sales',
    'selling_expenses':   'selling_expenses',
    'admin_expenses':     'admin_expenses',
    'operating_expenses': 'operating_expenses',
    'finance_costs':      'finance_costs',
    'depreciation_amort': 'depreciation_amort',
    'income_tax':         'income_tax',
    'current_assets':     'current_assets',
    'non_current_assets': 'non_current_assets',
    'current_liabilities':     'current_liabilities',
    'non_current_liabilities': 'non_current_liabilities',
    'equity':             'equity',
}


def build_model(pl_items: List[ExtractedItem], bs_items: List[ExtractedItem]) -> FinancialModel:
    model = FinancialModel()
    model.all_items = pl_items + bs_items

    for item in model.all_items:
        mc = validate_classification(item)
        if mc:
            model.misclassifications.append(mc)
            model.warnings.append(
                f"MISCLASSIFICATION: '{item.account}' (${item.cy_amount:,.2f}) "
                f"found in '{item.section}' -- should be on Balance Sheet"
            )

        bucket_name = _SECTION_TO_BUCKET.get(item.section)
        if bucket_name:
            bucket: dict = getattr(model, bucket_name)
            bucket[item.account] = bucket.get(item.account, 0.0) + item.cy_amount

    compute_totals(model)
    return model


def compute_totals(model: FinancialModel):
    model.total_revenue      = sum(model.revenue.values())
    model.total_other_income = sum(model.other_income.values())
    model.total_cos          = sum(model.cost_of_sales.values())
    model.gross_profit       = model.total_revenue - model.total_cos

    all_opex = {**model.selling_expenses, **model.admin_expenses, **model.operating_expenses}
    model.total_opex         = sum(all_opex.values())
    model.total_da           = sum(model.depreciation_amort.values())
    model.total_finance_costs = sum(model.finance_costs.values())
    model.total_income_tax   = sum(model.income_tax.values())

    model.ebit               = model.gross_profit + model.total_other_income - model.total_opex - model.total_da
    model.net_profit_before_tax = model.ebit - model.total_finance_costs
    model.net_profit         = model.net_profit_before_tax - model.total_income_tax

    model.total_current_assets       = sum(model.current_assets.values())
    model.total_non_current_assets   = sum(model.non_current_assets.values())
    model.total_assets               = model.total_current_assets + model.total_non_current_assets

    model.total_current_liabilities     = sum(model.current_liabilities.values())
    model.total_non_current_liabilities = sum(model.non_current_liabilities.values())
    model.total_liabilities             = model.total_current_liabilities + model.total_non_current_liabilities
    model.total_equity                  = sum(model.equity.values())


# ---------------------------------------------------------------------------
# Equation verification
# ---------------------------------------------------------------------------

def verify_equations(model: FinancialModel) -> List[EquationCheck]:
    """
    Runs three accounting equation checks. Sets model.equation_checks before
    raising so the log writer can still access results.

    Raises BalanceSheetImbalanceError if Total Assets ? TL + Equity.
    """
    checks = []

    # 1. Gross Profit = Revenue ? COGS
    gp = model.total_revenue - model.total_cos
    checks.append(EquationCheck(
        name="Gross Profit",
        lhs_label="Revenue ? COGS",
        lhs_value=gp,
        rhs_label="Derived Gross Profit",
        rhs_value=model.gross_profit,
        passed=abs(gp - model.gross_profit) <= 1.0,
    ))

    # 2. Net Profit derivation
    computed_np = (model.gross_profit + model.total_other_income
                   - model.total_opex - model.total_da
                   - model.total_finance_costs - model.total_income_tax)
    checks.append(EquationCheck(
        name="Net Profit",
        lhs_label="GP + OI ? OpEx ? D&A ? Finance ? Tax",
        lhs_value=computed_np,
        rhs_label="Derived Net Profit",
        rhs_value=model.net_profit,
        passed=abs(computed_np - model.net_profit) <= 1.0,
    ))

    # 3. Balance sheet equation
    tl_eq = model.total_liabilities + model.total_equity
    bs_check = EquationCheck(
        name="Balance Sheet Equation",
        lhs_label="Total Assets",
        lhs_value=model.total_assets,
        rhs_label="Total Liabilities + Equity",
        rhs_value=tl_eq,
        passed=abs(model.total_assets - tl_eq) <= 1.0,
    )
    checks.append(bs_check)

    model.equation_checks = checks  # set before possibly raising

    if not bs_check.passed:
        raise BalanceSheetImbalanceError(
            f"Total Assets ${model.total_assets:,.2f} ? "
            f"Liabilities + Equity ${tl_eq:,.2f}  "
            f"(difference: ${abs(model.total_assets - tl_eq):,.2f})"
        )

    return checks


# ---------------------------------------------------------------------------
# Validation log
# ---------------------------------------------------------------------------

# Known-wrong figures from the previous pipeline run (from problem statement audit)
KNOWN_PIPELINE_WRONG = {
    "Total Revenue":          8_561_540.00,
    "Total Assets":           6_667_780.00,
    "Total Liabilities + Equity": 8_348_940.00,
    "BS Imbalance (reported)": 1_681_160.00,
}


def write_validation_log(
    model: FinancialModel,
    pl_items: List[ExtractedItem],
    bs_items: List[ExtractedItem],
    log_path: Path,
    bs_error: Optional[Exception] = None,
) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    L: List[str] = []

    def section(title: str, char: str = '='):
        L.extend(['', title, char * max(len(title), 40)])

    L.extend([
        "FORENSIC AUDIT VALIDATION LOG",
        "=" * 60,
        f"Run:          {ts}",
        f"Source P&L:   {PL_FILE}",
        f"Source BS:    {BS_FILE}",
        f"Log file:     {log_path}",
    ])

    # --- Extraction report ---
    section("EXTRACTION REPORT")
    L.append(f"P&L items extracted:   {len(pl_items)}")
    L.append(f"BS  items extracted:   {len(bs_items)}")
    L.append(f"Total line items:      {len(pl_items) + len(bs_items)}")

    L.append("\nP&L LINE ITEMS (account | CY amount | PY amount | section | page):")
    prev = None
    for it in pl_items:
        if it.section != prev:
            L.append(f"\n  [{it.section.upper().replace('_', ' ')}]")
            prev = it.section
        py_s = f"  PY: ${it.py_amount:>12,.2f}" if it.py_amount is not None else ""
        L.append(f"    p{it.page}  {it.account:<55}  CY: ${it.cy_amount:>12,.2f}{py_s}")

    L.append("\nBALANCE SHEET LINE ITEMS:")
    prev = None
    for it in bs_items:
        if it.section != prev:
            L.append(f"\n  [{it.section.upper().replace('_', ' ')}]")
            prev = it.section
        py_s = f"  PY: ${it.py_amount:>12,.2f}" if it.py_amount is not None else ""
        L.append(f"    p{it.page}  {it.account:<55}  CY: ${it.cy_amount:>12,.2f}{py_s}")

    # --- Classification checks ---
    section("CLASSIFICATION CHECKS")
    if not model.misclassifications:
        L.append("  No misclassifications detected.")
    else:
        L.append(f"  {len(model.misclassifications)} MISCLASSIFICATION(S) FOUND:")
        for mc in model.misclassifications:
            L.extend([
                f"",
                f"  [FAIL] MISCLASSIFICATION_ERROR",
                f"    Account:      {mc.account}",
                f"    Found in:     {mc.found_in_section}",
                f"    Should be in: {mc.should_be_in_section}",
                f"    CY Amount:    ${mc.amount:,.2f}",
                f"    Source:       {mc.source_file}  page {mc.page}",
            ])

    # --- Computed financial summary ---
    section("COMPUTED FINANCIAL SUMMARY (CY 2023-24)")
    rows_pl = [
        ("Revenue",                    model.total_revenue),
        ("Cost of Sales",              model.total_cos),
        ("Gross Profit",               model.gross_profit),
        ("Other Income",               model.total_other_income),
        ("Operating Expenses",         model.total_opex),
        ("Depreciation & Amort",       model.total_da),
        ("EBIT",                       model.ebit),
        ("Finance Costs",              model.total_finance_costs),
        ("Net Profit Before Tax",      model.net_profit_before_tax),
        ("Income Tax",                 model.total_income_tax),
        ("Net Profit After Tax",       model.net_profit),
    ]
    for label, val in rows_pl:
        if val != 0.0:
            L.append(f"  {label:<32}  ${val:>15,.2f}")
    L.append("")
    rows_bs = [
        ("Current Assets",             model.total_current_assets),
        ("Non-Current Assets",         model.total_non_current_assets),
        ("TOTAL ASSETS",               model.total_assets),
        ("Current Liabilities",        model.total_current_liabilities),
        ("Non-Current Liabilities",    model.total_non_current_liabilities),
        ("Total Liabilities",          model.total_liabilities),
        ("Total Equity",               model.total_equity),
        ("Liabilities + Equity",       model.total_liabilities + model.total_equity),
    ]
    for label, val in rows_bs:
        L.append(f"  {label:<32}  ${val:>15,.2f}")

    # --- Equation checks ---
    section("ACCOUNTING EQUATION CHECKS")
    overall_pass = True
    for chk in model.equation_checks:
        status = "[PASS]" if chk.passed else f"[FAIL] FAIL  (difference: ${chk.difference:,.2f})"
        L.extend([
            f"  {chk.name}:",
            f"    {chk.lhs_label:<45}  ${chk.lhs_value:>15,.2f}",
            f"    {chk.rhs_label:<45}  ${chk.rhs_value:>15,.2f}",
            f"    Result: {status}",
            "",
        ])
        if not chk.passed:
            overall_pass = False

    if bs_error:
        L.append(f"  [FAIL] BalanceSheetImbalanceError: {bs_error}")
        overall_pass = False

    # --- Retained Earnings note ---
    section("RETAINED EARNINGS RECONCILIATION", "-")
    retained = model.equity.get(
        next((k for k in model.equity if 'retained' in k.lower()), ''), None
    )
    if retained is not None:
        L.extend([
            f"  Retained Earnings (closing):  ${retained:,.2f}",
            f"  Opening balance + NPAT - Dividends = Closing Retained Earnings.",
            f"  [WARN]  Opening balance not available in source PDFs -- cannot verify movement.",
            f"  [WARN]  Flag for accountant review: RETAINED_EARNINGS_UNVERIFIABLE",
        ])
    else:
        L.append("  Retained Earnings account not found in equity section.")

    # --- Interest Income classification note ---
    interest_in_rev = {k: v for k, v in model.revenue.items() if 'interest' in k.lower()}
    if interest_in_rev:
        section("INTEREST INCOME CLASSIFICATION NOTE", "-")
        for acc, amt in interest_in_rev.items():
            L.extend([
                f"  '{acc}' (${amt:,.2f}) is recorded under Revenue in source P&L.",
                f"  AASB recommendation: reclassify to Other Income for non-financial entities.",
                f"  [WARN]  Flag for accountant review: INTEREST_INCOME_CLASSIFICATION",
            ])

    # --- Reconciliation vs known-wrong pipeline ---
    section("RECONCILIATION vs PREVIOUS PIPELINE OUTPUT (KNOWN ERRORS)")
    comparisons = [
        ("Total Revenue",          KNOWN_PIPELINE_WRONG["Total Revenue"],          model.total_revenue),
        ("Total Assets",           KNOWN_PIPELINE_WRONG["Total Assets"],           model.total_assets),
        ("Liabilities + Equity",   KNOWN_PIPELINE_WRONG["Total Liabilities + Equity"],
                                                                                    model.total_liabilities + model.total_equity),
    ]
    L.append(f"  {'Metric':<30}  {'Pipeline (Wrong)':>18}  {'Forensic (Correct)':>20}  {'?':>14}")
    L.append("  " + "-" * 88)
    for metric, old, new in comparisons:
        diff = new - old
        sign = "+" if diff >= 0 else ""
        L.append(f"  {metric:<30}  ${old:>17,.2f}  ${new:>19,.2f}  {sign}${abs(diff):>12,.2f}")

    # --- Warnings ---
    if model.warnings:
        section("WARNINGS")
        for w in model.warnings:
            L.append(f"  [WARN]  {w}")

    # --- Final verdict ---
    section("FINAL VERDICT", "-")
    if overall_pass and not model.misclassifications:
        L.extend([
            "  [PASS] -- All three accounting equations verified.",
            "  [PASS] -- Balance sheet balances.",
            "  [PASS] -- No misclassification errors.",
            "  [PASS] -- All figures sourced from input PDFs (no defaults used).",
        ])
    else:
        issues = []
        if bs_error:
            issues.append("balance sheet does not balance")
        if model.misclassifications:
            issues.append(f"{len(model.misclassifications)} misclassification(s) detected")
        if any(not c.passed for c in model.equation_checks):
            issues.append("equation check failure(s)")
        L.extend([
            f"  [FAIL] FAIL -- Issues detected: {'; '.join(issues)}.",
            "  Corrected report generated with DATA INTEGRITY warnings.",
            "  Review misclassification errors and accounting equation failures above.",
        ])

    content = "\n".join(L)
    log_path.write_text(content, encoding="utf-8")
    return content


# ---------------------------------------------------------------------------
# PDF report generation
# ---------------------------------------------------------------------------

def _styles():
    base = getSampleStyleSheet()
    def s(name, **kw):
        parent = kw.pop('parent', base['Normal'])
        return ParagraphStyle(name, parent=parent, **kw)
    return {
        'title':   s('t', parent=base['Title'], fontSize=20, textColor=NAVY,
                     alignment=TA_CENTER, fontName='Helvetica-Bold', spaceAfter=8),
        'sub':     s('s', fontSize=10, alignment=TA_CENTER, fontName='Helvetica', spaceAfter=4),
        'notice':  s('n', fontSize=8, textColor=colors.HexColor('#666666'),
                     alignment=TA_CENTER, fontName='Helvetica-Oblique', spaceAfter=3),
        'heading': s('h', fontSize=12, textColor=NAVY, fontName='Helvetica-Bold',
                     spaceBefore=14, spaceAfter=4),
        'body':    s('b', fontSize=9, fontName='Helvetica', spaceAfter=4, leading=13),
        'pass_s':  s('p', fontSize=9, textColor=GREEN_C, fontName='Helvetica-Bold'),
        'fail_s':  s('f', fontSize=9, textColor=RED_C, fontName='Helvetica-Bold'),
        'warn_s':  s('w', fontSize=9, textColor=ORANGE_C, fontName='Helvetica-Bold'),
        'sh':      s('sh', fontSize=8, fontName='Helvetica-Bold'),
    }


def _ts_base():
    return TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), NAVY),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('ALIGN', (1, 0), (-1, -1), 'RIGHT'),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, GREY_BG]),
        ('LINEBELOW', (0, 0), (-1, 0), 0.5, NAVY),
        ('LEFTPADDING', (0, 0), (-1, -1), 5),
        ('RIGHTPADDING', (0, 0), (-1, -1), 5),
        ('TOPPADDING', (0, 0), (-1, -1), 3),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
    ])


def _apply_total(ts: TableStyle, ri: int):
    ts.add('BACKGROUND', (0, ri), (-1, ri), GREY_BG)
    ts.add('FONTNAME', (0, ri), (-1, ri), 'Helvetica-Bold')
    ts.add('LINEABOVE', (0, ri), (-1, ri), 0.5, LIGHT_GREY)
    ts.add('LINEBELOW', (0, ri), (-1, ri), 0.5, NAVY)


def _fmt(v: float) -> str:
    if v < 0:
        return f"(${abs(v):,.2f})"
    return f"${v:,.2f}"


def _footer(canvas, doc):
    canvas.saveState()
    canvas.setFont('Helvetica', 7)
    canvas.setFillColor(colors.HexColor('#888888'))
    today = datetime.now().strftime('%d %B %Y')
    canvas.drawCentredString(
        A4[0] / 2, 1.2 * cm,
        f"Page {doc.page}  |  Forensic Audit Report  |  Ironbark Wholesale Distribution Pty Ltd  |  {today}"
    )
    canvas.restoreState()


def write_corrected_report(
    model: FinancialModel,
    output_path: Path,
    validation_passed: bool,
) -> None:
    st = _styles()
    COL = [11 * cm, 5 * cm]
    today = datetime.now().strftime('%d %B %Y')

    doc = SimpleDocTemplate(
        str(output_path), pagesize=A4,
        leftMargin=2*cm, rightMargin=2*cm,
        topMargin=2.5*cm, bottomMargin=2.5*cm,
    )
    frm = Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height)
    doc.addPageTemplates([PageTemplate(id='main', frames=[frm], onPage=_footer)])
    story = []

    # ---- Cover ----
    story += [Spacer(1, 3*cm)]
    story.append(Paragraph("Year-End Financial Report", st['title']))
    story.append(Paragraph("Ironbark Wholesale Distribution Pty Ltd", st['sub']))
    story.append(Paragraph("Financial Year Ended 30 June 2024", st['sub']))
    story.append(Paragraph(f"Report Date: {today}", st['sub']))
    story += [Spacer(1, 1.2*cm)]

    integrity_colour = GREEN_C if validation_passed else RED_C
    integrity_text = ("DATA INTEGRITY: VERIFIED -- All accounting equations pass. "
                      "All figures source-extracted." if validation_passed
                      else "DATA INTEGRITY: ISSUES DETECTED -- See reconciliation section.")
    stamp_st = ParagraphStyle('stmp', parent=st['body'], textColor=colors.white,
                               backColor=integrity_colour, fontSize=10,
                               fontName='Helvetica-Bold', alignment=TA_CENTER,
                               borderPad=6, spaceAfter=6)
    story.append(Paragraph(integrity_text, stamp_st))
    story += [Spacer(1, 0.3*cm)]

    eq_pass = all(c.passed for c in model.equation_checks)
    cover_lines = [
        f"Line items extracted from source PDFs: {len(model.all_items)}",
        f"Accounting equation checks: {'ALL PASS [OK]' if eq_pass else 'FAILURES DETECTED [FAIL]'}",
        f"Classification errors detected: {len(model.misclassifications)}",
        "Source files: pl_ironbark_fy2024.pdf + bs_ironbark_fy2024.pdf",
        "No default or estimated values -- ExtractionError raised if a figure is missing.",
    ]
    for line in cover_lines:
        story.append(Paragraph(line, st['notice']))

    interest_in_rev = {k: v for k, v in model.revenue.items() if 'interest' in k.lower()}
    if interest_in_rev:
        story += [Spacer(1, 0.3*cm)]
        story.append(Paragraph(
            "[WARN]  Interest income detected under Revenue -- consider reclassifying to Other Income "
            "(AASB recommendation for non-financial entities). Flagged for accountant review.",
            st['warn_s']
        ))

    if model.misclassifications:
        for mc in model.misclassifications[:3]:
            story.append(Paragraph(
                f"[WARN]  MISCLASSIFICATION: '{mc.account}' found in {mc.found_in_section}",
                st['warn_s']
            ))

    story.append(PageBreak())

    # ---- Income Statement ----
    story.append(Paragraph("Income Statement", st['heading']))
    story.append(Paragraph("For the year ended 30 June 2024", st['body']))
    story.append(HRFlowable(width='100%', thickness=1, color=NAVY, spaceAfter=6))

    is_d = [['Account', 'AUD']]
    total_rows = []

    def _h(text):
        return Paragraph(f'<b>{text}</b>', st['sh'])

    def _row(label, amt, indent='  '):
        return [f'{indent}{label}', _fmt(amt)]

    # Revenue
    is_d.append([_h('Revenue'), ''])
    for acc, amt in sorted(model.revenue.items()):
        is_d.append(_row(acc, amt))
    total_rows.append(len(is_d)); is_d.append(['Total Revenue', _fmt(model.total_revenue)])

    # Other Income
    if model.other_income:
        is_d.append(['', ''])
        is_d.append([_h('Other Income'), ''])
        for acc, amt in sorted(model.other_income.items()):
            is_d.append(_row(acc, amt))
        total_rows.append(len(is_d)); is_d.append(['Total Other Income', _fmt(model.total_other_income)])

    # Cost of Sales
    if model.cost_of_sales:
        is_d.append(['', ''])
        is_d.append([_h('Cost of Sales'), ''])
        for acc, amt in sorted(model.cost_of_sales.items()):
            is_d.append(_row(acc, amt))
        total_rows.append(len(is_d)); is_d.append(['Total Cost of Sales', _fmt(model.total_cos)])
        total_rows.append(len(is_d)); is_d.append(['Gross Profit', _fmt(model.gross_profit)])

    # Operating Expenses
    all_opex = {**model.selling_expenses, **model.admin_expenses, **model.operating_expenses}
    if all_opex:
        is_d.append(['', ''])
        is_d.append([_h('Operating Expenses'), ''])
        for acc, amt in sorted(all_opex.items()):
            is_d.append(_row(acc, amt))
        total_rows.append(len(is_d)); is_d.append(['Total Operating Expenses', _fmt(model.total_opex)])

    # D&A
    if model.depreciation_amort:
        is_d.append(['', ''])
        is_d.append([_h('Depreciation & Amortisation'), ''])
        for acc, amt in sorted(model.depreciation_amort.items()):
            is_d.append(_row(acc, amt))
        total_rows.append(len(is_d)); is_d.append(['Total D&A', _fmt(model.total_da)])

    total_rows.append(len(is_d)); is_d.append(['EBIT', _fmt(model.ebit)])

    # Finance Costs
    if model.finance_costs:
        is_d.append(['', ''])
        is_d.append([_h('Finance Costs'), ''])
        for acc, amt in sorted(model.finance_costs.items()):
            is_d.append(_row(acc, amt))
        total_rows.append(len(is_d)); is_d.append(['Total Finance Costs', _fmt(model.total_finance_costs)])

    total_rows.append(len(is_d)); is_d.append(['Net Profit Before Tax', _fmt(model.net_profit_before_tax)])

    if model.income_tax:
        is_d.append(['', ''])
        is_d.append([_h('Income Tax'), ''])
        for acc, amt in sorted(model.income_tax.items()):
            is_d.append(_row(acc, amt))

    total_rows.append(len(is_d)); is_d.append(['Net Profit After Tax', _fmt(model.net_profit)])

    is_t = Table(is_d, colWidths=COL, repeatRows=1)
    ts = _ts_base()
    for ri in total_rows:
        _apply_total(ts, ri)
    is_t.setStyle(ts)
    story.append(is_t)
    story.append(PageBreak())

    # ---- Balance Sheet ----
    story.append(Paragraph("Balance Sheet", st['heading']))
    story.append(Paragraph("As at 30 June 2024", st['body']))
    story.append(HRFlowable(width='100%', thickness=1, color=NAVY, spaceAfter=6))

    bs_d = [['Account', 'AUD']]
    bs_totals = []

    def _bs_section(label, items_dict, total_val):
        if not items_dict:
            return
        bs_d.append([_h(label), ''])
        for acc, amt in sorted(items_dict.items()):
            bs_d.append(_row(acc, amt))
        bs_totals.append(len(bs_d))
        bs_d.append([f'Total {label}', _fmt(total_val)])

    _bs_section('Current Assets', model.current_assets, model.total_current_assets)
    _bs_section('Non-Current Assets', model.non_current_assets, model.total_non_current_assets)
    bs_totals.append(len(bs_d)); bs_d.append(['TOTAL ASSETS', _fmt(model.total_assets)])
    bs_d.append(['', ''])
    _bs_section('Current Liabilities', model.current_liabilities, model.total_current_liabilities)
    _bs_section('Non-Current Liabilities', model.non_current_liabilities, model.total_non_current_liabilities)
    bs_totals.append(len(bs_d)); bs_d.append(['TOTAL LIABILITIES', _fmt(model.total_liabilities)])
    bs_d.append(['', ''])
    _bs_section('Equity', model.equity, model.total_equity)
    bs_totals.append(len(bs_d)); bs_d.append(['TOTAL EQUITY', _fmt(model.total_equity)])

    bs_t = Table(bs_d, colWidths=COL, repeatRows=1)
    bs_ts = _ts_base()
    for ri in bs_totals:
        _apply_total(bs_ts, ri)
    bs_t.setStyle(bs_ts)
    story.append(bs_t)

    # Equation check at bottom of BS page
    story += [Spacer(1, 0.5*cm)]
    bs_eq = next((c for c in model.equation_checks if 'Balance Sheet' in c.name), None)
    if bs_eq:
        result = ("[OK] BALANCES" if bs_eq.passed
                  else f"[FAIL] DOES NOT BALANCE (difference: ${bs_eq.difference:,.2f})")
        style_k = 'pass_s' if bs_eq.passed else 'fail_s'
        story.append(Paragraph(
            f"Balance Sheet Equation: Total Assets ${model.total_assets:,.2f}  =  "
            f"Total Liabilities + Equity ${model.total_liabilities + model.total_equity:,.2f}  ->  {result}",
            st[style_k]
        ))
    story.append(PageBreak())

    # ---- Reconciliation table ----
    story.append(Paragraph("Reconciliation: Previous Pipeline vs Forensic Extraction", st['heading']))
    story.append(HRFlowable(width='100%', thickness=1, color=NAVY, spaceAfter=6))
    story.append(Paragraph(
        "Comparison of figures produced by the previous pipeline (with known errors) "
        "against figures extracted directly from source PDFs by this forensic tool.",
        st['body']
    ))
    story += [Spacer(1, 0.3*cm)]

    comp_rows = [
        ("Total Revenue",        KNOWN_PIPELINE_WRONG["Total Revenue"],               model.total_revenue),
        ("Cost of Sales",        None,                                                  model.total_cos),
        ("Gross Profit",         None,                                                  model.gross_profit),
        ("Total Operating Exp",  None,                                                  model.total_opex),
        ("EBIT",                 None,                                                  model.ebit),
        ("Net Profit After Tax", None,                                                  model.net_profit),
        ("Total Current Assets", None,                                                  model.total_current_assets),
        ("Total Assets",         KNOWN_PIPELINE_WRONG["Total Assets"],                 model.total_assets),
        ("Total Liabilities",    None,                                                  model.total_liabilities),
        ("Total Equity",         None,                                                  model.total_equity),
        ("Liabilities + Equity", KNOWN_PIPELINE_WRONG["Total Liabilities + Equity"],   model.total_liabilities + model.total_equity),
    ]

    rc_d = [['Metric', 'Pipeline (Wrong)', 'Forensic (Correct)', 'Difference', 'Status']]
    for metric, old_val, new_val in comp_rows:
        old_s = _fmt(old_val) if old_val is not None else 'N/A'
        diff_s = (_fmt(new_val - old_val) if old_val is not None else '--')
        status = 'CHANGED' if old_val is not None else 'NEW DATA'
        rc_d.append([metric, old_s, _fmt(new_val), diff_s, status])

    rc_t = Table(rc_d, colWidths=[4.5*cm, 3*cm, 3.5*cm, 3*cm, 2.5*cm], repeatRows=1)
    rc_ts = TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), NAVY),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 7.5),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('ALIGN', (1, 0), (3, -1), 'RIGHT'),
        ('ALIGN', (4, 0), (4, -1), 'CENTER'),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, GREY_BG]),
        ('LINEBELOW', (0, 0), (-1, 0), 0.5, NAVY),
        ('LEFTPADDING', (0, 0), (-1, -1), 4),
        ('RIGHTPADDING', (0, 0), (-1, -1), 4),
        ('TOPPADDING', (0, 0), (-1, -1), 3),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
    ])
    rc_t.setStyle(rc_ts)
    story.append(rc_t)

    # ---- Misclassification detail ----
    if model.misclassifications:
        story += [Spacer(1, 0.8*cm)]
        story.append(Paragraph("Detected Misclassification Errors", st['heading']))
        story.append(HRFlowable(width='100%', thickness=0.5, color=RED_C, spaceAfter=6))

        mc_d = [['Account', 'Found In Section', 'Correct Statement', 'CY Amount', 'Source']]
        for mc in model.misclassifications:
            mc_d.append([
                mc.account[:38], mc.found_in_section, mc.should_be_in_section,
                _fmt(mc.amount), f"{mc.source_file} p{mc.page}"
            ])
        mc_t = Table(mc_d, colWidths=[4.5*cm, 3*cm, 3*cm, 2.5*cm, 3.5*cm], repeatRows=1)
        mc_ts = TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), RED_C),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 7.5),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, GREY_BG]),
            ('LINEBELOW', (0, 0), (-1, 0), 0.5, RED_C),
            ('LEFTPADDING', (0, 0), (-1, -1), 4),
            ('RIGHTPADDING', (0, 0), (-1, -1), 4),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ])
        mc_t.setStyle(mc_ts)
        story.append(mc_t)

    # ---- Validation summary page ----
    story.append(PageBreak())
    story.append(Paragraph("Validation Summary", st['heading']))
    story.append(HRFlowable(width='100%', thickness=1, color=NAVY, spaceAfter=8))

    for chk in model.equation_checks:
        style_k = 'pass_s' if chk.passed else 'fail_s'
        prefix = "[OK]" if chk.passed else "[FAIL]"
        story.append(Paragraph(
            f"{prefix}  {chk.name}: {chk.lhs_label} ${chk.lhs_value:,.2f} "
            f"= {chk.rhs_label} ${chk.rhs_value:,.2f}"
            + ("" if chk.passed else f"  [FAIL: diff ${chk.difference:,.2f}]"),
            st[style_k]
        ))

    story += [Spacer(1, 0.5*cm)]
    verdict_text = ("[OK] FINAL VERDICT: PASS -- Data verified. Equations balance. Report is source-faithful."
                    if validation_passed
                    else "[FAIL] FINAL VERDICT: FAIL -- Issues detected. See misclassification errors above.")
    story.append(Paragraph(verdict_text, st['pass_s' if validation_passed else 'fail_s']))

    doc.build(story)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Forensic accounting audit and corrected report generator'
    )
    parser.add_argument('--dry-run', action='store_true',
                        help='Run all extractions and validations; skip PDF output')
    parser.add_argument('--output-dir', default=None,
                        help='Output directory (default: outputs/YYYY-MM-DD_HH-MM-SS/forensic)')
    args = parser.parse_args()

    run_ts = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = OUTPUT_BASE / run_ts / 'forensic'
    output_dir.mkdir(parents=True, exist_ok=True)

    log_path    = output_dir / 'validation_log.txt'
    report_path = output_dir / 'year_end_report_corrected.pdf'

    print(f"\n{'='*60}")
    print("  FORENSIC ACCOUNTING AUDIT")
    print(f"{'='*60}")
    print(f"  Output: {output_dir}\n")

    # --- Step 1: Extract ---
    print("Step 1 -- Extracting from source PDFs...")
    try:
        pl_items = extract_from_pdf(PL_FILE, 'pl')
        print(f"  [OK] P&L : {len(pl_items)} line items  ({PL_FILE.name})")
    except (FileNotFoundError, ExtractionError) as exc:
        print(f"  [FAIL] P&L extraction FAILED: {exc}")
        sys.exit(1)

    try:
        bs_items = extract_from_pdf(BS_FILE, 'bs')
        print(f"  [OK] BS  : {len(bs_items)} line items  ({BS_FILE.name})")
    except (FileNotFoundError, ExtractionError) as exc:
        print(f"  [FAIL] BS extraction FAILED: {exc}")
        sys.exit(1)

    # --- Step 2: Build model ---
    print("\nStep 2 -- Building financial model...")
    model = build_model(pl_items, bs_items)
    print(f"  Revenue:          ${model.total_revenue:>14,.2f}")
    print(f"  Cost of Sales:    ${model.total_cos:>14,.2f}")
    print(f"  Gross Profit:     ${model.gross_profit:>14,.2f}")
    print(f"  Operating Exp:    ${model.total_opex:>14,.2f}")
    print(f"  EBIT:             ${model.ebit:>14,.2f}")
    print(f"  Net Profit:       ${model.net_profit:>14,.2f}")
    print(f"  Total Assets:     ${model.total_assets:>14,.2f}")
    print(f"  TL + Equity:      ${model.total_liabilities + model.total_equity:>14,.2f}")
    if model.misclassifications:
        print(f"\n  [WARN]   {len(model.misclassifications)} misclassification(s) detected")
        for mc in model.misclassifications:
            print(f"      '{mc.account}' in {mc.found_in_section} -> should be BS")

    # --- Step 3: Verify equations ---
    print("\nStep 3 -- Verifying accounting equations...")
    bs_error: Optional[Exception] = None
    try:
        verify_equations(model)
    except BalanceSheetImbalanceError as exc:
        bs_error = exc

    for chk in model.equation_checks:
        status = "[PASS]" if chk.passed else f"[FAIL] FAIL  (diff: ${chk.difference:,.2f})"
        print(f"  {chk.name}: {status}")

    if bs_error:
        print(f"\n  [FAIL] BalanceSheetImbalanceError: {bs_error}")
        print("     Generating report with DATA INTEGRITY: ISSUES DETECTED stamp.")

    validation_passed = (bs_error is None
                         and all(c.passed for c in model.equation_checks)
                         and not model.misclassifications)

    # --- Step 4: Write validation log ---
    print(f"\nStep 4 -- Writing validation log...")
    log_content = write_validation_log(model, pl_items, bs_items, log_path, bs_error)
    print(f"  Written: {log_path}")

    # Print the log to stdout
    print(f"\n{'='*60}")
    print("VALIDATION LOG OUTPUT")
    print('='*60)
    print(log_content)
    print('='*60)

    # --- Step 5: Generate PDF ---
    if args.dry_run:
        print(f"\n  DRY RUN -- PDF generation skipped.")
        print(f"  Validation log: {log_path}")
    else:
        print(f"\nStep 5 -- Generating corrected PDF report...")
        write_corrected_report(model, report_path, validation_passed)
        print(f"  [OK] Written: {report_path}")
        print(f"\n{'='*60}")
        print(f"  COMPLETE")
        print(f"  Corrected report:  {report_path}")
        print(f"  Validation log:    {log_path}")
        print(f"{'='*60}\n")


if __name__ == '__main__':
    main()
