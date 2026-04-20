# CLAUDE.md — ATO Year-End Report Generator (Mock Prototype)

## READ THIS FIRST

You are building a focused proof-of-concept pipeline for generating ATO-compliant year-end financial reports from Xero exports. Read this entire file before writing a single line of code. Every architectural decision is specified — follow it precisely.

---

## WHAT YOU ARE BUILDING

A local Python pipeline with three demonstrable capabilities:
1. **Folder scanner** — reads Xero P&L and Balance Sheet exports (PDF and/or XLSX) from `input/`
2. **Presidio PII redaction** — detects and tokenises AU-specific PII *before* any content reaches the Claude API
3. **ATO year-end report generation** — produces a structured, compliant financial report via LangGraph pipeline

This is NOT the full framework. No workpaper automation, no tax advisory, no batch processing, no OCR for scanned PDFs.

---

## ARCHITECTURE (6-node LangGraph pipeline)

```
input/ (Xero PDF/XLSX exports)
    ↓
Node 1: folder_scanner        — inventory files, route to parser
    ↓
Node 2: pii_redactor          — Presidio + custom AU recognisers → token map → HUMAN CHECKPOINT
    ↓
Node 3: session_embedder      — chunk redacted text → embed → ephemeral ChromaDB (RAG store)
    ↓
Node 4: report_builder        — deterministic Python only, NO LLM — Xero → ATO account mapping
    ↓
Node 5: narrative_writer      — RAG retrieval from ChromaDB → Claude Sonnet (narrative only)
    ↓
Node 6: publisher             — Claude Haiku quality gate → de-anonymise → write outputs
    ↓
outputs/YYYY-MM-DD/           — final report (de-anonymised) + redacted version + token map + logs
```

---

## PROJECT STRUCTURE

```
ato-mock/
├── .env                          # ANTHROPIC_API_KEY only
├── .env.example                  # Template — commit this, not .env
├── run.py                        # Single entry point: python run.py
├── config.py                     # All settings (see Config section below)
├── requirements.txt              # Pinned versions
│
├── input/                        # Drop Xero exports here (PDF and/or XLSX)
├── templates/                    # Firm's P&L and Balance Sheet templates (optional)
├── examples/                     # Example year-end reports for style reference (optional)
│
├── redaction/
│   ├── presidio_engine.py        # Presidio AnalyzerEngine setup — FOCAL COMPONENT
│   ├── au_recognisers.py         # Custom AU recognisers: TFN, DOB, ABN, BSB, bank account
│   ├── token_map.py              # SQLite token store (session-scoped, data/session.db)
│   ├── redactor.py               # Orchestrates: detect → substitute → save map
│   └── deanonymiser.py           # Restores tokens in final output
│
├── ingestion/
│   ├── scanner.py                # Scans input/, identifies file types, routes to parsers
│   ├── pdf_parser.py             # pdfplumber for structured financial PDFs
│   └── xlsx_parser.py            # openpyxl + pandas for Xero Excel exports
│
├── report/
│   ├── account_mapper.py         # Deterministic Xero → ATO category mapping (NO LLM)
│   ├── report_builder.py         # Builds numerical report structure from mapped accounts
│   └── narrative_writer.py       # Claude Sonnet generates commentary sections only
│
├── pipeline/
│   └── graph.py                  # LangGraph StateGraph — 6 nodes, 1 human checkpoint
│
├── data/
│   ├── session.db                # SQLite: token map + run log
│   └── chroma/                   # Ephemeral ChromaDB (auto-deleted after run)
│
└── outputs/
    └── YYYY-MM-DD/
        ├── year_end_report.md           # Final report (de-anonymised)
        ├── year_end_report_redacted.md  # Redacted version (safe to archive)
        ├── token_map.json               # Redaction map (handle securely)
        ├── sources.json                 # Every figure traced to source document
        ├── redaction_summary.txt        # What was detected, confidence scores, tokens
        └── run_log.txt                  # Complete processing log
```

---

## FOCAL COMPONENT — PRESIDIO REDACTION (build this first)

### Installation
```
presidio-analyzer>=2.2.354
presidio-anonymizer>=2.2.354
spacy>=3.7.0
en_core_web_lg  (mandatory — en_core_web_sm misses too many patterns)
```

### Custom AU Recognisers (au_recognisers.py)

Each recogniser must have: regex pattern, context word list, checksum validator (where applicable), entity type label.

**AU_TFN — Tax File Number (HIGHEST PRIORITY)**
- Pattern: `\b\d{3}\s?\d{3}\s?\d{2,3}\b`
- Checksum weights: `[1, 4, 3, 7, 5, 8, 6, 9, 10]` — multiply each digit, sum products, divide by 11, valid if remainder == 0
- Context: `["TFN", "tax file number", "tax file", "ATO", "withholding"]`
- Confidence: 0.65 without checksum, 0.90 with valid checksum
- Log EVERY detection regardless of confidence — Privacy Act critical
- Test with synthetic TFN: `123 456 782`

**AU_DOB — Date of Birth**
- Do NOT use generic date pattern (too many false positives in financial docs)
- Detect: any date format within 50 characters of DOB context words
- Formats: `DD/MM/YYYY`, `DD-MM-YYYY`, `D Month YYYY`, `YYYY-MM-DD`
- Context: `["date of birth", "DOB", "born", "d.o.b", "birth date", "birthdate"]`
- MUST NOT redact reporting period dates (`30 June 2024`, `FY2024`, `Q3 2024`) — DOB context word must be within 50 chars

**AU_ABN — Australian Business Number**
- Pattern: `\b\d{2}\s?\d{3}\s?\d{3}\s?\d{3}\b`
- Checksum: subtract 1 from first digit, multiply each of 11 digits by weights `[10,1,3,5,7,9,11,13,15,17,19]`, sum products, divide by 89, valid if remainder == 0
- Context: `["ABN", "australian business number", "business number", "abn:"]`
- Exclude government ABNs (ATO, ASIC, APRA) if `PRESERVE_REGULATORY_ORGS = True` in config

**AU_BSB — Bank State Branch**
- Pattern: `\b\d{3}-\d{3}\b` (always hyphenated)
- No checksum needed — format is highly specific
- Context: `["BSB", "bank state branch", "branch code", "bsb:"]`

**AU_BANK_ACCOUNT — Bank Account Number**
- Format: 6–10 digits, may have spaces
- REQUIRES context word to activate (too many false positives otherwise)
- Required context: `["account number", "account no", "a/c", "acc no", "direct debit", "account:"]`
- Minimum confidence: 0.80

### Allow-List (NEVER redact these)
- Dollar amounts: any number preceded by `$` or followed by `%`
- Financial period dates: `30 June 2024`, `FY2024`, `FY2025`, `Q1–Q4`
- Chart of accounts codes: `4-1000`, `1-0100`
- Regulatory entities: ATO, ASIC, APRA, RBA, AFCA
- Accounting terms with numbers: `Division 7A`, `Section 8-1`, `s100A`

### Human Checkpoint (blocking input())
After redaction runs, display `redaction_summary.txt` in terminal (use Rich) then:
```
HUMAN APPROVAL REQUIRED BEFORE PROCEEDING.
Type 'approve' to continue or 'abort' to cancel:
```
System cannot proceed until operator types `approve`. Use blocking `input()` — NOT LangGraph interrupt().

### Redaction Summary Format
```
REDACTION SUMMARY — Session [ID] — [Timestamp]
================================================
Files processed: [n]
Total entities detected: [n]

By type:
  AU_TFN:          [n] instances → tokens: AU_TFN_001 ... AU_TFN_00n
  AU_DOB:          [n] instances → tokens: AU_DOB_001 ...
  AU_ABN:          [n] instances → tokens: AU_ABN_001 ...
  AU_BSB:          [n] instances → tokens: AU_BSB_001 ...
  AU_BANK_ACCOUNT: [n] instances → tokens: AU_BANK_ACCOUNT_001 ...
  PERSON:          [n] instances → tokens: PERSON_001 ...
  ORG:             [n] instances → tokens: ORG_001 ...

Low-confidence detections (review recommended):
  [entity type] at [location] — confidence: [score] — token: [token]

Financial figures preserved (NOT redacted): [n]
Reporting dates preserved (NOT redacted): [n]

Token map saved to: data/session.db
Full map export: outputs/[date]/token_map.json
```

---

## ACCOUNT MAPPER (report/account_mapper.py) — NO LLM, PURE PYTHON

```python
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
    # ... expand as needed
}
```

Fuzzy matching fallback order:
1. Lowercase comparison
2. Remove punctuation comparison
3. Keyword extraction (`"rent" in account_name` → `"Occupancy Costs"`)
4. No match → `UNMAPPED` → halt pipeline → blocking `input()` for human resolution

All mapping decisions logged to `run_log.txt` with method used (exact/fuzzy/manual).

---

## ATO REPORT FORMAT

Required sections (all togglable via config):
1. Cover page — entity name, ABN, reporting period, report date, preparer details
2. Directors'/Trustees' Declaration (template text — accountant completes)
3. Income Statement (P&L) — current year + prior year comparative if available
4. Balance Sheet — current year + prior year comparative if available
5. Statement of Cash Flows — only if cash flow data available
6. Notes to Financial Statements — AI-drafted, flagged for review
7. Key Financial Ratios — calculated deterministically
8. ATO Benchmark Comparison — flag outliers (only if `INDUSTRY_CODE` set in config)
9. Accountant Sign-Off Block

Narrative sections watermarked: `⚠️ AI DRAFT — REQUIRES ACCOUNTANT REVIEW`

Reconciliation check: net profit in Income Statement must equal retained earnings movement in Balance Sheet. Fail loudly if mismatch.

---

## CONFIG.PY

```python
# API
ANTHROPIC_API_KEY = None  # Set via .env

# Models
GENERATION_MODEL = "claude-sonnet-4-5"
QUALITY_MODEL = "claude-haiku-4-5"

# Entity & Report
ENTITY_TYPE = "pty_ltd"          # pty_ltd | trust | partnership | sole_trader
TAX_YEAR_END = "30_june"
REPORTING_CURRENCY = "AUD"
ROUNDING = "nearest_dollar"      # nearest_dollar | two_decimal_places

# Sections
INCLUDE_INCOME_STATEMENT = True
INCLUDE_BALANCE_SHEET = True
INCLUDE_CASH_FLOW = True
INCLUDE_NOTES = True
INCLUDE_RATIOS = True
INCLUDE_ATO_BENCHMARKS = False   # Requires INDUSTRY_CODE
INCLUDE_COMPARATIVE_YEAR = True
INCLUDE_VARIANCE_ANALYSIS = True

# Tax Rates
COMPANY_TAX_RATE_BASE = 0.30
COMPANY_TAX_RATE_SBE = 0.25
SBE_TURNOVER_THRESHOLD = 50_000_000
GST_RATE = 0.10

# ATO Benchmarks
INDUSTRY_CODE = None
INDUSTRY_NAME = None

# Presidio
REDACTION_CONFIDENCE_THRESHOLD = 0.75
SHOW_FULL_TOKEN_MAP_IN_TERMINAL = False
PRESERVE_REGULATORY_ORGS = True

# Custom account mapping overrides (take priority over XERO_TO_ATO_MAP)
CUSTOM_ACCOUNT_MAPPINGS = {}

# Paths
INPUT_FOLDER = "./input"
TEMPLATES_FOLDER = "./templates"
EXAMPLES_FOLDER = "./examples"
OUTPUT_FOLDER = "./outputs"
DATA_FOLDER = "./data"
```

---

## REQUIREMENTS.TXT (pin all versions)

```
anthropic>=0.40.0
presidio-analyzer>=2.2.354
presidio-anonymizer>=2.2.354
spacy>=3.7.0
en_core_web_lg @ https://github.com/explosion/spacy-models/releases/download/en_core_web_lg-3.7.1/en_core_web_lg-3.7.1-py3-none-any.whl
pdfplumber>=0.11.0
openpyxl>=3.1.2
pandas>=2.2.0
langchain>=0.3.0
langgraph>=0.2.0
chromadb>=0.5.0
rich>=13.7.0
python-dotenv>=1.0.0
```

---

## INSTALLATION & RUN

```bash
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
python -m spacy download en_core_web_lg

python run.py --validate          # Check setup
python run.py                     # Run pipeline
```

macOS: handle both Apple Silicon and Intel (no architecture-specific packages unless necessary).

---

## SUCCESS CRITERIA

Build is complete when ALL of these pass:
1. Scanner reads a folder with Xero P&L PDF + Balance Sheet XLSX, correctly identifies both
2. Presidio detects and tokenises any TFN or DOB in test documents (test with `123 456 782`)
3. Human checkpoint blocks pipeline before any API call — displays redaction summary
4. Account mapper maps ≥90% of standard Xero account names, clearly flags unmapped
5. Report builder produces numerically correct statements — net profit reconciles to retained earnings
6. Narrative sections generated by Claude Sonnet, watermarked for review
7. De-anonymisation correctly restores all tokens in final report
8. Output folder contains all expected files (redacted + de-anonymised + token map + logs)
9. Token map saved to `data/session.db` and exported to `token_map.json`
10. Run log contains complete record of every processing step

---

## DO NOT BUILD (out of scope for this mock)

- Workpaper automation
- Tax advisory layer
- Batch processing
- LangGraph `interrupt()` for human checkpoint — use `input()` instead
- Rich monitoring dashboard — simple Rich terminal output only
- Scanned PDF OCR
- Any external data sources (Reddit, arXiv, etc.)

---

## COMPLIANCE NOTE

All outputs require review and sign-off by a qualified accountant (CPA or CA) before use for any tax, legal, or business purpose. AI-generated narrative sections are drafts only. This system assists qualified professionals — it does not replace professional judgement.
