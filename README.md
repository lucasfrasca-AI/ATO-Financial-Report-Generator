# ATO Financial Report Generator

A local Python pipeline for generating ATO-compliant year-end financial reports
from Xero exports, with Australian PII redaction, deterministic account mapping,
and AI-assisted narrative generation. Includes a standalone forensic audit tool
for source-faithful PDF extraction and accounting equation verification.

## Architecture

```
input/ (Xero PDF/XLSX exports)
    ↓
Node 1: folder_scanner        — inventory files, route to parser
    ↓
Node 2: pii_redactor          — Presidio + custom AU recognisers → HUMAN CHECKPOINT
    ↓
Node 3: session_embedder      — chunk redacted text → embed → ephemeral ChromaDB
    ↓
Node 4: report_builder        — deterministic Python only, NO LLM — Xero → ATO mapping
    ↓
Node 5: narrative_writer      — RAG retrieval → Gemini Flash (narrative sections only)
    ↓
Node 6: publisher             — Gemini quality gate → de-anonymise → write outputs
    ↓
outputs/YYYY-MM-DD/           — final report + redacted version + token map + logs
```

## Project Structure

```
ato-mock/
├── .env                        # GEMINI_API_KEY only — never commit
├── .env.example                # Template — commit this
├── run.py                      # Entry point: python run.py
├── config.py                   # All settings
├── requirements.txt            # Pinned dependencies
├── forensic_audit.py           # Standalone forensic audit tool
├── test_redaction.py           # Redaction layer test suite
│
├── input/                      # Drop Xero PDF/XLSX exports here
├── redaction/                  # Presidio PII engine + AU recognisers
├── ingestion/                  # File scanner + PDF/XLSX parsers
├── report/                     # Account mapper + report builder + narrative writer
├── pipeline/                   # LangGraph StateGraph (6 nodes)
└── outputs/                    # Generated reports (gitignored)
```

## Redaction Layer

Australian-specific PII detection built on Presidio with custom recognisers:

| Entity | Pattern | Checksum |
|---|---|---|
| AU_TFN | `\d{3}\s?\d{3}\s?\d{2,3}` | Weights `[1,4,3,7,5,8,6,9,10]` mod 11 |
| AU_ABN | `\d{2}\s?\d{3}\s?\d{3}\s?\d{3}` | Weights `[10,1,3,5,7,9,11,13,15,17,19]` mod 89 |
| AU_DOB | Date formats within 50 chars of DOB context | — |
| AU_BSB | `\d{3}-\d{3}` | — |
| AU_BANK_ACCOUNT | 6–10 digits with context word | — |

Financial figures (`$125,000`), reporting dates (`30 June 2024`, `FY2024`),
and regulatory entities (ATO, ASIC, APRA) are allow-listed and never redacted.

A **blocking human checkpoint** displays the full redaction summary before any
content reaches the API — type `approve` to proceed or `abort` to cancel.

## Forensic Audit Tool

`forensic_audit.py` is a standalone tool that:
- Extracts figures directly from source PDFs (raises `ExtractionError` if any figure is missing — no defaults used)
- Validates three accounting equations: Gross Profit, Net Profit, Balance Sheet
- Detects account misclassifications (BS accounts appearing in P&L sections)
- Generates a corrected PDF report with a `DATA INTEGRITY: VERIFIED` stamp and reconciliation table

```bash
python forensic_audit.py              # Full run
python forensic_audit.py --dry-run   # Validate only, no PDF output
python forensic_audit.py --output-dir DIR
```

## Setup

```bash
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # macOS/Linux

pip install -r requirements.txt
python -m spacy download en_core_web_lg

# Create .env
echo GEMINI_API_KEY=your_key_here > .env

python run.py --validate     # Check setup
python run.py                # Run pipeline
```

## Requirements

- Python 3.11+
- Gemini API key — [aistudio.google.com](https://aistudio.google.com)
- Xero P&L and/or Balance Sheet exports in `input/` (PDF or XLSX)

## Configuration

Edit `config.py`:

```python
ENTITY_TYPE = "pty_ltd"          # pty_ltd | trust | partnership | sole_trader
INCLUDE_ATO_BENCHMARKS = False   # Requires INDUSTRY_CODE
CUSTOM_ACCOUNT_MAPPINGS = {}     # Override Xero → ATO mapping
```

## Outputs

Each run produces a timestamped folder in `outputs/`:

| File | Contents |
|---|---|
| `year_end_report.md` | Final report (de-anonymised) |
| `year_end_report_redacted.md` | Redacted version (safe to archive) |
| `token_map.json` | PII token map (handle securely) |
| `sources.json` | Every figure traced to source document |
| `redaction_summary.txt` | Detected entities, confidence scores, tokens |
| `run_log.txt` | Complete processing log |

## Compliance Note

All outputs require review and sign-off by a qualified accountant (CPA or CA)
before use for any tax, legal, or business purpose. AI-generated narrative
sections are watermarked `⚠️ AI DRAFT — REQUIRES ACCOUNTANT REVIEW`.
