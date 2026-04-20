"""
scanner.py — Scans the input/ folder, identifies file types, and routes to parsers.

Returns a list of parsed document dicts:
  {
    "name":      str,         # filename
    "path":      str,         # absolute path
    "type":      str,         # "pdf" | "xlsx"
    "text":      str,         # full extracted text (for redaction)
    "tables":    list[dict],  # structured data (for account mapping)
    "doc_type":  str,         # "pl" | "bs" | "unknown" — detected from filename/content
  }

Only .pdf and .xlsx/.xls files are processed. Others are logged and skipped.
Scanned PDFs (no extractable text) raise a clear error — OCR is out of scope.
"""

import logging
from pathlib import Path

from ingestion.pdf_parser import parse_pdf
from ingestion.xlsx_parser import parse_xlsx

logger = logging.getLogger(__name__)

SUPPORTED_EXTENSIONS = {".pdf", ".xlsx", ".xls"}

# Keywords used to detect document type from filename or first-page text
_PL_KEYWORDS = ["profit", "loss", "income statement", "p&l"]
_BS_KEYWORDS = ["balance sheet", "financial position", "assets", "liabilities"]


def scan_input_folder(input_folder: str) -> list:
    """
    Scan `input_folder` and return a list of parsed document dicts.
    Skips unsupported file types with a warning. Raises on empty folder.
    """
    folder = Path(input_folder)
    if not folder.exists():
        raise FileNotFoundError(f"Input folder not found: {folder}")

    # rglob("*") recursively traverses all subdirectories
    files = sorted(f for f in folder.rglob("*") if f.is_file())
    if not files:
        raise ValueError(f"Input folder is empty: {folder}")

    documents = []
    for file in files:
        ext = file.suffix.lower()
        if ext not in SUPPORTED_EXTENSIONS:
            logger.warning("Skipping unsupported file type: %s", file.relative_to(folder).as_posix())
            continue

        rel_name = file.relative_to(folder).as_posix()  # e.g. "subfolder/file.pdf"
        logger.info("Processing: %s", rel_name)
        try:
            if ext == ".pdf":
                doc = parse_pdf(str(file))
            else:  # .xlsx / .xls
                doc = parse_xlsx(str(file))
        except Exception as exc:
            logger.error("Failed to parse %s: %s", rel_name, exc)
            raise

        # Override name with relative path for traceability in nested folders
        doc["name"] = rel_name
        doc["path"] = str(file)
        doc["doc_type"] = _detect_doc_type(rel_name, doc.get("text", ""))
        documents.append(doc)
        logger.info(
            "Parsed %s as %s (%d chars text, %d tables)",
            doc["name"], doc["doc_type"], len(doc.get("text", "")), len(doc.get("tables", [])),
        )

    if not documents:
        raise ValueError("No supported files found in input folder.")

    return documents


def _detect_doc_type(filename: str, text: str) -> str:
    """Heuristically detect whether a document is a P&L or Balance Sheet."""
    combined = (filename + " " + text[:500]).lower()

    pl_score = sum(1 for kw in _PL_KEYWORDS if kw in combined)
    bs_score = sum(1 for kw in _BS_KEYWORDS if kw in combined)

    if pl_score > bs_score:
        return "pl"
    elif bs_score > pl_score:
        return "bs"
    else:
        return "unknown"
