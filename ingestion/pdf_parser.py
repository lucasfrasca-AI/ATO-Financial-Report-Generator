"""
pdf_parser.py — pdfplumber-based parser for structured financial PDFs.

Extracts:
  - Full text (concatenated page text)
  - Tables (as list of row dicts, one per table detected by pdfplumber)

NOTE: Scanned PDFs (image-only, no embedded text) are explicitly not supported.
      If a PDF yields no text, a clear error is raised — do not silently skip.
"""

import logging
import re
from pathlib import Path

import pdfplumber

logger = logging.getLogger(__name__)

MIN_TEXT_LENGTH = 50  # Below this, assume scanned/image PDF

# ---------------------------------------------------------------------------
# Text-line fallback (used when pdfplumber finds no grid tables)
# ---------------------------------------------------------------------------
# Matches Australian-formatted amounts: 1,234 / 1,234.56 / (1,234) for negatives
_ROW_AMOUNT_RE = re.compile(r'\(?\d{1,3}(?:,\d{3})+(?:\.\d{1,2})?\)?|\(\d+(?:\.\d{1,2})?\)')

# Lines that start with aggregation labels — skip them to avoid double-counting
_SKIP_LINE_RE = re.compile(
    r'^\s*(?:total|sub.?total|grand total|net\s+(?:assets|profit|loss)|'
    r'page\s*\d|prepared\s+by|basis\s*:|period\s*:|entity\s*:|currency\s*:|'
    r'acn\s*:|abn\s+\d|(?:\d{1,2}\s+)?(?:june|july|march|december)\s+\d{4}|'
    r'fy\s*\d{4}|q[1-4]\s+\d{4}|as\s+at\s+|for\s+the\s+(?:year|period))',
    re.IGNORECASE,
)


def _text_to_rows(text: str) -> list:
    """
    Parse extracted page text into synthetic table row dicts when pdfplumber
    finds no grid tables (common with text-layout financial statements).

    Each line that contains at least one formatted number and a non-numeric prefix
    becomes a row: {'Account': name, 'Col1': amt, 'Col2': amt2, ...}
    The report_builder picks the last numeric column as the current-year amount.
    """
    rows = []
    for line in text.split('\n'):
        line = line.strip()
        if not line or len(line) < 5:
            continue
        if _SKIP_LINE_RE.match(line):
            continue
        amounts = _ROW_AMOUNT_RE.findall(line)
        if not amounts:
            continue
        first_match = _ROW_AMOUNT_RE.search(line)
        account = line[:first_match.start()].strip().rstrip('—–-. \t')
        if not account or len(account) < 3:
            continue
        row = {'Account': account}
        for i, amt in enumerate(amounts):
            row[f'Col{i + 1}'] = amt
        rows.append(row)
    return rows


def parse_pdf(path: str) -> dict:
    """
    Parse a PDF file with pdfplumber.

    Returns:
        {
            "name":   filename,
            "path":   absolute path,
            "type":   "pdf",
            "text":   full extracted text,
            "tables": list of table dicts with "headers" and "rows"
        }

    Raises:
        ValueError: if no text is extractable (likely a scanned PDF).
    """
    file_path = Path(path)

    pages_text = []
    all_tables = []

    with pdfplumber.open(str(file_path)) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            # Extract text
            text = page.extract_text() or ""
            pages_text.append(text)

            # Extract tables
            for table in page.extract_tables():
                if not table:
                    continue
                parsed = _parse_raw_table(table, page_num)
                if parsed:
                    all_tables.append(parsed)

    full_text = "\n".join(pages_text).strip()

    # If pdfplumber found no grid tables (common with text-layout financial PDFs),
    # fall back to line-by-line text parsing to create synthetic table rows.
    if not all_tables and full_text:
        logger.warning(
            "No grid tables found in '%s' — using text-line fallback parser", file_path.name
        )
        fallback_rows = _text_to_rows(full_text)
        if fallback_rows:
            all_tables.append({
                "page": 0,
                "headers": list(fallback_rows[0].keys()),
                "rows": fallback_rows,
            })
            logger.info(
                "Text fallback extracted %d rows from '%s'", len(fallback_rows), file_path.name
            )

    if len(full_text) < MIN_TEXT_LENGTH:
        raise ValueError(
            f"PDF '{file_path.name}' appears to be a scanned/image PDF "
            f"(only {len(full_text)} chars extracted). OCR is out of scope for this prototype."
        )

    logger.debug(
        "PDF parsed: %s — %d pages, %d chars, %d tables",
        file_path.name, len(pages_text), len(full_text), len(all_tables),
    )

    return {
        "name": file_path.name,
        "path": str(file_path.resolve()),
        "type": "pdf",
        "text": full_text,
        "tables": all_tables,
    }


def _parse_raw_table(raw_table: list, page_num: int) -> dict | None:
    """
    Convert a pdfplumber raw table (list of lists) to a structured dict.
    First non-empty row is treated as headers.
    """
    # Filter out completely empty rows
    rows = [row for row in raw_table if any(cell for cell in row if cell)]
    if len(rows) < 2:
        return None

    headers = [str(cell or "").strip() for cell in rows[0]]
    data_rows = []
    for row in rows[1:]:
        cells = [str(cell or "").strip() for cell in row]
        # Skip rows that are entirely empty after stripping
        if not any(cells):
            continue
        row_dict = dict(zip(headers, cells))
        data_rows.append(row_dict)

    if not data_rows:
        return None

    return {
        "page": page_num,
        "headers": headers,
        "rows": data_rows,
    }
