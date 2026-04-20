"""
redactor.py — Orchestrates the redaction pipeline.

Steps per document:
  1. Analyse text with Presidio (via presidio_engine.analyze_text)
  2. Sort results right-to-left to preserve offsets during substitution
  3. Substitute each detected span with its token
  4. Save token -> original mapping to SQLite (token_map)
  5. Write redaction_summary.txt
  6. Display summary with Rich and block on human approval (blocking input())

Returns redacted text and a list of RedactionRecord objects.
"""

import logging
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from rich.console import Console
from rich.panel import Panel

import config
from redaction.presidio_engine import analyze_text
from redaction.token_map import TokenMap

logger = logging.getLogger(__name__)
console = Console()

# Patterns for counting preserved items in the summary
_DOLLAR_RE = re.compile(r"\$[\d,]+(?:\.\d+)?")
_DATE_RE = re.compile(
    r"\b(?:30\s+June|31\s+(?:March|December))\s+\d{4}\b"
    r"|\bFY\d{4}\b"
    r"|\bQ[1-4]\s+\d{4}\b",
    re.IGNORECASE,
)


@dataclass
class RedactionRecord:
    entity_type: str
    original: str
    token: str
    confidence: float
    source: str = ""


@dataclass
class RedactionResult:
    redacted_text: str
    records: list = field(default_factory=list)
    financial_figures_preserved: int = 0
    reporting_dates_preserved: int = 0


def redact_text(
    text: str,
    source: str = "",
    token_map: TokenMap = None,
) -> RedactionResult:
    """
    Redact PII from `text`, returning the redacted version and metadata.

    Args:
        text:      Raw text to redact.
        source:    Label identifying the source document (for logging/summary).
        token_map: Optional shared TokenMap; creates a temporary one if omitted.
    """
    own_map = token_map is None
    if own_map:
        token_map = TokenMap(config.DATA_FOLDER + "/session.db")

    results = analyze_text(text, confidence_threshold=config.REDACTION_CONFIDENCE_THRESHOLD)

    # Sort right-to-left so substitutions don't shift subsequent offsets
    results.sort(key=lambda r: r.start, reverse=True)

    records = []
    redacted = text

    for result in results:
        original_span = text[result.start: result.end]
        token = token_map.get_or_create_token(
            entity_type=result.entity_type,
            original=original_span,
            confidence=result.score,
            source=source,
        )
        redacted = redacted[: result.start] + f"[{token}]" + redacted[result.end:]
        records.append(RedactionRecord(
            entity_type=result.entity_type,
            original=original_span,
            token=token,
            confidence=result.score,
            source=source,
        ))

    financial_preserved = len(_DOLLAR_RE.findall(text))
    dates_preserved = len(_DATE_RE.findall(text))

    if own_map:
        token_map.close()

    return RedactionResult(
        redacted_text=redacted,
        records=records,
        financial_figures_preserved=financial_preserved,
        reporting_dates_preserved=dates_preserved,
    )


def redact_documents(
    documents: list,
    session_id: str,
    output_dir: str,
    token_map: TokenMap,
) -> dict:
    """
    Redact a list of documents, write the redaction summary, and block on human approval.

    Args:
        documents:  List of {"name": str, "text": str} dicts.
        session_id: Unique session identifier for summary header.
        output_dir: Path to the output directory for this run.
        token_map:  Shared TokenMap for the session.

    Returns:
        {document_name: RedactionResult}
    """
    results = {}

    for doc in documents:
        name = doc["name"]
        text = doc["text"]
        logger.info("Redacting document: %s", name)
        result = redact_text(text, source=name, token_map=token_map)
        results[name] = result
        token_map.log("INFO", f"Redacted {name}: {len(result.records)} entities found")

    summary = _build_summary(session_id, results, token_map)
    summary_path = Path(output_dir) / "redaction_summary.txt"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(summary, encoding="utf-8")
    logger.info("Redaction summary written to %s", summary_path)

    _display_and_checkpoint(summary, summary_path)

    return results


# ---------------------------------------------------------------------------
# Summary builder
# ---------------------------------------------------------------------------

def _build_summary(
    session_id: str,
    results: dict,
    token_map: TokenMap,
) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    total_entities = sum(len(r.records) for r in results.values())
    total_financial = sum(r.financial_figures_preserved for r in results.values())
    total_dates = sum(r.reporting_dates_preserved for r in results.values())

    by_type = token_map.tokens_by_type()

    lines = [
        f"REDACTION SUMMARY — Session {session_id} — {ts}",
        "=" * 80,
        f"Files processed: {len(results)}",
        f"Total entities detected: {total_entities}",
        "",
        "By type:",
    ]

    for entity_type, records in sorted(by_type.items()):
        tokens_str = " ".join(r["token"] for r in records[:5])
        if len(records) > 5:
            tokens_str += f" ... (+{len(records) - 5} more)"
        lines.append(f"  {entity_type:<20} {len(records)} instances -> tokens: {tokens_str}")

    low_conf = []
    for doc_name, result in results.items():
        for rec in result.records:
            if rec.confidence < 0.80:
                low_conf.append(
                    f"  {rec.entity_type} in '{doc_name}' — confidence: {rec.confidence:.2f} — token: {rec.token}"
                )

    if low_conf:
        lines.append("")
        lines.append("Low-confidence detections (review recommended):")
        lines.extend(low_conf)

    lines += [
        "",
        f"Financial figures preserved (NOT redacted): {total_financial}",
        f"Reporting dates preserved (NOT redacted): {total_dates}",
        "",
        f"Token map saved to: {config.DATA_FOLDER}/session.db",
        "Full map export: outputs/<date>/token_map.json",
    ]

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Human checkpoint
# ---------------------------------------------------------------------------

def _display_and_checkpoint(summary: str, summary_path: Path):
    """Display redaction summary with Rich and block until operator types 'approve'."""
    console.print()
    console.print(Panel(
        summary,
        title="[bold yellow]REDACTION SUMMARY[/bold yellow]",
        border_style="yellow",
        expand=False,
    ))
    console.print()
    console.print(Panel(
        "[bold red]HUMAN APPROVAL REQUIRED BEFORE PROCEEDING.[/bold red]\n"
        f"Summary saved to: [cyan]{summary_path}[/cyan]\n\n"
        "Type [bold green]'approve'[/bold green] to continue or [bold red]'abort'[/bold red] to cancel:",
        border_style="red",
    ))

    while True:
        try:
            response = input("> ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            console.print("[bold red]Aborted.[/bold red]")
            sys.exit(1)

        if response == "approve":
            console.print("[bold green]Approved. Continuing pipeline...[/bold green]")
            break
        elif response == "abort":
            console.print("[bold red]Pipeline aborted by operator.[/bold red]")
            sys.exit(1)
        else:
            console.print("[yellow]Please type 'approve' or 'abort'.[/yellow]")
