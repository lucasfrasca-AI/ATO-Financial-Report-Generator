"""
pipeline/graph.py — LangGraph StateGraph for the ATO Year-End Report Generator.

6-node pipeline:
  Node 1: folder_scanner    — inventory files, route to parser
  Node 2: pii_redactor      — Presidio + AU recognisers -> token map -> HUMAN CHECKPOINT
  Node 3: session_embedder  — chunk redacted text -> embed -> ephemeral ChromaDB
  Node 4: report_builder    — deterministic Python only, NO LLM
  Node 5: narrative_writer  — RAG retrieval -> Gemini (narrative sections only)
  Node 6: publisher         — Gemini quality gate -> de-anonymise -> write outputs

Human checkpoint (blocking input()) is inside Node 2 (pii_redactor), NOT LangGraph interrupt().
"""

import json
import logging
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import TypedDict, Annotated

from langgraph.graph import StateGraph, END
from rich.console import Console

import config
from ingestion.scanner import scan_input_folder
from redaction.redactor import redact_documents
from redaction.deanonymiser import deanonymise_text
from redaction.token_map import TokenMap
from report.report_builder import build_report
from report.narrative_writer import generate_all_sections
from report.pdf_writer import write_pdf, write_redacted_input_pdf

logger = logging.getLogger(__name__)
console = Console()


# ---------------------------------------------------------------------------
# Pipeline state schema
# ---------------------------------------------------------------------------

class PipelineState(TypedDict, total=False):
    session_id: str
    output_dir: str
    run_log_path: str
    token_map_path: str

    # Node 1 outputs
    documents: list                  # Parsed document dicts

    # Node 2 outputs
    redacted_documents: dict         # {doc_name: RedactionResult}
    redacted_texts: list             # [{name, text}] — redacted version for embedding
    redacted_inputs_dir: str         # Path to per-document redacted .txt files

    # Node 3 outputs
    chroma_collection_name: str      # ChromaDB collection name for this session
    rag_context: str                 # Retrieved context for narrative generation

    # Node 4 outputs
    report_structure: dict           # Numerical report (income statement, balance sheet, ratios)
    sources: list                    # Traceability records

    # Node 5 outputs
    narratives: dict                 # {section_key: text}

    # Node 6 outputs
    final_report_path: str
    final_md_path: str


# ---------------------------------------------------------------------------
# Node implementations
# ---------------------------------------------------------------------------

def node_folder_scanner(state: PipelineState) -> PipelineState:
    """Node 1: Scan input folder and parse all supported files."""
    _log(state, "INFO", "Node 1: folder_scanner — scanning input folder")
    console.print("[bold cyan]Node 1: Scanning input folder...[/bold cyan]")

    documents = scan_input_folder(config.INPUT_FOLDER)

    _log(state, "INFO", f"Scanned {len(documents)} document(s): {[d['name'] for d in documents]}")
    console.print(f"  Found {len(documents)} document(s)")

    return {"documents": documents}


def node_pii_redactor(state: PipelineState) -> PipelineState:
    """Node 2: Presidio PII redaction -> token map -> HUMAN CHECKPOINT."""
    _log(state, "INFO", "Node 2: pii_redactor — running Presidio redaction")
    console.print("[bold cyan]Node 2: Running PII redaction...[/bold cyan]")

    documents = state["documents"]
    session_id = state["session_id"]
    output_dir = state["output_dir"]

    token_map = TokenMap(db_path=f"{config.DATA_FOLDER}/session.db")

    docs_for_redaction = [{"name": d["name"], "text": d["text"]} for d in documents]

    # redact_documents writes the summary and blocks on human approval
    redacted_results = redact_documents(
        documents=docs_for_redaction,
        session_id=session_id,
        output_dir=output_dir,
        token_map=token_map,
    )

    # Export token map to JSON
    token_map_path = str(Path(output_dir) / "token_map.json")
    token_map.export_json(token_map_path)
    token_map.close()

    # Write per-document redacted PDFs so the user can see exactly what the AI saw,
    # with PII tokens highlighted in red and a legend listing every entity redacted.
    redacted_inputs_dir = Path(output_dir) / "redacted_inputs"
    redacted_inputs_dir.mkdir(parents=True, exist_ok=True)
    for name, result in redacted_results.items():
        safe_name = name.replace("/", "_").replace("\\", "_")
        pdf_path = redacted_inputs_dir / f"{safe_name}_redacted.pdf"
        try:
            write_redacted_input_pdf(
                name=name,
                redacted_text=result.redacted_text,
                token_records=result.records,
                output_path=str(pdf_path),
                session_id=session_id,
            )
            logger.info("Redacted input PDF saved: %s", pdf_path)
        except Exception as exc:
            logger.warning("Redacted input PDF failed for %s: %s — saving .txt fallback", name, exc)
            txt_path = redacted_inputs_dir / f"{safe_name}_redacted.txt"
            txt_path.write_text(result.redacted_text, encoding="utf-8")

    # Build list of redacted texts for embedding
    redacted_texts = [
        {"name": name, "text": result.redacted_text}
        for name, result in redacted_results.items()
    ]

    _log(state, "INFO", f"Redaction complete. {len(redacted_results)} redacted input(s) in {redacted_inputs_dir}. Token map at {token_map_path}")

    return {
        "redacted_documents": redacted_results,
        "redacted_texts": redacted_texts,
        "token_map_path": token_map_path,
        "redacted_inputs_dir": str(redacted_inputs_dir),
    }


def node_session_embedder(state: PipelineState) -> PipelineState:
    """
    Node 3: Chunk redacted text -> embed -> ephemeral ChromaDB.
    ChromaDB collection is session-scoped and deleted after the run.
    """
    _log(state, "INFO", "Node 3: session_embedder — building RAG store")
    console.print("[bold cyan]Node 3: Building session RAG store...[/bold cyan]")

    redacted_texts = state.get("redacted_texts", [])
    session_id = state["session_id"]
    collection_name = f"session_{session_id.replace('-', '_')}"

    try:
        import chromadb
        from chromadb.utils.embedding_functions import DefaultEmbeddingFunction

        chroma_path = str(Path(config.DATA_FOLDER) / "chroma")
        client = chromadb.PersistentClient(path=chroma_path)
        collection = client.get_or_create_collection(
            name=collection_name,
            embedding_function=DefaultEmbeddingFunction(),
        )

        chunk_size = 500
        doc_ids = []
        doc_texts = []

        for doc in redacted_texts:
            text = doc["text"]
            name = doc["name"]
            chunks = [text[i: i + chunk_size] for i in range(0, len(text), chunk_size)]
            for idx, chunk in enumerate(chunks):
                doc_ids.append(f"{name}__chunk_{idx}")
                doc_texts.append(chunk)

        if doc_texts:
            collection.add(documents=doc_texts, ids=doc_ids)
            logger.info("Embedded %d chunks into ChromaDB collection '%s'", len(doc_texts), collection_name)

        # Retrieve a short context sample for narrative generation
        # Use a generic financial query — the RAG context enriches narrative sections
        query = "financial performance revenue expenses profit assets liabilities"
        results = collection.query(query_texts=[query], n_results=min(5, len(doc_texts)))
        rag_context = "\n\n".join(results["documents"][0]) if results["documents"] else ""

    except Exception as exc:
        logger.warning("ChromaDB embedding failed (non-fatal): %s", exc)
        rag_context = ""
        collection_name = ""

    _log(state, "INFO", f"RAG store built: collection='{collection_name}'")

    return {
        "chroma_collection_name": collection_name,
        "rag_context": rag_context,
    }


def node_report_builder(state: PipelineState) -> PipelineState:
    """Node 4: Deterministic numerical report construction. NO LLM."""
    _log(state, "INFO", "Node 4: report_builder — building financial statements")
    console.print("[bold cyan]Node 4: Building financial report (deterministic)...[/bold cyan]")

    documents = state["documents"]
    run_log_path = state.get("run_log_path")

    try:
        report_structure = build_report(documents, run_log_path=run_log_path)
    except ValueError as exc:
        # Reconciliation failure — log and re-raise
        _log(state, "ERROR", f"Report builder error: {exc}")
        raise

    sources = report_structure.get("sources", [])

    # Write sources.json
    sources_path = Path(state["output_dir"]) / "sources.json"
    sources_path.write_text(json.dumps(sources, indent=2), encoding="utf-8")

    _log(state, "INFO", f"Report built. {len(sources)} source records. sources.json written.")

    return {
        "report_structure": report_structure,
        "sources": sources,
    }


def node_narrative_writer(state: PipelineState) -> PipelineState:
    """Node 5: Claude Sonnet generates AI commentary sections (watermarked)."""
    _log(state, "INFO", "Node 5: narrative_writer — generating AI narrative sections")
    console.print("[bold cyan]Node 5: Generating AI narrative sections (Claude Sonnet)...[/bold cyan]")

    report_structure = state.get("report_structure", {})
    rag_context = state.get("rag_context", "")

    narratives = generate_all_sections(
        report_structure=report_structure,
        rag_context=rag_context,
        api_key=config.GEMINI_API_KEY,
    )

    _log(state, "INFO", f"Narrative sections generated: {list(narratives)}")

    return {"narratives": narratives}


def node_publisher(state: PipelineState) -> PipelineState:
    """
    Node 6: Quality gate (Gemini) -> de-anonymise -> write all output files.
    Runs Gemini as a lightweight check that the report is internally consistent.
    """
    _log(state, "INFO", "Node 6: publisher — quality gate + writing outputs")
    console.print("[bold cyan]Node 6: Quality gate + writing outputs...[/bold cyan]")

    output_dir = Path(state["output_dir"])
    report_structure = state.get("report_structure", {})
    narratives = state.get("narratives", {})
    session_id = state["session_id"]

    # Build the redacted report (tokens still present)
    redacted_md = _render_report_md(report_structure, narratives, redacted=True, session_id=session_id)

    # Claude Haiku quality gate
    redacted_md = _haiku_quality_gate(redacted_md)

    # De-anonymise: restore all tokens for the final report
    token_map = TokenMap(db_path=f"{config.DATA_FOLDER}/session.db")
    final_md = deanonymise_text(redacted_md, token_map)
    token_map.close()

    final_md_path = output_dir / "year_end_report.md"
    final_md_path.write_text(final_md, encoding="utf-8")

    # --- Final PDF — de-anonymise narratives before writing ---
    final_narratives = {}
    token_map_pdf = TokenMap(db_path=f"{config.DATA_FOLDER}/session.db")
    for k, v in narratives.items():
        final_narratives[k] = deanonymise_text(v, token_map_pdf)
    token_map_pdf.close()

    final_pdf_path = output_dir / "year_end_report.pdf"
    try:
        write_pdf(report_structure, final_narratives, str(final_pdf_path), redacted=False, session_id=session_id)
    except Exception as exc:
        logger.warning("PDF generation failed (non-fatal): %s", exc)
        final_pdf_path = None

    redacted_inputs_dir = state.get("redacted_inputs_dir", "")
    n_redacted = len(list(Path(redacted_inputs_dir).glob("*.txt"))) if redacted_inputs_dir else 0

    _log(state, "INFO", f"Output files written to {output_dir}")
    console.print(f"\n[bold green]Pipeline complete. Outputs written to {output_dir}[/bold green]")
    console.print(f"  [cyan]Final PDF:[/cyan]        {final_pdf_path}")
    console.print(f"  [cyan]Final Markdown:[/cyan]   {final_md_path}")
    console.print(f"  [cyan]Redacted inputs:[/cyan]  {redacted_inputs_dir}/ ({n_redacted} file(s))")
    console.print(f"  [cyan]Token map:[/cyan]        {state.get('token_map_path')}")

    return {
        "final_report_path": str(final_pdf_path) if final_pdf_path else "",
        "final_md_path": str(final_md_path),
    }


# ---------------------------------------------------------------------------
# Report renderer
# ---------------------------------------------------------------------------

def _render_report_md(
    report_structure: dict,
    narratives: dict,
    redacted: bool,
    session_id: str,
) -> str:
    """Render the full report as Markdown."""
    is_ = report_structure.get("income_statement", {})
    bs = report_structure.get("balance_sheet", {})
    ratios = report_structure.get("ratios", {})

    today = datetime.now(timezone.utc).strftime("%d %B %Y")
    label = " (REDACTED VERSION)" if redacted else ""

    lines = [
        f"# ATO Year-End Financial Report{label}",
        f"**Session:** {session_id}  ",
        f"**Report Date:** {today}  ",
        f"**Currency:** {config.REPORTING_CURRENCY}  ",
        f"**Entity Type:** {config.ENTITY_TYPE}  ",
        "",
        "---",
        "",
    ]

    # Executive Summary
    if "executive_summary" in narratives:
        lines += ["## Executive Summary", narratives["executive_summary"], ""]

    # Income Statement
    if is_ and config.INCLUDE_INCOME_STATEMENT:
        lines += [
            "## Income Statement",
            "",
            "| Account | Amount |",
            "|---------|--------|",
        ]
        for cat, amt in is_.get("revenue", {}).items():
            lines.append(f"| {cat} | ${amt:,.0f} |")
        lines.append(f"| **Total Revenue** | **${is_.get('total_revenue', 0):,.0f}** |")
        for cat, amt in is_.get("cost_of_sales", {}).items():
            lines.append(f"| {cat} | (${abs(amt):,.0f}) |")
        lines.append(f"| **Gross Profit** | **${is_.get('gross_profit', 0):,.0f}** |")
        for cat, amt in is_.get("expenses", {}).items():
            lines.append(f"| {cat} | (${abs(amt):,.0f}) |")
        lines.append(f"| **Net Profit** | **${is_.get('net_profit', 0):,.0f}** |")
        lines.append("")

    # Balance Sheet
    if bs and config.INCLUDE_BALANCE_SHEET:
        lines += [
            "## Balance Sheet",
            "",
            "| Account | Amount |",
            "|---------|--------|",
            "| **Current Assets** | |",
        ]
        for cat, amt in bs.get("current_assets", {}).items():
            lines.append(f"| {cat} | ${amt:,.0f} |")
        lines.append(f"| **Total Current Assets** | **${bs.get('total_current_assets', 0):,.0f}** |")
        lines.append("| **Non-Current Assets** | |")
        for cat, amt in bs.get("non_current_assets", {}).items():
            lines.append(f"| {cat} | ${amt:,.0f} |")
        lines.append(f"| **Total Assets** | **${bs.get('total_assets', 0):,.0f}** |")
        lines.append("| **Current Liabilities** | |")
        for cat, amt in bs.get("current_liabilities", {}).items():
            lines.append(f"| {cat} | ${amt:,.0f} |")
        lines.append(f"| **Total Liabilities** | **${bs.get('total_liabilities', 0):,.0f}** |")
        lines.append("| **Equity** | |")
        for cat, amt in bs.get("equity", {}).items():
            lines.append(f"| {cat} | ${amt:,.0f} |")
        lines.append(f"| **Total Equity** | **${bs.get('total_equity', 0):,.0f}** |")
        lines.append("")

    # Key Ratios
    if ratios and config.INCLUDE_RATIOS:
        lines += ["## Key Financial Ratios", ""]
        for k, v in ratios.items():
            label_str = k.replace("_", " ").title()
            lines.append(f"- **{label_str}:** {v}")
        lines.append("")

    # Notes to Financial Statements
    if "notes_to_financials" in narratives and config.INCLUDE_NOTES:
        lines += ["## Notes to Financial Statements", narratives["notes_to_financials"], ""]

    # Directors' Declaration
    if "directors_declaration" in narratives:
        lines += ["## Directors' Declaration", narratives["directors_declaration"], ""]

    # Compliance footer
    lines += [
        "---",
        "",
        "*All AI-drafted narrative sections are marked AI DRAFT and require review and sign-off "
        "by a qualified accountant (CPA or CA) before use for any tax, legal, or business purpose.*",
    ]

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Claude Haiku quality gate
# ---------------------------------------------------------------------------

def _haiku_quality_gate(report_md: str) -> str:
    """
    Run Gemini as a lightweight consistency check.
    Returns the report unchanged if API key not set or Gemini flags no issues.
    Only checks internal consistency — does not rewrite content.
    """
    api_key = config.GEMINI_API_KEY
    if not api_key:
        logger.warning("Quality gate skipped — GEMINI_API_KEY not set")
        return report_md

    try:
        from google import genai

        client = genai.Client(api_key=api_key)

        prompt = (
            "You are reviewing a draft ATO year-end financial report for internal consistency.\n"
            "Check ONLY:\n"
            "1. Are all section headers present?\n"
            "2. Are there any obvious arithmetic errors visible in the tables?\n"
            "3. Are all AI DRAFT sections watermarked?\n\n"
            "If everything looks consistent, respond with exactly: QUALITY GATE PASSED\n"
            "If there are issues, list them briefly (under 100 words). Do not rewrite the report.\n\n"
            f"Report (first 3000 chars):\n{report_md[:3000]}"
        )

        response = client.models.generate_content(model=config.QUALITY_MODEL, contents=prompt)
        result = response.text.strip()

        if result != "QUALITY GATE PASSED":
            logger.warning("Quality gate flagged issues: %s", result)
            report_md += f"\n\n---\n\n**Quality Gate Notes (Gemini):**\n{result}\n"
        else:
            logger.info("Quality gate passed.")

    except Exception as exc:
        logger.warning("Quality gate failed (non-fatal): %s", exc)

    return report_md


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------

def build_graph():
    """Build and compile the 6-node LangGraph StateGraph."""
    graph = StateGraph(PipelineState)

    graph.add_node("folder_scanner", node_folder_scanner)
    graph.add_node("pii_redactor", node_pii_redactor)
    graph.add_node("session_embedder", node_session_embedder)
    graph.add_node("report_builder", node_report_builder)
    graph.add_node("narrative_writer", node_narrative_writer)
    graph.add_node("publisher", node_publisher)

    graph.set_entry_point("folder_scanner")
    graph.add_edge("folder_scanner", "pii_redactor")
    graph.add_edge("pii_redactor", "session_embedder")
    graph.add_edge("session_embedder", "report_builder")
    graph.add_edge("report_builder", "narrative_writer")
    graph.add_edge("narrative_writer", "publisher")
    graph.add_edge("publisher", END)

    return graph.compile()


# ---------------------------------------------------------------------------
# Helper: run log
# ---------------------------------------------------------------------------

def _log(state: PipelineState, level: str, message: str):
    run_log_path = state.get("run_log_path")
    logger.info(message)
    if run_log_path:
        try:
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            with open(run_log_path, "a", encoding="utf-8") as f:
                f.write(f"[{ts}] [{level}] {message}\n")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_pipeline() -> dict:
    """
    Initialise session state and run the full 6-node pipeline.
    Returns the final pipeline state.
    """
    session_id = str(uuid.uuid4())[:8]
    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    output_dir = str(Path(config.OUTPUT_FOLDER) / run_ts)
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    run_log_path = str(Path(output_dir) / "run_log.txt")

    initial_state: PipelineState = {
        "session_id": session_id,
        "output_dir": output_dir,
        "run_log_path": run_log_path,
    }

    console.print(f"\n[bold]ATO Year-End Report Generator[/bold]")
    console.print(f"Session: {session_id} | Output: {output_dir}\n")

    pipeline = build_graph()
    final_state = pipeline.invoke(initial_state)

    # Clean up ephemeral ChromaDB after run
    chroma_path = Path(config.DATA_FOLDER) / "chroma"
    if chroma_path.exists():
        try:
            shutil.rmtree(str(chroma_path))
            logger.info("Ephemeral ChromaDB cleaned up.")
        except Exception as exc:
            logger.warning("ChromaDB cleanup failed: %s", exc)

    return final_state
