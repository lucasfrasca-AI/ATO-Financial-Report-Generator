"""
narrative_writer.py — Gemini generates narrative commentary sections only.

All AI-drafted sections are watermarked: "AI DRAFT - REQUIRES ACCOUNTANT REVIEW"
Retrieval context is sourced from ChromaDB (session RAG store).
"""

import logging
from typing import Optional

from google import genai

import config

logger = logging.getLogger(__name__)

WATERMARK = "\n\n> **AI DRAFT - REQUIRES ACCOUNTANT REVIEW**\n"

SECTION_PROMPTS = {
    "notes_to_financials": """You are an experienced Australian public accountant (CPA).
Draft the Notes to Financial Statements for an ATO-compliant year-end financial report.

Entity details and financial data:
{context}

Requirements:
- Use Australian accounting standards (AASB)
- Cover: basis of preparation, significant accounting policies, key estimates
- Be factual; do not invent figures not in the context
- Flag any areas requiring specific accountant judgement with [ACCOUNTANT REVIEW REQUIRED]
- Keep to approximately 400-600 words""",

    "directors_declaration": """You are an experienced Australian public accountant (CPA).
Draft a Directors' Declaration for an ATO-compliant year-end financial report.

Entity details:
{context}

Requirements:
- Follow standard Australian Corporations Act 2001 s295A format
- Include standard solvency declaration language
- Leave signature/date blocks blank — accountant will complete
- Keep to approximately 150-200 words""",

    "executive_summary": """You are an experienced Australian public accountant (CPA).
Write a brief executive summary of the financial year performance.

Financial data:
{context}

Requirements:
- Highlight key financial metrics: revenue, gross profit, net profit, key ratios
- Note significant movements year-on-year if comparative data available
- Keep factual — do not speculate on causes not evident in the data
- Approximately 150-250 words""",
}


def _format_financials_for_prompt(report_structure: dict, rag_context: str = "") -> str:
    """Format the report structure into a readable prompt context block."""
    lines = []

    if rag_context:
        lines.append("=== Retrieved Context ===")
        lines.append(rag_context)
        lines.append("")

    is_ = report_structure.get("income_statement", {})
    bs = report_structure.get("balance_sheet", {})
    ratios = report_structure.get("ratios", {})

    if is_:
        lines.append("=== Income Statement ===")
        lines.append(f"Total Revenue: ${is_.get('total_revenue', 0):,.0f}")
        lines.append(f"Cost of Sales: ${is_.get('total_cost_of_sales', 0):,.0f}")
        lines.append(f"Gross Profit: ${is_.get('gross_profit', 0):,.0f}")
        lines.append(f"Total Expenses: ${is_.get('total_expenses', 0):,.0f}")
        lines.append(f"Net Profit: ${is_.get('net_profit', 0):,.0f}")
        lines.append("")

    if bs:
        lines.append("=== Balance Sheet ===")
        lines.append(f"Total Assets: ${bs.get('total_assets', 0):,.0f}")
        lines.append(f"Total Liabilities: ${bs.get('total_liabilities', 0):,.0f}")
        lines.append(f"Total Equity: ${bs.get('total_equity', 0):,.0f}")
        lines.append("")

    if ratios:
        lines.append("=== Key Ratios ===")
        for k, v in ratios.items():
            lines.append(f"  {k}: {v}")
        lines.append("")

    return "\n".join(lines)


def generate_section(
    section_key: str,
    report_structure: dict,
    rag_context: str = "",
    api_key: Optional[str] = None,
) -> str:
    """
    Generate a single narrative section using Claude Sonnet.

    Args:
        section_key:      Key from SECTION_PROMPTS dict.
        report_structure: The numerical report dict from report_builder.
        rag_context:      Retrieved text from ChromaDB (may be empty).
        api_key:          Anthropic API key (falls back to config).

    Returns:
        AI-drafted text with watermark prepended.
    """
    if section_key not in SECTION_PROMPTS:
        raise ValueError(f"Unknown section key: {section_key}. Valid: {list(SECTION_PROMPTS)}")

    context = _format_financials_for_prompt(report_structure, rag_context)
    prompt = SECTION_PROMPTS[section_key].format(context=context)

    key = api_key or config.GEMINI_API_KEY
    if not key:
        logger.warning("GEMINI_API_KEY not set — returning placeholder narrative")
        return f"[PLACEHOLDER — GEMINI_API_KEY not configured]{WATERMARK}"

    client = genai.Client(api_key=key)

    logger.info("Requesting narrative section '%s' from %s...", section_key, config.GENERATION_MODEL)

    response = client.models.generate_content(model=config.GENERATION_MODEL, contents=prompt)
    text = response.text
    logger.info("Narrative section '%s' received (%d chars)", section_key, len(text))

    return WATERMARK + text


def generate_all_sections(
    report_structure: dict,
    rag_context: str = "",
    api_key: Optional[str] = None,
) -> dict:
    """
    Generate all narrative sections. Returns {section_key: text}.
    """
    narratives = {}
    for key in SECTION_PROMPTS:
        try:
            narratives[key] = generate_section(
                key, report_structure, rag_context=rag_context, api_key=api_key
            )
        except Exception as exc:
            logger.error("Failed to generate section '%s': %s", key, exc)
            narratives[key] = f"[ERROR generating section: {exc}]{WATERMARK}"
    return narratives
