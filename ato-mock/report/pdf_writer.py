"""
pdf_writer.py — Generates a formatted A4 PDF year-end financial report using reportlab.

Public interface:
    write_pdf(report_structure, narratives, output_path, redacted, session_id) -> None

Sections: cover page, income statement, balance sheet, key ratios, narrative sections.
Negative amounts rendered as ($1,234) in red. Footer on every page. Diagonal "REDACTED"
watermark on every page when redacted=True.
"""

import logging
import re
import math
from collections import defaultdict
from datetime import date
from io import BytesIO

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (
    HRFlowable,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)
from reportlab.platypus.doctemplate import PageTemplate
from reportlab.platypus.frames import Frame

import config

logger = logging.getLogger(__name__)

# Token placeholder pattern used in redacted text: [AU_TFN_001]
_TOKEN_RE = re.compile(r'\[([A-Z][A-Z0-9_]+_\d{3})\]')

# ---------------------------------------------------------------------------
# Colours
# ---------------------------------------------------------------------------
NAVY = colors.HexColor("#1B3A6B")
GREY_BG = colors.HexColor("#F2F2F2")
RED = colors.HexColor("#CC0000")
WHITE = colors.white
BLACK = colors.black
LIGHT_GREY = colors.HexColor("#CCCCCC")


# ---------------------------------------------------------------------------
# Styles
# ---------------------------------------------------------------------------
def _build_styles():
    base = getSampleStyleSheet()
    styles = {}

    styles["cover_title"] = ParagraphStyle(
        "cover_title",
        parent=base["Title"],
        fontSize=22,
        textColor=NAVY,
        spaceAfter=12,
        alignment=TA_CENTER,
        fontName="Helvetica-Bold",
    )
    styles["cover_sub"] = ParagraphStyle(
        "cover_sub",
        parent=base["Normal"],
        fontSize=11,
        textColor=BLACK,
        spaceAfter=6,
        alignment=TA_CENTER,
        fontName="Helvetica",
    )
    styles["cover_notice"] = ParagraphStyle(
        "cover_notice",
        parent=base["Normal"],
        fontSize=8,
        textColor=colors.HexColor("#666666"),
        spaceAfter=4,
        alignment=TA_CENTER,
        fontName="Helvetica-Oblique",
    )
    styles["section_heading"] = ParagraphStyle(
        "section_heading",
        parent=base["Heading1"],
        fontSize=13,
        textColor=NAVY,
        spaceBefore=18,
        spaceAfter=6,
        fontName="Helvetica-Bold",
    )
    styles["narrative_body"] = ParagraphStyle(
        "narrative_body",
        parent=base["Normal"],
        fontSize=9,
        spaceAfter=6,
        leading=14,
        fontName="Helvetica",
    )
    styles["watermark_draft"] = ParagraphStyle(
        "watermark_draft",
        parent=base["Normal"],
        fontSize=8,
        textColor=colors.HexColor("#CC0000"),
        spaceAfter=10,
        fontName="Helvetica-Oblique",
    )
    return styles


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _fmt_amount(value, red_negative: bool = True):
    """Format a numeric value as a right-aligned currency string."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return str(value) if value else "-"
    if v < 0:
        text = f"(${abs(v):,.0f})"
        return f'<font color="#{RED.hexval()[2:]}"><b>{text}</b></font>' if red_negative else text
    return f"${v:,.0f}"


def _section_name(key: str) -> str:
    return key.replace("_", " ").title()


# ---------------------------------------------------------------------------
# Page callback — footer + optional diagonal watermark
# ---------------------------------------------------------------------------
class _PageDecorator:
    def __init__(self, session_id: str, redacted: bool, total_pages_ref: list):
        self.session_id = session_id
        self.redacted = redacted
        self.total_pages_ref = total_pages_ref
        self.report_date = date.today().strftime("%d %B %Y")

    def __call__(self, canvas, doc):
        canvas.saveState()

        # Footer
        footer_text = (
            f"Page {doc.page}  |  Session: {self.session_id}  |  Generated: {self.report_date}"
        )
        canvas.setFont("Helvetica", 7)
        canvas.setFillColor(colors.HexColor("#888888"))
        canvas.drawCentredString(A4[0] / 2, 1.2 * cm, footer_text)

        # Diagonal REDACTED watermark
        if self.redacted:
            canvas.setFont("Helvetica-Bold", 52)
            canvas.setFillColor(colors.Color(0.7, 0.7, 0.7, alpha=0.15))
            canvas.translate(A4[0] / 2, A4[1] / 2)
            canvas.rotate(45)
            canvas.drawCentredString(0, 0, "REDACTED")

        canvas.restoreState()


# ---------------------------------------------------------------------------
# Table builders
# ---------------------------------------------------------------------------
def _table_style_financial():
    return TableStyle([
        # Header row
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 9),
        ("ALIGN", (1, 0), (-1, 0), "RIGHT"),
        # Body
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 1), (-1, -1), 9),
        ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, GREY_BG]),
        # Grid
        ("LINEBELOW", (0, 0), (-1, 0), 0.5, NAVY),
        ("LINEBELOW", (0, -1), (-1, -1), 0.5, NAVY),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ])


def _total_row_style(row_index: int) -> list:
    return [
        ("BACKGROUND", (0, row_index), (-1, row_index), GREY_BG),
        ("FONTNAME", (0, row_index), (-1, row_index), "Helvetica-Bold"),
        ("LINEABOVE", (0, row_index), (-1, row_index), 0.5, LIGHT_GREY),
    ]


def _build_income_statement_table(is_: dict) -> Table:
    col_widths = [10 * cm, 5 * cm]
    data = [["Account", "Amount (AUD)"]]

    revenue_items = is_.get("revenue", {})
    for account, amount in revenue_items.items():
        data.append([account, Paragraph(_fmt_amount(amount), ParagraphStyle("r", alignment=TA_RIGHT, fontSize=9))])

    total_rev_row = len(data)
    data.append(["Total Revenue", Paragraph(_fmt_amount(is_.get("total_revenue", 0)), ParagraphStyle("r", alignment=TA_RIGHT, fontSize=9, fontName="Helvetica-Bold"))])

    cos_items = is_.get("cost_of_sales", {})
    if cos_items:
        data.append(["", ""])
        data.append(["Cost of Sales", ""])
        for account, amount in cos_items.items():
            data.append([f"  {account}", Paragraph(_fmt_amount(amount), ParagraphStyle("r", alignment=TA_RIGHT, fontSize=9))])
        total_cos_row = len(data)
        data.append(["Total Cost of Sales", Paragraph(_fmt_amount(is_.get("total_cost_of_sales", 0)), ParagraphStyle("r", alignment=TA_RIGHT, fontSize=9, fontName="Helvetica-Bold"))])
        gp_row = len(data)
        data.append(["Gross Profit", Paragraph(_fmt_amount(is_.get("gross_profit", 0)), ParagraphStyle("r", alignment=TA_RIGHT, fontSize=9, fontName="Helvetica-Bold"))])
    else:
        total_cos_row = gp_row = None

    exp_items = is_.get("expenses", {})
    if exp_items:
        data.append(["", ""])
        data.append(["Operating Expenses", ""])
        for account, amount in exp_items.items():
            data.append([f"  {account}", Paragraph(_fmt_amount(amount), ParagraphStyle("r", alignment=TA_RIGHT, fontSize=9))])
        total_exp_row = len(data)
        data.append(["Total Expenses", Paragraph(_fmt_amount(is_.get("total_expenses", 0)), ParagraphStyle("r", alignment=TA_RIGHT, fontSize=9, fontName="Helvetica-Bold"))])

    np_row = len(data)
    data.append(["Net Profit / (Loss)", Paragraph(_fmt_amount(is_.get("net_profit", 0)), ParagraphStyle("r", alignment=TA_RIGHT, fontSize=9, fontName="Helvetica-Bold"))])

    t = Table(data, colWidths=col_widths, repeatRows=1)
    style = _table_style_financial()
    for ri in [total_rev_row, np_row]:
        style.add(*_total_row_style(ri)[0])
        style.add(*_total_row_style(ri)[1])
        style.add(*_total_row_style(ri)[2])
    t.setStyle(style)
    return t


def _build_balance_sheet_table(bs: dict) -> Table:
    col_widths = [10 * cm, 5 * cm]
    data = [["Account", "Amount (AUD)"]]
    total_rows = []

    def _amt(v):
        return Paragraph(_fmt_amount(v), ParagraphStyle("r", alignment=TA_RIGHT, fontSize=9))

    def _amt_bold(v):
        return Paragraph(_fmt_amount(v), ParagraphStyle("rb", alignment=TA_RIGHT, fontSize=9, fontName="Helvetica-Bold"))

    def _header(label):
        return Paragraph(f"<b>{label}</b>", ParagraphStyle("h", fontSize=9, fontName="Helvetica-Bold"))

    # ---- Assets ----
    cur_assets = bs.get("current_assets", {})
    non_cur_assets = bs.get("non_current_assets", {})
    if cur_assets:
        data.append([_header("Current Assets"), ""])
        for account, amount in cur_assets.items():
            data.append([f"    {account}", _amt(amount)])
        total_rows.append(len(data))
        data.append(["  Total Current Assets", _amt_bold(bs.get("total_current_assets", 0))])
    if non_cur_assets:
        data.append([_header("Non-Current Assets"), ""])
        for account, amount in non_cur_assets.items():
            data.append([f"    {account}", _amt(amount)])
        total_rows.append(len(data))
        data.append(["  Total Non-Current Assets", _amt_bold(bs.get("total_non_current_assets", 0))])
    total_rows.append(len(data))
    data.append(["Total Assets", _amt_bold(bs.get("total_assets", 0))])

    data.append(["", ""])

    # ---- Liabilities ----
    cur_liab = bs.get("current_liabilities", {})
    non_cur_liab = bs.get("non_current_liabilities", {})
    if cur_liab:
        data.append([_header("Current Liabilities"), ""])
        for account, amount in cur_liab.items():
            data.append([f"    {account}", _amt(amount)])
        total_rows.append(len(data))
        data.append(["  Total Current Liabilities", _amt_bold(bs.get("total_current_liabilities", 0))])
    if non_cur_liab:
        data.append([_header("Non-Current Liabilities"), ""])
        for account, amount in non_cur_liab.items():
            data.append([f"    {account}", _amt(amount)])
        total_rows.append(len(data))
        data.append(["  Total Non-Current Liabilities", _amt_bold(bs.get("total_non_current_liabilities", 0))])
    total_rows.append(len(data))
    data.append(["Total Liabilities", _amt_bold(bs.get("total_liabilities", 0))])

    data.append(["", ""])

    # ---- Equity ----
    equity = bs.get("equity", {})
    if equity:
        data.append([_header("Equity"), ""])
        for account, amount in equity.items():
            data.append([f"    {account}", _amt(amount)])
    total_rows.append(len(data))
    data.append(["Total Equity", _amt_bold(bs.get("total_equity", 0))])

    t = Table(data, colWidths=col_widths, repeatRows=1)
    style = _table_style_financial()
    for ri in total_rows:
        style.add(*_total_row_style(ri)[0])
        style.add(*_total_row_style(ri)[1])
        style.add(*_total_row_style(ri)[2])
    t.setStyle(style)
    return t


def _build_ratios_table(ratios: dict) -> Table:
    col_widths = [10 * cm, 5 * cm]
    data = [["Ratio", "Value"]]
    for k, v in ratios.items():
        data.append([_section_name(k), str(v)])
    t = Table(data, colWidths=col_widths, repeatRows=1)
    t.setStyle(_table_style_financial())
    return t


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------
def write_pdf(
    report_structure: dict,
    narratives: dict,
    output_path: str,
    redacted: bool,
    session_id: str,
) -> None:
    """
    Write a formatted A4 PDF financial report to output_path.

    Args:
        report_structure: Numerical report dict from report_builder.
        narratives:        Dict of {section_key: narrative_text} from narrative_writer.
        output_path:       Absolute path for the output PDF file.
        redacted:          If True, every page gets a diagonal "REDACTED" watermark.
        session_id:        Session identifier shown in the page footer.
    """
    logger.info("Writing PDF report to %s (redacted=%s)", output_path, redacted)

    styles = _build_styles()
    total_pages_ref = [0]
    page_decorator = _PageDecorator(session_id, redacted, total_pages_ref)

    doc = SimpleDocTemplate(
        output_path,
        pagesize=A4,
        rightMargin=2 * cm,
        leftMargin=2 * cm,
        topMargin=2.5 * cm,
        bottomMargin=2.5 * cm,
    )

    # Attach footer/watermark via onPage
    frame = Frame(
        doc.leftMargin, doc.bottomMargin,
        doc.width, doc.height,
        id="normal",
    )
    template = PageTemplate(id="main", frames=[frame], onPage=page_decorator)
    doc.addPageTemplates([template])

    story = []
    today = date.today()

    # ------------------------------------------------------------------ Cover
    story.append(Spacer(1, 4 * cm))
    entity_type = config.ENTITY_TYPE.replace("_", " ").title()
    story.append(Paragraph("Year-End Financial Report", styles["cover_title"]))
    story.append(Spacer(1, 0.5 * cm))
    story.append(Paragraph(f"Entity Type: {entity_type}", styles["cover_sub"]))
    story.append(Paragraph(f"Reporting Currency: {config.REPORTING_CURRENCY}", styles["cover_sub"]))
    tax_year = config.TAX_YEAR_END.replace("_", " ").title()
    story.append(Paragraph(f"Tax Year End: {tax_year}", styles["cover_sub"]))
    story.append(Paragraph(f"Report Date: {today.strftime('%d %B %Y')}", styles["cover_sub"]))
    story.append(Paragraph(f"Session ID: {session_id}", styles["cover_sub"]))
    story.append(Spacer(1, 2 * cm))
    if redacted:
        story.append(Paragraph(
            "REDACTED VERSION — Contains anonymised tokens. Not for external distribution.",
            styles["cover_notice"],
        ))
    story.append(Spacer(1, 0.5 * cm))
    story.append(Paragraph(
        "All AI-generated sections are drafts only and require review and sign-off "
        "by a qualified accountant (CPA or CA) before use for any tax, legal, or business purpose.",
        styles["cover_notice"],
    ))
    story.append(PageBreak())

    # --------------------------------------------------------- Income Statement
    is_ = report_structure.get("income_statement", {})
    if is_ and config.INCLUDE_INCOME_STATEMENT:
        story.append(Paragraph("Income Statement", styles["section_heading"]))
        story.append(HRFlowable(width="100%", thickness=1, color=NAVY, spaceAfter=6))
        story.append(_build_income_statement_table(is_))
        story.append(Spacer(1, 0.5 * cm))

    # ----------------------------------------------------------- Balance Sheet
    bs = report_structure.get("balance_sheet", {})
    if bs and config.INCLUDE_BALANCE_SHEET:
        story.append(Paragraph("Balance Sheet", styles["section_heading"]))
        story.append(HRFlowable(width="100%", thickness=1, color=NAVY, spaceAfter=6))
        story.append(_build_balance_sheet_table(bs))
        story.append(Spacer(1, 0.5 * cm))

    # --------------------------------------------------------------- Ratios
    ratios = report_structure.get("ratios", {})
    if ratios and config.INCLUDE_RATIOS:
        story.append(Paragraph("Key Financial Ratios", styles["section_heading"]))
        story.append(HRFlowable(width="100%", thickness=1, color=NAVY, spaceAfter=6))
        story.append(_build_ratios_table(ratios))
        story.append(Spacer(1, 0.5 * cm))

    # -------------------------------------------------------- Narrative sections
    if narratives:
        story.append(PageBreak())
        story.append(Paragraph("Narrative Commentary", styles["section_heading"]))
        story.append(HRFlowable(width="100%", thickness=2, color=NAVY, spaceAfter=12))

        section_labels = {
            "executive_summary": "Executive Summary",
            "notes_to_financials": "Notes to Financial Statements",
            "directors_declaration": "Directors' Declaration",
        }

        for key, text in narratives.items():
            label = section_labels.get(key, _section_name(key))
            story.append(Paragraph(label, styles["section_heading"]))
            story.append(HRFlowable(width="100%", thickness=0.5, color=LIGHT_GREY, spaceAfter=6))

            # Strip markdown watermark syntax and re-render as styled paragraph
            cleaned = text.replace("> **AI DRAFT - REQUIRES ACCOUNTANT REVIEW**", "").strip()
            story.append(Paragraph(
                "<i>AI DRAFT — REQUIRES ACCOUNTANT REVIEW</i>",
                styles["watermark_draft"],
            ))

            # Split on double newlines to preserve paragraph breaks
            for para in cleaned.split("\n\n"):
                para = para.strip()
                if para:
                    # Replace single newlines with spaces for paragraph flow
                    para = para.replace("\n", " ")
                    # Escape any bare ampersands
                    para = para.replace("&", "&amp;")
                    try:
                        story.append(Paragraph(para, styles["narrative_body"]))
                    except Exception:
                        # Fall back to plain text if XML parsing fails
                        story.append(Paragraph(para.replace("<", "&lt;").replace(">", "&gt;"), styles["narrative_body"]))

            story.append(Spacer(1, 0.8 * cm))

    doc.build(story)
    logger.info("PDF written: %s", output_path)


def write_redacted_input_pdf(
    name: str,
    redacted_text: str,
    token_records: list,
    output_path: str,
    session_id: str,
) -> None:
    """
    Write a PDF showing the redacted version of an input document.

    Tokens like [AU_TFN_001] are rendered in red bold so the reviewer can see
    exactly what was redacted. A legend table at the top lists every entity type
    detected and which tokens were assigned.

    Args:
        name:          Original document filename.
        redacted_text: Full text with PII replaced by [TOKEN] placeholders.
        token_records: List of RedactionRecord objects (have .entity_type, .token, .original).
        output_path:   Absolute path for the output PDF.
        session_id:    Session identifier for the footer.
    """
    logger.info("Writing redacted input PDF: %s", output_path)

    styles = _build_styles()
    page_decorator = _PageDecorator(session_id, redacted=False, total_pages_ref=[0])

    # Override the watermark flag — add a custom "REDACTED INPUT" badge instead
    page_decorator.redacted = False  # no diagonal watermark on these input PDFs

    doc = SimpleDocTemplate(
        output_path,
        pagesize=A4,
        rightMargin=2 * cm,
        leftMargin=2 * cm,
        topMargin=2.5 * cm,
        bottomMargin=2.5 * cm,
    )
    frame = Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height, id="normal")
    template = PageTemplate(id="main", frames=[frame], onPage=page_decorator)
    doc.addPageTemplates([template])

    story = []

    # ---- Header ----
    badge_style = ParagraphStyle(
        "badge",
        parent=styles["cover_sub"],
        textColor=WHITE,
        backColor=NAVY,
        fontSize=10,
        spaceAfter=6,
        borderPad=4,
        fontName="Helvetica-Bold",
    )
    story.append(Paragraph("REDACTED INPUT — AI VIEW", badge_style))
    story.append(Paragraph(f"Source document: {name}", styles["cover_sub"]))
    story.append(Paragraph(f"Session: {session_id}", styles["cover_sub"]))
    story.append(HRFlowable(width="100%", thickness=1, color=NAVY, spaceAfter=6))

    # ---- Legend: what was redacted ----
    by_type = defaultdict(list)
    for rec in token_records:
        by_type[rec.entity_type].append(rec.token)

    if by_type:
        story.append(Paragraph("Redacted Entities", styles["section_heading"]))
        legend_data = [["Entity Type", "Count", "Tokens Assigned"]]
        for etype in sorted(by_type.keys()):
            tokens = by_type[etype]
            token_str = ", ".join(tokens[:6])
            if len(tokens) > 6:
                token_str += f" (+{len(tokens) - 6} more)"
            legend_data.append([etype, str(len(tokens)), token_str])

        legend_table = Table(legend_data, colWidths=[5 * cm, 2 * cm, 9 * cm], repeatRows=1)
        legend_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), NAVY),
            ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, GREY_BG]),
            ("LEFTPADDING", (0, 0), (-1, -1), 5),
            ("RIGHTPADDING", (0, 0), (-1, -1), 5),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("LINEBELOW", (0, 0), (-1, 0), 0.5, NAVY),
        ]))
        story.append(legend_table)
        story.append(Spacer(1, 0.5 * cm))
    else:
        story.append(Paragraph(
            "No PII entities detected in this document.",
            styles["narrative_body"],
        ))
        story.append(Spacer(1, 0.3 * cm))

    story.append(HRFlowable(width="100%", thickness=1, color=NAVY, spaceAfter=8))
    story.append(Paragraph("Document Content (with PII replaced by tokens)", styles["section_heading"]))
    story.append(Spacer(1, 0.3 * cm))

    # ---- Body: redacted text with tokens highlighted ----
    token_style = ParagraphStyle(
        "token_inline",
        parent=styles["narrative_body"],
        fontSize=9,
        fontName="Helvetica",
        leading=14,
    )

    # Split into paragraphs; replace token placeholders with red-bold inline markup
    for block in redacted_text.split('\n\n'):
        block = block.strip()
        if not block:
            continue
        for line in block.split('\n'):
            line = line.strip()
            if not line:
                continue
            # Replace tokens with red bold markup; escape XML special chars first
            safe = line.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            # Re-render token placeholders with colour (the & in &amp; won't interfere
            # because token names are uppercase alphanumeric/underscore only)
            safe = _TOKEN_RE.sub(
                lambda m: f'<font color="#CC0000"><b>[{m.group(1)}]</b></font>',
                safe,
            )
            try:
                story.append(Paragraph(safe, token_style))
            except Exception:
                # Strip all markup if XML parse fails
                plain = _TOKEN_RE.sub(lambda m: f'[{m.group(1)}]', line)
                story.append(Paragraph(plain.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;'), token_style))

    doc.build(story)
    logger.info("Redacted input PDF written: %s", output_path)
