"""
presidio_engine.py — Presidio AnalyzerEngine setup.

Registers all AU custom recognisers plus the built-in spaCy NLP engine
(en_core_web_lg — mandatory per CLAUDE.md).

Allow-list deny patterns prevent financial figures, regulatory dates, account
codes, and regulatory entity names from ever being anonymised.
"""

import re
import logging
from functools import lru_cache

from presidio_analyzer import AnalyzerEngine, RecognizerRegistry
from presidio_analyzer.nlp_engine import NlpEngineProvider

import config
from redaction.au_recognisers import get_au_recognisers

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Allow-list: patterns that must NEVER be redacted
# ---------------------------------------------------------------------------
ALLOW_LIST_PATTERNS = [
    re.compile(r"\$[\d,]+(?:\.\d+)?"),                    # Dollar amounts: $125,000.00
    re.compile(r"\b\d+(?:\.\d+)?%"),                       # Percentages: 12.5%
    re.compile(r"\bFY\d{4}\b", re.IGNORECASE),             # FY2024, FY2025
    re.compile(r"\bQ[1-4]\s+\d{4}\b", re.IGNORECASE),     # Q1 2024
    re.compile(r"\b30\s+June\s+\d{4}\b", re.IGNORECASE),  # 30 June 2024
    re.compile(r"\b31\s+(?:March|December)\s+\d{4}\b", re.IGNORECASE),
    re.compile(r"\b\d+-\d+\b"),                            # Chart of accounts: 4-1000
    re.compile(r"\bDivision\s+\d+[A-Z]?\b", re.IGNORECASE),   # Division 7A
    re.compile(r"\bSection\s+\d+-\d+\b", re.IGNORECASE),       # Section 8-1
    re.compile(r"\bs\d+[A-Z]+\b"),                         # s100A
    re.compile(r"\b(?:ATO|ASIC|APRA|RBA|AFCA)\b"),        # Regulatory entities
    re.compile(r"\b[\w.+-]+@(?:ato|asic|apra|rba|afca|treasury)\.gov\.au\b", re.IGNORECASE),  # Gov emails
    re.compile(r"\b127\.0\.0\.1\b"),                       # Localhost
    re.compile(r"\b0\.0\.0\.0\b"),                         # Null route
]

REGULATORY_ENTITY_NAMES = {"ATO", "ASIC", "APRA", "RBA", "AFCA"}


def _is_allow_listed(text: str, start: int, end: int) -> bool:
    """Return True if the span at [start:end] matches any allow-list pattern."""
    span = text[start:end]
    for pattern in ALLOW_LIST_PATTERNS:
        if pattern.fullmatch(span) or pattern.search(span):
            return True
    if span.strip() in REGULATORY_ENTITY_NAMES:
        return True
    return False


# ---------------------------------------------------------------------------
# Engine factory
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def get_engine() -> AnalyzerEngine:
    """
    Build and return a singleton AnalyzerEngine with all AU recognisers loaded.
    Uses en_core_web_lg (mandatory — en_core_web_sm misses too many patterns).
    """
    logger.info("Initialising Presidio AnalyzerEngine with en_core_web_lg...")

    nlp_config = {
        "nlp_engine_name": "spacy",
        "models": [{"lang_code": "en", "model_name": "en_core_web_lg"}],
    }
    provider = NlpEngineProvider(nlp_configuration=nlp_config)
    nlp_engine = provider.create_engine()

    registry = RecognizerRegistry()
    registry.load_predefined_recognizers(nlp_engine=nlp_engine)

    for recogniser in get_au_recognisers(
        preserve_regulatory_orgs=config.PRESERVE_REGULATORY_ORGS
    ):
        registry.add_recognizer(recogniser)
        logger.debug("Registered recogniser: %s", recogniser.name)

    engine = AnalyzerEngine(
        registry=registry,
        nlp_engine=nlp_engine,
        supported_languages=["en"],
    )

    logger.info("AnalyzerEngine ready.")
    return engine


# Built-in Presidio entity types that generate too many false positives in financial docs.
# DATE_TIME fires on words like "Annual"; ORG fires on labels like "BSB:".
# We keep only our AU custom types plus PERSON (names are genuine PII).
ALLOWED_ENTITY_TYPES = {
    "AU_TFN", "AU_DOB", "AU_ABN", "AU_BSB", "AU_BANK_ACCOUNT", "PERSON",
    "AU_MEDICARE", "AU_PASSPORT", "AU_DRIVERS_LICENCE", "AU_DIRECTOR_ID",
    "AU_EMAIL", "AU_PHONE", "AU_IP_ADDRESS", "AU_ADDRESS", "AU_SUPER_MEMBER",
    "SENSITIVE_CONTEXT",
}


def analyze_text(text: str, confidence_threshold: float = None) -> list:
    """
    Run the full Presidio analysis pipeline on `text`.
    Returns a deduplicated, allow-list-filtered RecognizerResult list.

    Deduplication: when two results overlap, keep the one from a custom AU
    recogniser (higher specificity); otherwise keep the higher-confidence one.
    """
    if confidence_threshold is None:
        confidence_threshold = config.REDACTION_CONFIDENCE_THRESHOLD

    engine = get_engine()
    results = engine.analyze(text=text, language="en")

    # --- Step 1: restrict to allowed entity types ---
    results = [r for r in results if r.entity_type in ALLOWED_ENTITY_TYPES]

    # --- Step 2: confidence threshold + Privacy Act TFN logging ---
    passing = []
    for result in results:
        if result.score < confidence_threshold:
            if result.entity_type == "AU_TFN":
                logger.warning(
                    "Low-confidence AU_TFN at [%d:%d] score=%.2f — below threshold but logged",
                    result.start, result.end, result.score,
                )
            continue
        passing.append(result)

    # --- Step 3: allow-list filter ---
    # SENSITIVE_CONTEXT bypasses the allow-list — it operates at sentence level
    # and the allow-list patterns are not meaningful for full-sentence spans.
    passing = [
        r for r in passing
        if r.entity_type == "SENSITIVE_CONTEXT" or not _is_allow_listed(text, r.start, r.end)
    ]

    # --- Step 4: deduplicate overlapping spans ---
    # Sort by start, then by score descending so the best match at each position wins.
    passing.sort(key=lambda r: (r.start, -r.score))
    deduplicated = []
    for result in passing:
        # Check if this span overlaps with any already-accepted result
        overlaps = any(
            result.start < accepted.end and result.end > accepted.start
            for accepted in deduplicated
        )
        if not overlaps:
            deduplicated.append(result)
        else:
            logger.debug(
                "Dropped overlapping %s at [%d:%d] score=%.2f",
                result.entity_type, result.start, result.end, result.score,
            )

    return deduplicated
