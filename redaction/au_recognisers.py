"""
au_recognisers.py — Custom Presidio recognisers for Australian PII.

Fifteen recognisers covering the full Australian Privacy Act 1988 scope:

Government Identifiers:
  AU_TFN             — Tax File Number (checksum)
  AU_MEDICARE        — Medicare card number (checksum)
  AU_PASSPORT        — Australian passport number
  AU_DRIVERS_LICENCE — State driver's licence (context-required)
  AU_DIRECTOR_ID     — ABRS Director Identification Number (context-required)

Financial Identifiers:
  AU_ABN             — Australian Business Number (checksum)
  AU_BSB             — Bank State Branch code
  AU_BANK_ACCOUNT    — Bank account number (context-required)
  AU_SUPER_MEMBER    — Superannuation member number (context-required)

Personal Information:
  AU_DOB             — Date of Birth (context-gated)
  AU_EMAIL           — Email address
  AU_PHONE           — Australian phone numbers (mobile, landline, international)
  AU_IP_ADDRESS      — IPv4 and IPv6 addresses
  AU_ADDRESS         — Australian street addresses (context-gated)

Sensitive Categories (sentence-level):
  SENSITIVE_CONTEXT  — Health, mental health, genetic, biometric, racial/ethnic, political,
                       religious, sexual orientation, criminal, trade union, gender,
                       marital status, residency, salary-linked, employment, next of kin.
                       Flags entire sentences when a sensitive keyword co-occurs with a
                       named person within 200 characters.
"""

import re
import logging
from typing import List

from presidio_analyzer import (
    PatternRecognizer,
    Pattern,
    RecognizerResult,
    EntityRecognizer,
    AnalysisExplanation,
)
from presidio_analyzer.nlp_engine import NlpArtifacts

logger = logging.getLogger(__name__)


# ===========================================================================
# Checksum validators
# ===========================================================================

def _tfn_checksum(digits: str) -> bool:
    """ATO TFN checksum: weights [1,4,3,7,5,8,6,9,10]; sum mod 11 == 0."""
    weights = [1, 4, 3, 7, 5, 8, 6, 9, 10]
    if len(digits) != 9:
        return False
    try:
        return sum(int(d) * w for d, w in zip(digits, weights)) % 11 == 0
    except ValueError:
        return False


def _abn_checksum(digits: str) -> bool:
    """ATO ABN checksum: subtract 1 from digit[0]; weights [10,1,3,5,7,9,11,13,15,17,19]; sum mod 89 == 0."""
    weights = [10, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19]
    if len(digits) != 11:
        return False
    try:
        d = [int(c) for c in digits]
        d[0] -= 1
        return sum(v * w for v, w in zip(d, weights)) % 89 == 0
    except ValueError:
        return False


def _medicare_checksum(digits: str) -> bool:
    """
    Medicare check digit validation.
    Weights [1,3,7,9,1,3,7,9] applied to digits[0:8]; sum mod 10 == digits[8].
    digits must be at least 9 characters (first 9 of the 10-digit number;
    digit 10 is the IRN and is not part of the checksum).
    """
    weights = [1, 3, 7, 9, 1, 3, 7, 9]
    if len(digits) < 9:
        return False
    try:
        total = sum(int(digits[i]) * weights[i] for i in range(8))
        return total % 10 == int(digits[8])
    except (ValueError, IndexError):
        return False


# ===========================================================================
# Government ABNs to preserve
# ===========================================================================
GOVERNMENT_ABNS = {
    "51824753556",  # ATO
    "26586565341",  # ASIC
    "81913830179",  # APRA
}


# ===========================================================================
# Shared helper
# ===========================================================================

def _context_near(text: str, start: int, end: int, context_words: list, window: int = 60) -> bool:
    snippet = text[max(0, start - window): end + window].lower()
    return any(cw.lower() in snippet for cw in context_words)


# ===========================================================================
# AU_TFN
# ===========================================================================

class AUTFNRecogniser(EntityRecognizer):
    """
    Australian Tax File Number.
    8 or 9 digits, optionally space-separated.
    ALL detections logged regardless of confidence (Privacy Act requirement).
    """
    ENTITY_TYPE = "AU_TFN"
    PATTERN = re.compile(r"\b(\d{3})\s?(\d{3})\s?(\d{2,3})\b")
    CONTEXT_WORDS = ["TFN", "tax file number", "tax file", "ATO", "withholding"]

    def __init__(self):
        super().__init__(supported_entities=[self.ENTITY_TYPE], name="AUTFNRecogniser", supported_language="en")

    def load(self): pass

    def analyze(self, text: str, entities: List[str], nlp_artifacts: NlpArtifacts) -> List[RecognizerResult]:
        results = []
        for match in self.PATTERN.finditer(text):
            raw = match.group(0)
            digits = re.sub(r"\s", "", raw)
            if len(digits) not in (8, 9):
                continue
            check_digits = digits if len(digits) == 9 else "0" + digits
            passes = _tfn_checksum(check_digits)
            score = 0.90 if passes else 0.65
            if not passes and _context_near(text, match.start(), match.end(), self.CONTEXT_WORDS, 50):
                score = 0.80
            logger.info("AU_TFN at [%d:%d] raw=%r checksum=%s score=%.2f", match.start(), match.end(), raw, passes, score)
            results.append(RecognizerResult(
                entity_type=self.ENTITY_TYPE, start=match.start(), end=match.end(), score=score,
                analysis_explanation=AnalysisExplanation(recognizer=self.name, original_score=score,
                    textual_explanation=f"TFN checksum {'passed' if passes else 'failed'}"),
            ))
        return results


# ===========================================================================
# AU_MEDICARE
# ===========================================================================

class AUMedicareRecogniser(EntityRecognizer):
    """
    Australian Medicare card number: 10 digits (4-5-1 grouping), optional spaces.
    Validated with the Medicare check digit algorithm on digits 1–9.
    """
    ENTITY_TYPE = "AU_MEDICARE"
    # 4 digits, optional space, 5 digits, optional space, 1 digit
    PATTERN = re.compile(r"\b(\d{4})\s?(\d{5})\s?(\d)\b")
    CONTEXT_WORDS = ["medicare", "medicare number", "medicare card", "health insurance", "DVA"]

    def __init__(self):
        super().__init__(supported_entities=[self.ENTITY_TYPE], name="AUMedicareRecogniser", supported_language="en")

    def load(self): pass

    def analyze(self, text: str, entities: List[str], nlp_artifacts: NlpArtifacts) -> List[RecognizerResult]:
        results = []
        for match in self.PATTERN.finditer(text):
            digits = re.sub(r"\s", "", match.group(0))
            if len(digits) != 10:
                continue
            passes = _medicare_checksum(digits)
            if not passes:
                continue
            ctx = _context_near(text, match.start(), match.end(), self.CONTEXT_WORDS, 80)
            score = 0.85 if ctx else 0.70
            results.append(RecognizerResult(
                entity_type=self.ENTITY_TYPE, start=match.start(), end=match.end(), score=score,
                analysis_explanation=AnalysisExplanation(recognizer=self.name, original_score=score,
                    textual_explanation=f"Medicare checksum passed; context {'found' if ctx else 'not found'}"),
            ))
        return results


# ===========================================================================
# AU_PASSPORT
# ===========================================================================

class AUPassportRecogniser(EntityRecognizer):
    """
    Australian passport: 1–2 uppercase letters followed by 7 digits (e.g. N1234567, PA123456).
    Context word is MANDATORY to avoid false positives on account codes.
    """
    ENTITY_TYPE = "AU_PASSPORT"
    PATTERN = re.compile(r"\b[A-Z]{1,2}\d{7}\b")
    CONTEXT_WORDS = ["passport", "passport number", "passport no", "travel document", "passport:"]

    def __init__(self):
        super().__init__(supported_entities=[self.ENTITY_TYPE], name="AUPassportRecogniser", supported_language="en")

    def load(self): pass

    def analyze(self, text: str, entities: List[str], nlp_artifacts: NlpArtifacts) -> List[RecognizerResult]:
        results = []
        for match in self.PATTERN.finditer(text):
            if not _context_near(text, match.start(), match.end(), self.CONTEXT_WORDS, 80):
                continue
            results.append(RecognizerResult(
                entity_type=self.ENTITY_TYPE, start=match.start(), end=match.end(), score=0.95,
                analysis_explanation=AnalysisExplanation(recognizer=self.name, original_score=0.95,
                    textual_explanation="Passport pattern with mandatory context word"),
            ))
        return results


# ===========================================================================
# AU_DRIVERS_LICENCE
# ===========================================================================

class AUDriversLicenceRecogniser(EntityRecognizer):
    """
    Australian driver's licence numbers are state-specific but share the form:
    0–2 uppercase letters followed by 6–9 digits.
    Context word is MANDATORY — the pattern is too broad alone in financial docs.
    """
    ENTITY_TYPE = "AU_DRIVERS_LICENCE"
    PATTERN = re.compile(r"\b[A-Z]{0,2}\d{6,9}\b")
    CONTEXT_WORDS = [
        "driver licence", "drivers licence", "driver's licence", "driving licence",
        "licence number", "license number", "DL number", "licence no", "driver license",
    ]

    def __init__(self):
        super().__init__(supported_entities=[self.ENTITY_TYPE], name="AUDriversLicenceRecogniser", supported_language="en")

    def load(self): pass

    def analyze(self, text: str, entities: List[str], nlp_artifacts: NlpArtifacts) -> List[RecognizerResult]:
        results = []
        for match in self.PATTERN.finditer(text):
            if not _context_near(text, match.start(), match.end(), self.CONTEXT_WORDS, 80):
                continue
            results.append(RecognizerResult(
                entity_type=self.ENTITY_TYPE, start=match.start(), end=match.end(), score=0.85,
                analysis_explanation=AnalysisExplanation(recognizer=self.name, original_score=0.85,
                    textual_explanation="Driver's licence pattern with mandatory context word"),
            ))
        return results


# ===========================================================================
# AU_DIRECTOR_ID
# ===========================================================================

class AUDirectorIDRecogniser(EntityRecognizer):
    """
    ABRS Director Identification Number: exactly 15 digits.
    Context word is MANDATORY.
    """
    ENTITY_TYPE = "AU_DIRECTOR_ID"
    PATTERN = re.compile(r"\b\d{15}\b")
    CONTEXT_WORDS = [
        "director id", "director ID", "director identification", "director identification number",
        "ABRS", "DIN", "director id number",
    ]

    def __init__(self):
        super().__init__(supported_entities=[self.ENTITY_TYPE], name="AUDirectorIDRecogniser", supported_language="en")

    def load(self): pass

    def analyze(self, text: str, entities: List[str], nlp_artifacts: NlpArtifacts) -> List[RecognizerResult]:
        results = []
        for match in self.PATTERN.finditer(text):
            if not _context_near(text, match.start(), match.end(), self.CONTEXT_WORDS, 80):
                continue
            results.append(RecognizerResult(
                entity_type=self.ENTITY_TYPE, start=match.start(), end=match.end(), score=0.90,
                analysis_explanation=AnalysisExplanation(recognizer=self.name, original_score=0.90,
                    textual_explanation="Director ID 15-digit pattern with mandatory context word"),
            ))
        return results


# ===========================================================================
# AU_ABN
# ===========================================================================

class AUABNRecogniser(EntityRecognizer):
    """
    Australian Business Number: 11 digits (2-3-3-3 grouping), checksum validated.
    Optionally preserves government entity ABNs.
    """
    ENTITY_TYPE = "AU_ABN"
    PATTERN = re.compile(r"\b(\d{2})\s?(\d{3})\s?(\d{3})\s?(\d{3})\b")
    CONTEXT_WORDS = ["ABN", "australian business number", "business number", "abn:"]

    def __init__(self, preserve_regulatory_orgs: bool = True):
        super().__init__(supported_entities=[self.ENTITY_TYPE], name="AUABNRecogniser", supported_language="en")
        self.preserve_regulatory_orgs = preserve_regulatory_orgs

    def load(self): pass

    def analyze(self, text: str, entities: List[str], nlp_artifacts: NlpArtifacts) -> List[RecognizerResult]:
        results = []
        for match in self.PATTERN.finditer(text):
            digits = re.sub(r"\s", "", match.group(0))
            if len(digits) != 11:
                continue
            if self.preserve_regulatory_orgs and digits in GOVERNMENT_ABNS:
                continue
            if not _abn_checksum(digits):
                continue
            ctx = _context_near(text, match.start(), match.end(), self.CONTEXT_WORDS, 60)
            score = 0.90 if ctx else 0.75
            results.append(RecognizerResult(
                entity_type=self.ENTITY_TYPE, start=match.start(), end=match.end(), score=score,
                analysis_explanation=AnalysisExplanation(recognizer=self.name, original_score=score,
                    textual_explanation=f"ABN checksum passed; context {'found' if ctx else 'not found'}"),
            ))
        return results


# ===========================================================================
# AU_BSB
# ===========================================================================

class AUBSBRecogniser(PatternRecognizer):
    """Bank State Branch: NNN-NNN (always hyphenated)."""
    PATTERNS = [Pattern("AU_BSB", r"\b\d{3}-\d{3}\b", 0.85)]
    CONTEXT = ["BSB", "bank state branch", "branch code", "bsb:"]

    def __init__(self):
        super().__init__(supported_entity="AU_BSB", name="AUBSBRecogniser",
                         patterns=self.PATTERNS, context=self.CONTEXT, supported_language="en")


# ===========================================================================
# AU_BANK_ACCOUNT
# ===========================================================================

class AUBankAccountRecogniser(EntityRecognizer):
    """Bank account: 6–10 digits, optional internal spaces. Context MANDATORY."""
    ENTITY_TYPE = "AU_BANK_ACCOUNT"
    PATTERN = re.compile(r"\b\d[\d ]{4,8}\d\b")
    CONTEXT_WORDS = ["account number", "account no", "a/c", "acc no", "direct debit", "account:"]

    def __init__(self):
        super().__init__(supported_entities=[self.ENTITY_TYPE], name="AUBankAccountRecogniser", supported_language="en")

    def load(self): pass

    def analyze(self, text: str, entities: List[str], nlp_artifacts: NlpArtifacts) -> List[RecognizerResult]:
        results = []
        for match in self.PATTERN.finditer(text):
            digits = re.sub(r"\s", "", match.group(0))
            if not (6 <= len(digits) <= 10):
                continue
            if not _context_near(text, match.start(), match.end(), self.CONTEXT_WORDS, 60):
                continue
            results.append(RecognizerResult(
                entity_type=self.ENTITY_TYPE, start=match.start(), end=match.end(), score=0.80,
                analysis_explanation=AnalysisExplanation(recognizer=self.name, original_score=0.80,
                    textual_explanation="Bank account with mandatory context word"),
            ))
        return results


# ===========================================================================
# AU_SUPER_MEMBER
# ===========================================================================

class AUSuperannuationRecogniser(EntityRecognizer):
    """
    Superannuation member number: 6–10 digits, indistinguishable from bank accounts
    without context. Context MANDATORY.
    """
    ENTITY_TYPE = "AU_SUPER_MEMBER"
    PATTERN = re.compile(r"\b\d{6,10}\b")
    CONTEXT_WORDS = [
        "super member", "member number", "membership number", "superannuation fund",
        "super fund", "SMSF", "member no", "fund member", "super account",
    ]

    def __init__(self):
        super().__init__(supported_entities=[self.ENTITY_TYPE], name="AUSuperannuationRecogniser", supported_language="en")

    def load(self): pass

    def analyze(self, text: str, entities: List[str], nlp_artifacts: NlpArtifacts) -> List[RecognizerResult]:
        results = []
        for match in self.PATTERN.finditer(text):
            if not _context_near(text, match.start(), match.end(), self.CONTEXT_WORDS, 60):
                continue
            results.append(RecognizerResult(
                entity_type=self.ENTITY_TYPE, start=match.start(), end=match.end(), score=0.80,
                analysis_explanation=AnalysisExplanation(recognizer=self.name, original_score=0.80,
                    textual_explanation="Super member number with mandatory context word"),
            ))
        return results


# ===========================================================================
# AU_DOB
# ===========================================================================

class AUDOBRecogniser(EntityRecognizer):
    """
    Date of Birth — only fires when a DOB context word is within 50 chars.
    Prevents false positives on reporting period dates (30 June 2024, FY2024).
    """
    ENTITY_TYPE = "AU_DOB"
    CONTEXT_WORDS = ["date of birth", "DOB", "born", "d.o.b", "birth date", "birthdate"]
    DATE_PATTERNS = [
        re.compile(r"\b\d{1,2}/\d{1,2}/\d{4}\b"),
        re.compile(r"\b\d{1,2}-\d{1,2}-\d{4}\b"),
        re.compile(r"\b\d{1,2}\s+(?:January|February|March|April|May|June|July|"
                   r"August|September|October|November|December)\s+\d{4}\b", re.IGNORECASE),
        re.compile(r"\b\d{4}-\d{2}-\d{2}\b"),
    ]

    def __init__(self):
        super().__init__(supported_entities=[self.ENTITY_TYPE], name="AUDOBRecogniser", supported_language="en")

    def load(self): pass

    def analyze(self, text: str, entities: List[str], nlp_artifacts: NlpArtifacts) -> List[RecognizerResult]:
        results = []
        text_lower = text.lower()
        for pattern in self.DATE_PATTERNS:
            for match in pattern.finditer(text):
                snippet = text_lower[max(0, match.start() - 50): match.end() + 50]
                if any(cw.lower() in snippet for cw in self.CONTEXT_WORDS):
                    results.append(RecognizerResult(
                        entity_type=self.ENTITY_TYPE, start=match.start(), end=match.end(), score=0.85,
                        analysis_explanation=AnalysisExplanation(recognizer=self.name, original_score=0.85,
                            textual_explanation="Date pattern with DOB context word within 50 chars"),
                    ))
        return results


# ===========================================================================
# AU_EMAIL
# ===========================================================================

class AUEmailRecogniser(EntityRecognizer):
    """
    Email address. High specificity — no context required.
    Government domain emails (ato.gov.au etc.) are handled by the allow-list in presidio_engine.py.
    """
    ENTITY_TYPE = "AU_EMAIL"
    PATTERN = re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b")

    def __init__(self):
        super().__init__(supported_entities=[self.ENTITY_TYPE], name="AUEmailRecogniser", supported_language="en")

    def load(self): pass

    def analyze(self, text: str, entities: List[str], nlp_artifacts: NlpArtifacts) -> List[RecognizerResult]:
        results = []
        for match in self.PATTERN.finditer(text):
            results.append(RecognizerResult(
                entity_type=self.ENTITY_TYPE, start=match.start(), end=match.end(), score=0.95,
                analysis_explanation=AnalysisExplanation(recognizer=self.name, original_score=0.95,
                    textual_explanation="Email address pattern"),
            ))
        return results


# ===========================================================================
# AU_PHONE
# ===========================================================================

class AUPhoneRecogniser(EntityRecognizer):
    """
    Australian phone numbers: mobile (04xx), landline (0[2378]), and international (+61).
    Format is distinctive enough to not require a context word.
    """
    ENTITY_TYPE = "AU_PHONE"
    PATTERNS = [
        # Mobile: 04xx xxx xxx (with optional spaces or hyphens)
        (re.compile(r"\b04\d{2}[\s\-]?\d{3}[\s\-]?\d{3}\b"), 0.85),
        # Landline: 0[2378] xxxx xxxx
        (re.compile(r"\b0[2378][\s\-]?\d{4}[\s\-]?\d{4}\b"), 0.75),
        # International mobile: +61 4xx xxx xxx
        (re.compile(r"\+61[\s\-]?4\d{2}[\s\-]?\d{3}[\s\-]?\d{3}\b"), 0.90),
        # International landline: +61 [2378] xxxx xxxx
        (re.compile(r"\+61[\s\-]?[2378][\s\-]?\d{4}[\s\-]?\d{4}\b"), 0.85),
    ]

    def __init__(self):
        super().__init__(supported_entities=[self.ENTITY_TYPE], name="AUPhoneRecogniser", supported_language="en")

    def load(self): pass

    def analyze(self, text: str, entities: List[str], nlp_artifacts: NlpArtifacts) -> List[RecognizerResult]:
        results = []
        for pattern, score in self.PATTERNS:
            for match in pattern.finditer(text):
                results.append(RecognizerResult(
                    entity_type=self.ENTITY_TYPE, start=match.start(), end=match.end(), score=score,
                    analysis_explanation=AnalysisExplanation(recognizer=self.name, original_score=score,
                        textual_explanation="Australian phone number pattern"),
                ))
        return results


# ===========================================================================
# AU_IP_ADDRESS
# ===========================================================================

class AUIPAddressRecogniser(EntityRecognizer):
    """
    IPv4 and IPv6 addresses.
    Loopback (127.0.0.1) and unroutable (0.0.0.0) are excluded via the allow-list
    in presidio_engine.py.
    """
    ENTITY_TYPE = "AU_IP_ADDRESS"
    IPV4_PATTERN = re.compile(
        r"\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\b"
    )
    # Simplified IPv6: groups of hex digits separated by colons (full or compressed)
    IPV6_PATTERN = re.compile(r"\b(?:[0-9a-fA-F]{1,4}:){2,7}[0-9a-fA-F]{1,4}\b")

    def __init__(self):
        super().__init__(supported_entities=[self.ENTITY_TYPE], name="AUIPAddressRecogniser", supported_language="en")

    def load(self): pass

    def analyze(self, text: str, entities: List[str], nlp_artifacts: NlpArtifacts) -> List[RecognizerResult]:
        results = []
        for match in self.IPV4_PATTERN.finditer(text):
            results.append(RecognizerResult(
                entity_type=self.ENTITY_TYPE, start=match.start(), end=match.end(), score=0.90,
                analysis_explanation=AnalysisExplanation(recognizer=self.name, original_score=0.90,
                    textual_explanation="IPv4 address"),
            ))
        for match in self.IPV6_PATTERN.finditer(text):
            results.append(RecognizerResult(
                entity_type=self.ENTITY_TYPE, start=match.start(), end=match.end(), score=0.85,
                analysis_explanation=AnalysisExplanation(recognizer=self.name, original_score=0.85,
                    textual_explanation="IPv6 address"),
            ))
        return results


# ===========================================================================
# AU_ADDRESS
# ===========================================================================

class AUAddressRecogniser(EntityRecognizer):
    """
    Australian street addresses: number + street name + street type.
    Context word is needed to reach the 0.75 confidence threshold.
    Without context the score is 0.65 (below threshold) so it won't redact
    regulatory letterhead addresses that appear without context words.
    """
    ENTITY_TYPE = "AU_ADDRESS"
    PATTERN = re.compile(
        r"\b\d{1,4}\s+[A-Z][a-zA-Z\s]{2,30}\s+"
        r"(?:Street|St|Road|Rd|Avenue|Ave|Drive|Dr|Court|Ct|Place|Pl|Lane|Ln|"
        r"Crescent|Cres|Boulevard|Blvd|Way|Close|Cl|Parade|Pde|Terrace|Tce|"
        r"Highway|Hwy|Grove|Gr|Circuit|Cct)\b"
    )
    CONTEXT_WORDS = [
        "address", "residential", "home address", "postal address",
        "street address", "mailing address", "lives at", "residing at", "resident at",
    ]

    def __init__(self):
        super().__init__(supported_entities=[self.ENTITY_TYPE], name="AUAddressRecogniser", supported_language="en")

    def load(self): pass

    def analyze(self, text: str, entities: List[str], nlp_artifacts: NlpArtifacts) -> List[RecognizerResult]:
        results = []
        for match in self.PATTERN.finditer(text):
            ctx = _context_near(text, match.start(), match.end(), self.CONTEXT_WORDS, 80)
            score = 0.80 if ctx else 0.65
            # Only report — threshold in presidio_engine will drop 0.65 ones
            results.append(RecognizerResult(
                entity_type=self.ENTITY_TYPE, start=match.start(), end=match.end(), score=score,
                analysis_explanation=AnalysisExplanation(recognizer=self.name, original_score=score,
                    textual_explanation=f"Street address pattern; context {'found' if ctx else 'not found'}"),
            ))
        return results


# ===========================================================================
# SENSITIVE_CONTEXT
# ===========================================================================

# Keywords that signal each sensitive category
SENSITIVE_CATEGORY_KEYWORDS = {
    "health": [
        "diagnosis", "medical condition", "health condition", "illness", "disease",
        "treatment", "medication", "hospital", "surgery", "disability", "chronic",
        "clinical", "GP", "doctor", "patient", "symptom", "prescription", "injury",
    ],
    "mental_health": [
        "mental health", "depression", "anxiety", "psychiatric", "psychological",
        "therapy", "counselling", "counseling", "PTSD", "bipolar", "schizophrenia",
        "mental illness", "psychologist", "psychiatrist",
    ],
    "genetic": ["genetic", "DNA", "chromosome", "hereditary", "genome", "genomic"],
    "biometric": [
        "fingerprint", "retina", "iris scan", "face recognition", "biometric",
        "voiceprint", "facial scan",
    ],
    "racial_ethnic": [
        "race", "ethnicity", "ethnic origin", "Aboriginal", "Torres Strait Islander",
        "ATSI", "cultural background", "First Nations",
    ],
    "political": [
        "political party", "political opinion", "political belief",
        "political membership", "political affiliation",
    ],
    "religious": [
        "religion", "faith", "church", "mosque", "synagogue", "temple",
        "religious belief", "Christian", "Muslim", "Jewish", "Buddhist", "Hindu",
        "denomination", "worship",
    ],
    "sexual_orientation": [
        "sexual orientation", "sexuality", "gay", "lesbian", "bisexual",
        "transgender", "LGBTQ", "same-sex", "queer",
    ],
    "criminal": [
        "criminal record", "conviction", "offence", "offense", "charged with",
        "arrested", "imprisonment", "parole", "criminal history", "guilty",
    ],
    "union": [
        "trade union", "union member", "union membership", "union rep",
        "industrial action", "strike", "union delegate",
    ],
    "gender": ["gender identity", "non-binary", "gender dysphoria"],
    "marital_status": [
        "married", "divorced", "widowed", "de facto", "separated", "marital status",
        "single parent",
    ],
    "residency": [
        "permanent resident", "visa holder", "citizenship", "residency status",
        "work visa", "temporary resident", "refugee",
    ],
    "salary_linked": [
        "salary", "earns", "paid", "remuneration", "wage", "income",
        "compensation package", "annual package",
    ],
    "employment": [
        "employed at", "job title", "position at", "works at", "works for",
        "employer", "job description",
    ],
    "next_of_kin": [
        "next of kin", "emergency contact", "dependant", "next-of-kin",
    ],
}

# Flat list of all sensitive keywords for quick pre-screening
_ALL_SENSITIVE_KEYWORDS = [kw for kws in SENSITIVE_CATEGORY_KEYWORDS.values() for kw in kws]

# Sentence boundary splitter
_SENTENCE_RE = re.compile(r"(?<=[.!?\n])\s+")


class SensitiveContextRecogniser(EntityRecognizer):
    """
    Sentence-level recogniser for the Privacy Act's sensitive information categories
    and context-dependent personal information.

    Logic:
      1. Split text into sentences.
      2. For each sentence containing a sensitive keyword:
         a. If a PERSON entity from spaCy's NLP artifacts appears within 200 chars
            of the sentence → flag the entire sentence at score 0.80 (above threshold).
         b. If no PERSON entity found → score 0.65 (below threshold; logged only).

    This correctly handles:
      "John Smith has been diagnosed with diabetes" → REDACTED (PERSON + health keyword)
      "The company health insurance policy covers all staff" → NOT redacted (no person)
    """

    ENTITY_TYPE = "SENSITIVE_CONTEXT"

    def __init__(self):
        super().__init__(
            supported_entities=[self.ENTITY_TYPE],
            name="SensitiveContextRecogniser",
            supported_language="en",
        )

    def load(self): pass

    def analyze(self, text: str, entities: List[str], nlp_artifacts: NlpArtifacts) -> List[RecognizerResult]:
        results = []
        text_lower = text.lower()

        # Collect PERSON entity spans from spaCy's NLP artifacts
        person_spans = []
        if nlp_artifacts and nlp_artifacts.entities:
            for ent in nlp_artifacts.entities:
                if ent.label_ == "PERSON":
                    person_spans.append((ent.start_char, ent.end_char))

        # Split into sentence spans
        sentence_spans = self._sentence_spans(text)

        for sent_start, sent_end in sentence_spans:
            sentence = text_lower[sent_start:sent_end]
            # Quick pre-screen: does this sentence contain ANY sensitive keyword?
            if not any(kw.lower() in sentence for kw in _ALL_SENSITIVE_KEYWORDS):
                continue

            # Identify which category was triggered (for explanation)
            triggered_categories = [
                cat for cat, kws in SENSITIVE_CATEGORY_KEYWORDS.items()
                if any(kw.lower() in sentence for kw in kws)
            ]

            # Check for PERSON entity within 200 chars of the sentence boundaries
            window_start = max(0, sent_start - 200)
            window_end = min(len(text), sent_end + 200)
            person_nearby = any(
                ps < window_end and pe > window_start
                for ps, pe in person_spans
            )

            score = 0.80 if person_nearby else 0.65

            logger.info(
                "SENSITIVE_CONTEXT at [%d:%d] categories=%s person_nearby=%s score=%.2f",
                sent_start, sent_end, triggered_categories, person_nearby, score,
            )

            results.append(RecognizerResult(
                entity_type=self.ENTITY_TYPE,
                start=sent_start,
                end=sent_end,
                score=score,
                analysis_explanation=AnalysisExplanation(
                    recognizer=self.name,
                    original_score=score,
                    textual_explanation=(
                        f"Sensitive categories: {triggered_categories}; "
                        f"person entity {'found' if person_nearby else 'not found'} within 200 chars"
                    ),
                ),
            ))

        return results

    def _sentence_spans(self, text: str) -> list:
        """Return list of (start, end) tuples for each sentence in text."""
        spans = []
        current_start = 0
        for match in re.finditer(r"[.!?\n]+\s*", text):
            end = match.end()
            if end > current_start:
                spans.append((current_start, match.start() + 1))
            current_start = end
        if current_start < len(text):
            spans.append((current_start, len(text)))
        return spans


# ===========================================================================
# Public factory
# ===========================================================================

def get_au_recognisers(preserve_regulatory_orgs: bool = True) -> list:
    """Return all AU custom recogniser instances, ready to register with Presidio."""
    return [
        # Government identifiers
        AUTFNRecogniser(),
        AUMedicareRecogniser(),
        AUPassportRecogniser(),
        AUDriversLicenceRecogniser(),
        AUDirectorIDRecogniser(),
        # Financial identifiers
        AUABNRecogniser(preserve_regulatory_orgs=preserve_regulatory_orgs),
        AUBSBRecogniser(),
        AUBankAccountRecogniser(),
        AUSuperannuationRecogniser(),
        # Personal information
        AUDOBRecogniser(),
        AUEmailRecogniser(),
        AUPhoneRecogniser(),
        AUIPAddressRecogniser(),
        AUAddressRecogniser(),
        # Sensitive categories (sentence-level)
        SensitiveContextRecogniser(),
    ]
