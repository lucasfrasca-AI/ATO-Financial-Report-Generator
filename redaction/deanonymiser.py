"""
deanonymiser.py — Restores tokens in the final report output.

Tokens are formatted as [ENTITY_TYPE_NNN] (e.g. [AU_TFN_001]).
The deanonymiser replaces each token with its original value from the token map.
"""

import logging
import re
from pathlib import Path

from redaction.token_map import TokenMap

logger = logging.getLogger(__name__)

# Matches any token of the form [UPPERCASE_WORD_NNN]
_TOKEN_RE = re.compile(r"\[([A-Z][A-Z0-9_]+_\d{3})\]")


def deanonymise_text(text: str, token_map: TokenMap) -> str:
    """
    Replace all tokens in `text` with their original values from `token_map`.

    Args:
        text:      Redacted text containing [TOKEN_NNN] placeholders.
        token_map: TokenMap instance for this session.

    Returns:
        De-anonymised text. Unresolved tokens are left in place and logged as warnings.
    """
    def replace_token(match: re.Match) -> str:
        token = match.group(1)
        original = token_map.resolve_token(token)
        if original is None:
            logger.warning("Unresolved token: %s — left in place", token)
            return match.group(0)  # Leave unresolved tokens intact
        return original

    result = _TOKEN_RE.sub(replace_token, text)

    resolved = len(_TOKEN_RE.findall(text)) - len(_TOKEN_RE.findall(result))
    logger.info("Deanonymised %d token(s) in text", resolved)
    return result


def deanonymise_file(
    input_path: str,
    output_path: str,
    token_map: TokenMap,
) -> int:
    """
    Read a redacted file, deanonymise it, and write the result.

    Args:
        input_path:  Path to the redacted file.
        output_path: Path to write the de-anonymised output.
        token_map:   TokenMap instance for this session.

    Returns:
        Number of tokens resolved.
    """
    text = Path(input_path).read_text(encoding="utf-8")
    original_tokens = set(_TOKEN_RE.findall(text))

    deanonymised = deanonymise_text(text, token_map)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(deanonymised, encoding="utf-8")

    remaining_tokens = set(_TOKEN_RE.findall(deanonymised))
    resolved_count = len(original_tokens) - len(remaining_tokens)

    logger.info(
        "Deanonymised file %s -> %s: %d/%d tokens resolved",
        input_path, output_path, resolved_count, len(original_tokens),
    )

    if remaining_tokens:
        logger.warning("Unresolved tokens in output: %s", sorted(remaining_tokens))

    return resolved_count
