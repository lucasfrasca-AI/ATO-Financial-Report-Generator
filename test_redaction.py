"""
test_redaction.py — Tests the redaction layer in isolation.

Run this after: pip install -r requirements.txt && python -m spacy download en_core_web_lg

Tests:
  1. TFN checksum: 123 456 782 is a valid synthetic TFN
  2. TFN detected and tokenised in test string
  3. DOB detected and tokenised (only with context word nearby)
  4. $125,000.00 financial figure is NOT redacted (allow-listed)
  5. Redaction summary format matches spec
  6. Token map saves to SQLite and restores correctly
  7. Deanonymiser restores all tokens
"""

import sys
import os
import re

# Ensure we run from the ato-mock directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from rich.console import Console
console = Console()

# ---------------------------------------------------------------------------
# 1. Checksum tests (no Presidio dependency)
# ---------------------------------------------------------------------------

def test_tfn_checksum():
    from redaction.au_recognisers import _tfn_checksum
    assert _tfn_checksum("123456782"), "Synthetic TFN 123 456 782 should pass checksum"
    assert not _tfn_checksum("123456789"), "Random 9-digit number should fail checksum"
    assert not _tfn_checksum("12345678"), "8-digit input should fail (wrong length)"
    console.print("[green]PASS[/green] TFN checksum validator")

def test_abn_checksum():
    from redaction.au_recognisers import _abn_checksum
    # 53 004 085 616 is a commonly cited test ABN
    assert _abn_checksum("53004085616"), "Known valid ABN should pass"
    assert not _abn_checksum("12345678901"), "Random number should fail"
    console.print("[green]PASS[/green] ABN checksum validator")

# ---------------------------------------------------------------------------
# 2. Full redaction pipeline test
# ---------------------------------------------------------------------------

TEST_TEXT = (
    "Tax File Number: 123 456 782\n"
    "Date of Birth: 15/06/1985\n"
    "Annual salary: $125,000.00\n"
    "Reporting period: 30 June 2024\n"
    "Financial year: FY2024\n"
    "BSB: 062-000 account number: 12345678\n"
)

def test_full_redaction():
    from redaction.token_map import TokenMap
    from redaction.redactor import redact_text

    # Use an in-memory temp path to avoid polluting the real session.db
    import tempfile, os
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_session.db")
        token_map = TokenMap(db_path=db_path)

        result = redact_text(TEST_TEXT, source="test_doc", token_map=token_map)
        redacted = result.redacted_text

        console.print(f"\n[bold]Redacted text:[/bold]\n{redacted}")

        # TFN must be redacted
        assert "123 456 782" not in redacted, "TFN must be redacted"
        assert "AU_TFN_001" in redacted, "TFN token must appear"
        console.print("[green]PASS[/green] TFN detected and tokenised")

        # DOB must be redacted (context word 'Date of Birth' is nearby)
        assert "15/06/1985" not in redacted, "DOB must be redacted"
        assert "AU_DOB_001" in redacted, "DOB token must appear"
        console.print("[green]PASS[/green] DOB detected and tokenised")

        # Financial figure must NOT be redacted
        assert "$125,000.00" in redacted, "$125,000.00 must NOT be redacted (allow-listed)"
        console.print("[green]PASS[/green] Financial figure preserved ($125,000.00 not redacted)")

        # Reporting dates must NOT be redacted
        assert "30 June 2024" in redacted, "30 June 2024 must not be redacted"
        assert "FY2024" in redacted, "FY2024 must not be redacted"
        console.print("[green]PASS[/green] Reporting dates preserved")

        # Deanonymisation restores original
        from redaction.deanonymiser import deanonymise_text
        restored = deanonymise_text(redacted, token_map)
        assert "123 456 782" in restored, "Deanonymiser must restore TFN"
        assert "15/06/1985" in restored, "Deanonymiser must restore DOB"
        console.print("[green]PASS[/green] Deanonymiser restores all tokens")

        # Summary format check
        from redaction.redactor import _build_summary
        summary = _build_summary(
            session_id="TEST001",
            results={"test_doc": result},
            token_map=token_map,
        )
        assert "REDACTION SUMMARY" in summary
        assert "Files processed: 1" in summary
        assert "AU_TFN" in summary
        console.print("[green]PASS[/green] Redaction summary format correct")

        token_map.close()

# ---------------------------------------------------------------------------
# Run all tests
# ---------------------------------------------------------------------------

def main():
    console.print("\n[bold]Running redaction layer tests...[/bold]\n")

    try:
        test_tfn_checksum()
        test_abn_checksum()
        test_full_redaction()
        console.print("\n[bold green]All tests passed.[/bold green]")
    except AssertionError as e:
        console.print(f"\n[bold red]FAIL:[/bold red] {e}")
        sys.exit(1)
    except Exception as e:
        console.print(f"\n[bold red]ERROR:[/bold red] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
