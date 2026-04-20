"""
run.py — Single entry point for the ATO Year-End Report Generator.

Usage:
  python run.py            — Run the full pipeline
  python run.py --validate — Check setup (API key, spaCy model, input folder, etc.)
"""

import argparse
import logging
import sys
from pathlib import Path

from rich.console import Console
from rich.table import Table
from rich import box

console = Console()

# ---------------------------------------------------------------------------
# Logging setup — must happen before any project imports
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def validate_setup() -> bool:
    """
    Check that all required components are installed and configured.
    Prints a Rich status table. Returns True if all checks pass.
    """
    checks = []

    # 1. GEMINI_API_KEY
    import config
    checks.append((
        "GEMINI_API_KEY",
        bool(config.GEMINI_API_KEY),
        "Set in .env file" if config.GEMINI_API_KEY else "Not set — add to .env",
    ))

    # 2. spaCy en_core_web_lg
    try:
        import spacy
        nlp = spacy.load("en_core_web_lg")
        checks.append(("spaCy en_core_web_lg", True, f"Loaded ({spacy.__version__})"))
    except OSError:
        checks.append((
            "spaCy en_core_web_lg",
            False,
            "Not installed — run: python -m spacy download en_core_web_lg",
        ))

    # 3. Presidio
    try:
        from presidio_analyzer import AnalyzerEngine  # noqa: F401
        checks.append(("presidio-analyzer", True, "OK"))
    except ImportError:
        checks.append(("presidio-analyzer", False, "Not installed — pip install presidio-analyzer"))

    # 4. Input folder
    input_folder = Path(config.INPUT_FOLDER)
    if not input_folder.exists():
        checks.append(("input/ folder", False, f"Does not exist: {input_folder}"))
    else:
        files = [f for f in input_folder.iterdir() if f.is_file()]
        if files:
            names = ", ".join(f.name for f in files[:5])
            checks.append(("input/ folder", True, f"{len(files)} file(s): {names}"))
        else:
            checks.append(("input/ folder", False, "Empty — drop Xero exports here"))

    # 5. LangGraph
    try:
        import langgraph  # noqa: F401
        checks.append(("langgraph", True, "OK"))
    except ImportError:
        checks.append(("langgraph", False, "Not installed — pip install langgraph"))

    # 6. ChromaDB
    try:
        import chromadb  # noqa: F401
        checks.append(("chromadb", True, "OK"))
    except ImportError:
        checks.append(("chromadb", False, "Not installed — pip install chromadb"))

    # 7. pdfplumber
    try:
        import pdfplumber  # noqa: F401
        checks.append(("pdfplumber", True, "OK"))
    except ImportError:
        checks.append(("pdfplumber", False, "Not installed — pip install pdfplumber"))

    # 8. pandas + openpyxl
    try:
        import pandas  # noqa: F401
        import openpyxl  # noqa: F401
        checks.append(("pandas + openpyxl", True, "OK"))
    except ImportError as exc:
        checks.append(("pandas + openpyxl", False, str(exc)))

    # Render results
    table = Table(title="Setup Validation", box=box.ROUNDED)
    table.add_column("Check", style="cyan")
    table.add_column("Status", justify="center")
    table.add_column("Details")

    all_pass = True
    for name, ok, detail in checks:
        status = "[bold green]PASS[/bold green]" if ok else "[bold red]FAIL[/bold red]"
        table.add_row(name, status, detail)
        if not ok:
            all_pass = False

    console.print()
    console.print(table)

    if all_pass:
        console.print("\n[bold green]All checks passed. Ready to run.[/bold green]")
    else:
        console.print("\n[bold red]Some checks failed. Fix the issues above before running.[/bold red]")

    return all_pass


def main():
    parser = argparse.ArgumentParser(
        description="ATO Year-End Report Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run.py --validate     # Check setup
  python run.py                # Run full pipeline
        """,
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate setup and exit without running the pipeline",
    )
    args = parser.parse_args()

    if args.validate:
        ok = validate_setup()
        sys.exit(0 if ok else 1)

    # Full pipeline run
    console.print("[bold]ATO Year-End Report Generator[/bold]")
    console.print("Starting pipeline...\n")

    try:
        from pipeline.graph import run_pipeline
        final_state = run_pipeline()

        console.print("\n[bold green]Pipeline completed successfully.[/bold green]")
        console.print(f"  Final report:    {final_state.get('final_report_path')}")
        console.print(f"  Redacted report: {final_state.get('redacted_report_path')}")
        console.print(f"  Token map:       {final_state.get('token_map_path')}")

    except KeyboardInterrupt:
        console.print("\n[bold red]Pipeline interrupted by user.[/bold red]")
        sys.exit(1)
    except Exception as exc:
        console.print(f"\n[bold red]Pipeline failed:[/bold red] {exc}")
        logger.exception("Pipeline error")
        sys.exit(1)


if __name__ == "__main__":
    main()
