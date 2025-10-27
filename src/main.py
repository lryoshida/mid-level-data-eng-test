# main.py
# Orchestrates the two pipelines:
#   1) "jordan"  → pdf_processor.py (Jordan 2019 specs) → optional analyzer_pdf.py
#   2) "invoice" → invoice_processor.py (invoice line items) → optional analyzer_invoice.py
#
# Usage examples (run from repo root or from src/):
#   python src/main.py --task jordan
#   python src/main.py --task invoice
#   python src/main.py --task invoice --pdf "data/input/MyInvoice.pdf" --pg-url "postgresql+psycopg2://..."
#
# Notes:
# - You can also pull input from S3 or a URL with --s3-uri or --url (takes precedence over --pdf if provided).
# - Set --no-analyze to skip analyzer stage.
# - Pass --replace to recreate/truncate tables before loading.

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


def run_or_die(cmd: list[str]) -> None:
    """Run a subprocess command; exit with same return code on failure."""
    print("➤ Running:", " ".join(map(str, cmd)))
    try:
        result = subprocess.run(cmd, check=False)
    except FileNotFoundError as e:
        print(f"✖ Command not found: {e}")
        sys.exit(1)

    if result.returncode != 0:
        print(f"✖ Command failed with exit code {result.returncode}")
        sys.exit(result.returncode)
    print("✓ Done\n")


def build_source_args(args: argparse.Namespace) -> list[str]:
    """Translate mutually exclusive input source flags into CLI args for processors."""
    if args.s3_uri:
        return ["--s3-uri", args.s3_uri]
    if args.url:
        return ["--url", args.url]
    # default to local file path
    return ["--pdf", str(args.pdf)]


def ensure_folders(paths: list[Path]) -> None:
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)


def main() -> None:
    # Base paths relative to this file (expected inside src/)
    base_path = Path(__file__).resolve().parent
    repo_root = base_path.parent
    data_in = repo_root / "data" / "input"
    data_out = repo_root / "data" / "output"
    reports = repo_root / "reports"

    parser = argparse.ArgumentParser(
        description="Pipeline launcher for Jordan specs and Invoice extraction."
    )
    parser.add_argument(
        "--task",
        required=True,
        choices=["jordan", "invoice"],
        help="Which pipeline to run.",
    )

    # Input selection (mutually exclusive): pdf (default), s3, or url
    src_group = parser.add_mutually_exclusive_group()
    src_group.add_argument("--pdf", type=Path, help="Local PDF/Image path to process.")
    src_group.add_argument("--s3-uri", help="S3 URI, e.g., s3://bucket/key.pdf")
    src_group.add_argument("--url", help="HTTP/HTTPS URL to fetch the file from.")

    # Outputs and DB
    parser.add_argument(
        "--out-xlsx",
        type=Path,
        help="Optional Excel output path (e.g., data/output/out.xlsx).",
    )
    parser.add_argument("--pg-url", help="PostgreSQL URL. If omitted, uses $PG_URL from env.")
    parser.add_argument("--schema", default="public", help="Target DB schema (default: public).")
    parser.add_argument(
        "--replace",
        action="store_true",
        help="If passed, (re)create or truncate tables before loading.",
    )

    # Control analyzer stage
    parser.add_argument(
        "--no-analyze",
        action="store_true",
        help="Skip the analyzer stage.",
    )

    args = parser.parse_args()

    # Resolve defaults based on task if not provided
    if args.task == "jordan":
        default_pdf = data_in / "Jordan-2019-Standard-Specifications-for-Construction.pdf"
        default_xlsx = data_out / "jordan_specs_extracted.xlsx"
        processor = base_path / "pdf_processor.py"
        analyzer = base_path / "analyzer_pdf.py"
    else:  # invoice
        default_pdf = data_in / "Invoice_1.pdf"
        default_xlsx = data_out / "invoices_extracted.xlsx"
        processor = base_path / "invoice_processor.py"  # IMPORTANT: fixed to the correct script
        analyzer = base_path / "analyzer_invoice.py"

    # Fill defaults
    if not (args.s3_uri or args.url or args.pdf):
        args.pdf = default_pdf
    if args.out_xlsx is None:
        args.out_xlsx = default_xlsx

    # Ensure output folders exist
    ensure_folders([data_out, reports, data_in])

    # Build base processor command
    if not processor.exists():
        print(f"✖ Processor script not found: {processor}")
        sys.exit(1)

    proc_cmd = [sys.executable, str(processor)]
    proc_cmd += build_source_args(args)

    # Optional outputs
    if args.out_xlsx:
        # Make parent dirs just in case
        args.out_xlsx.parent.mkdir(parents=True, exist_ok=True)
        proc_cmd += ["--out-xlsx", str(args.out_xlsx)]

    # DB flags
    pg_url = args.pg_url or os.getenv("PG_URL")
    if pg_url:
        proc_cmd += ["--pg-url", pg_url]
    if args.schema:
        proc_cmd += ["--schema", args.schema]
    if args.replace:
        proc_cmd += ["--replace"]

    # Run processor
    print(f"=== [{args.task.upper()}] Extract stage ===")
    run_or_die(proc_cmd)

    # Analyzer stage (optional)
    if args.no_analyze:
        print("⏭  Analyzer stage skipped (--no-analyze).")
        return

    if not analyzer.exists():
        print(f"⚠ Analyzer not found: {analyzer}. Skipping analysis.")
        return

    print(f"=== [{args.task.upper()}] Analyzer stage ===")
    ana_cmd = [sys.executable, str(analyzer)]
    if pg_url:
        ana_cmd += ["--pg-url", pg_url]
    if args.schema:
        ana_cmd += ["--schema", args.schema]
    # analyzer_pdf.py supports --save-csv; analyzer_invoice.py does not require it.
    if args.task == "jordan":
        ana_cmd += ["--save-csv"]

    run_or_die(ana_cmd)


if __name__ == "__main__":
    main()
