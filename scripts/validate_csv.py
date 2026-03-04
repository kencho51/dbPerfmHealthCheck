#!/usr/bin/env python
"""
Standalone CSV validation CLI — usable without the FastAPI server.

Usage:
    uv run scripts/validate_csv.py --file data/Jan2026/maxElapsedQueriesProdJan26.csv
    uv run scripts/validate_csv.py --directory data/Jan2026

Exit code:
    0 — all files valid (warnings allowed)
    1 — one or more files have errors
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure project root is on sys.path so `api` package can be imported
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from api.services.validator import ValidationResult, validate_csv, validate_directory

# ---------------------------------------------------------------------------
# Terminal colours (no external dependency — plain ANSI)
# ---------------------------------------------------------------------------
_RESET  = "\033[0m"
_GREEN  = "\033[92m"
_YELLOW = "\033[93m"
_RED    = "\033[91m"
_CYAN   = "\033[96m"
_BOLD   = "\033[1m"


def _colour(text: str, code: str) -> str:
    return f"{code}{text}{_RESET}"


def _print_result(filename: str, r: ValidationResult) -> None:
    status_str = (
        _colour("VALID  ", _GREEN + _BOLD)
        if r.is_valid
        else _colour("INVALID", _RED + _BOLD)
    )
    print(f"\n{status_str}  {_colour(filename, _CYAN)}")
    print(f"  Type        : {r.file_type}")
    print(f"  Environment : {r.environment}")
    print(f"  Rows        : {r.row_count:,}")

    if r.null_rates:
        print("  Null rates  :")
        for col, rate in r.null_rates.items():
            colour = _RED if rate > 0.5 else (_YELLOW if rate > 0.1 else _GREEN)
            print(f"    {col:<30} {_colour(f'{rate:.1%}', colour)}")

    if r.warnings:
        print(f"  {_colour('Warnings', _YELLOW)} ({len(r.warnings)}):")
        for w in r.warnings:
            print(f"    ⚠  {w}")

    if r.errors:
        print(f"  {_colour('Errors', _RED)} ({len(r.errors)}):")
        for e in r.errors:
            print(f"    ✗  {e}")


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="validate_csv",
        description="Dry-run validate Splunk performance CSV files.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--file", "-f", type=str, help="Path to a single CSV file")
    group.add_argument("--directory", "-d", type=str, help="Directory containing CSV files")
    args = parser.parse_args()

    if args.file:
        path = Path(args.file)
        if not path.is_absolute():
            path = Path.cwd() / path
        if not path.exists():
            print(_colour(f"Error: file not found: {path}", _RED))
            return 1
        results = [(path.name, validate_csv(path))]
    else:
        directory = Path(args.directory)
        if not directory.is_absolute():
            directory = Path.cwd() / directory
        if not directory.exists() or not directory.is_dir():
            print(_colour(f"Error: directory not found: {directory}", _RED))
            return 1
        csv_files = sorted(directory.glob("*.csv"))
        if not csv_files:
            print(_colour(f"No CSV files found in {directory}", _YELLOW))
            return 0
        results = [(f.name, validate_csv(f)) for f in csv_files]

    # Print per-file summaries
    any_errors = False
    for filename, r in results:
        _print_result(filename, r)
        if not r.is_valid:
            any_errors = True

    # Overall summary
    total = len(results)
    valid = sum(1 for _, r in results if r.is_valid)
    print(f"\n{'─' * 60}")
    print(f"  {_colour(str(valid), _GREEN)} / {total} files valid", end="")
    if any_errors:
        print(f"  {_colour('— fix errors before ingesting', _RED)}")
    else:
        print(f"  {_colour('✓ ready to upload', _GREEN)}")

    return 1 if any_errors else 0


if __name__ == "__main__":
    sys.exit(main())
