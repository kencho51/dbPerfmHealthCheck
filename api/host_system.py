"""
Static mapping of database hostnames → infrastructure system names.

Loaded once at import from data/ITWS_DB_Hosts.csv.
CSV columns: Hostname, System  (e.g.  WINFODB06HV11, FO)

Provides:
  SYSTEM_HOSTS : dict[str, list[str]]  — system → list of UPPERCASE hostnames
  ALL_SYSTEMS  : list[str]             — sorted list of unique system names
  apply_system_filter(stmt, system)    — SQLModel helper; adds WHERE UPPER(host) IN (...)
"""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Optional

from sqlalchemy import func, text
from sqlmodel import col

from api.models import RawQuery

# ---------------------------------------------------------------------------
# Load CSV
# ---------------------------------------------------------------------------

_CSV_PATH = Path(__file__).parent.parent / "data" / "ITWS_DB_Hosts.csv"

SYSTEM_HOSTS: dict[str, list[str]] = {}

with _CSV_PATH.open(newline="", encoding="utf-8") as fh:
    for row in csv.DictReader(fh):
        system = row["System"].strip()
        host   = row["Hostname"].strip().upper()
        SYSTEM_HOSTS.setdefault(system, []).append(host)

ALL_SYSTEMS: list[str] = sorted(SYSTEM_HOSTS.keys())

# ---------------------------------------------------------------------------
# Query helper
# ---------------------------------------------------------------------------

def apply_system_filter(stmt, system: Optional[str]):
    """
    Restrict a RawQuery-based SQLModel statement to hosts belonging to the
    given infrastructure system.

    Matching is case-insensitive (UPPER(host) IN (...)) so rows with either
    'WINFODB06HV11' or 'winfodb06hv11' are included.

    Returns the statement unchanged when system is None / empty.
    Returns a zero-row statement when system is not found in the CSV mapping.
    """
    if not system:
        return stmt
    hosts = SYSTEM_HOSTS.get(system, [])
    if not hosts:
        # Unknown system — produce no rows rather than silently ignoring
        return stmt.where(text("1=0"))
    return stmt.where(func.upper(col(RawQuery.host)).in_(hosts))
