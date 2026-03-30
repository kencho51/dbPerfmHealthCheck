"""
Static mapping of database hostnames → infrastructure system names.

Source: data/ITWS_DB_Hosts.csv (last synced March 2026).
Hardcoded here to avoid file I/O on every server start and to allow the CSV
to be excluded from version control.

Provides:
  SYSTEM_HOSTS : dict[str, list[str]]  — system → list of UPPERCASE hostnames
  ALL_SYSTEMS  : list[str]             — sorted list of unique system names
  apply_system_filter(stmt, system)    — SQLModel helper; adds WHERE UPPER(host) IN (...)
"""

from __future__ import annotations

from sqlalchemy import func, text
from sqlmodel import col

from api.models import RawQuery

# ---------------------------------------------------------------------------
# Host → System mapping  (update manually when infrastructure changes)
# ---------------------------------------------------------------------------

SYSTEM_HOSTS: dict[str, list[str]] = {
    "AP": [
        "WINDB01HV01N",
        "WINDB01HV02N",
        "WINDB01ST01N",
        "WINDB01ST02N",
        "WINDB02HV01N",
        "WINDB02HV02N",
        "WINDB02ST01N",
        "WINDB02ST02N",
        "WINDB08HV01N",
        "WINDB08HV02N",
        "WINDB08ST01N",
        "WINDB08ST02N",
        "WINDB09HV01N",
        "WINDB09HV02N",
        "WINDB09ST01N",
        "WINDB09ST02N",
        "WINDB11HV01N",
        "WINDB11HV02N",
        "WINDB11ST01N",
        "WINDB11ST02N",
    ],
    "BCS-AA": ["AADBSRV21", "AADBSRV22", "AADBSRV23", "AADBSRV24"],
    "BCS-BA": ["BADBSRV21", "BADBSRV22", "BADBSRV23", "BADBSRV24"],
    "CMGC": ["CMGCSRV21", "CMGCSRV22", "CMGCSRV23", "CMGCSRV24"],
    "FO": [
        "WINFODB04HV11",
        "WINFODB04HV12",
        "WINFODB04ST11",
        "WINFODB04ST12",
        "WINFODB05HV11",
        "WINFODB05HV12",
        "WINFODB05ST11",
        "WINFODB05ST12",
        "WINFODB06HV11",
        "WINFODB06HV12",
        "WINFODB06ST11",
        "WINFODB06ST12",
        "WINFODB07HV11",
        "WINFODB07HV12",
        "WINFODB07ST11",
        "WINFODB07ST12",
        "WINFODB10HV01",
        "WINFODB10HV02",
        "WINFODB10ST01",
        "WINFODB10ST02",
    ],
    "IDA": [
        "IDAHVBDB01",
        "IDAHVBDB02",
        "IDAHVBDB03",
        "IDAHVBDB04",
        "IDASTBDB01",
        "IDASTBDB02",
        "IDASTBDB03",
        "IDASTBDB04",
    ],
    "PMU.COL": ["WINDB03HV11", "WINDB03HV12", "WINDB03ST11", "WINDB03ST12"],
    "PMU.ODPS": [
        "WINODPSMDBST01",
        "WINODPSMDBST02",
        "WINODPSMDBST03",
        "WINODPSMDBHV01",
        "WINODPSMDBHV02",
    ],
    "PTRM": ["PTRMMDBST01", "PTRMMDBST02", "PTRMMDBST03", "PTRMMDBHV01", "PTRMMDBHV02"],
    "TRD.QFM": ["TQFMWDB1ST11", "TQFMWDB1ST12", "TQFMWDB1HV11", "TQFMWDB1HV12"],
    "WCR": ["WGCSRV31", "WGCSRV32", "WGCSRV33", "WGCSRV34"],
}

ALL_SYSTEMS: list[str] = sorted(SYSTEM_HOSTS.keys())

# ---------------------------------------------------------------------------
# Query helper
# ---------------------------------------------------------------------------


def apply_system_filter(stmt, system: str | None):
    """
    Restrict a RawQuery-based SQLModel statement to hosts belonging to the
    given infrastructure system.

    Matching is case-insensitive (UPPER(host) IN (...)) so rows with either
    'WINFODB06HV11' or 'winfodb06hv11' are included.

    Returns the statement unchanged when system is None / empty.
    Returns a zero-row statement when system is not found in the mapping.
    """
    if not system:
        return stmt
    hosts = SYSTEM_HOSTS.get(system, [])
    if not hosts:
        # Unknown system — produce no rows rather than silently ignoring
        return stmt.where(text("1=0"))
    return stmt.where(func.upper(col(RawQuery.host)).in_(hosts))
