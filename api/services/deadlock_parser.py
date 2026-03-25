"""
DeadlockParser — parse a Splunk _raw deadlock XML-log event into structured rows.

One Splunk _raw event contains the entire deadlock graph for ONE incident.
This parser produces ONE ``DeadlockProcess`` per process involved (victim +
waiters), so a 2-way deadlock yields 2 rows and a 3-way deadlock yields 3 rows.

Edge cases handled
------------------
* **Interleaved spids** (3-way deadlocks): SQL Server writes the deadlock graph
  simultaneously from two scheduler threads, so lines from spid43s and spid416s
  are interleaved.  We strip the timestamp+spid prefix from every line and
  deduplicate blank-equivalent lines before parsing.

* **Stored-proc executions**: The actual SQL causing the deadlock appears
  *between* the ``frame procname=db.schema.proc`` line and the next ``frame``
  or ``inputbuf`` line.  ``inputbuf`` in this case shows only
  ``Proc [Database Id = …]``.

* **Adhoc / JDBC queries**: SQL appears *after* the ``inputbuf`` marker.
  ``executionStack`` shows only ``frame procname=adhoc`` followed by
  ``unknown``.

* **UpdateQPStats (auto-stats)**: Internal SQL Server deadlocks with no SQL
  in either section.  Classified by ``transactionname=UpdateQPStats``.

* **Empty fragment rows**: 3-way deadlocks sometimes split across two Splunk
  events due to log-line limits.  Fragment rows (empty ``id`` column) are
  silently skipped by the caller; the parser itself handles duplicate process
  blocks by tracking seen pids.

Output
------
Each ``DeadlockProcess`` becomes one row in ``raw_query`` with:
  * ``query_details`` = ``sql_text`` (best SQL candidate)
  * ``extra_metadata`` = JSON with all structured deadlock fields
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Compiled patterns
# ---------------------------------------------------------------------------

# Strips "2026-02-27 13:07:26.70 spid113s   " from the start of each line.
_TS_PREFIX = re.compile(
    r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+\s+spid\S+\s*",
    re.MULTILINE,
)

# Splits the clean text at each "process id=" anchor (start of a process block).
_PROCESS_SPLIT = re.compile(r"(?m)^[\t ]*process id=")

# Per-attribute patterns — applied to the full "process id=<attr_line>".
_ATTRS: dict[str, re.Pattern] = {
    "spid":            re.compile(r"\bspid=(\d+)\b"),
    "kpid":            re.compile(r"\bkpid=(\d+)\b"),
    "logused":         re.compile(r"\blogused=(\d+)\b"),
    "waitresource":    re.compile(r"\bwaitresource=(.+?)\s+(?:waittime|ownerId)="),
    "waittime":        re.compile(r"\bwaittime=(\d+)\b"),
    "lockMode":        re.compile(r"\blockMode=(\S+)"),
    "trancount":       re.compile(r"\btrancount=(\d+)\b"),
    "transactionname": re.compile(r"\btransactionname=(\S+)"),
    "lasttranstarted": re.compile(r"\blasttranstarted=(\S+)"),
    "isolationlevel":  re.compile(r"\bisolationlevel=([^(]+\(\d+\))"),
    "loginname":       re.compile(r"\bloginname=(\S+)"),
    "clientapp":       re.compile(r"\bclientapp=(.+?)\s+hostname="),
    "apphost":         re.compile(r"\bhostname=(\S+)"),
    "currentdbname":   re.compile(r"\bcurrentdbname=(\w+)"),
    "lockTimeout":     re.compile(r"\blockTimeout=(\d+)"),
    "status":          re.compile(r"\bstatus=(\S+)"),
}

# Matches a real (non-adhoc, non-unknown) stored-proc frame line.
_REAL_FRAME   = re.compile(r"frame procname=(?!unknown|adhoc)(\S+)")
# Matches ANY frame line (to know when a new frame starts).
_ANY_FRAME    = re.compile(r"^\s*frame procname=")
# Lines that are just the word "unknown" (blank execution stack).
_UNKNOWN_ONLY = re.compile(r"^\s*unknown\s*$")
# A "Proc [Database Id …]" inputbuf stub — means the SQL is inside a stored proc.
_PROC_STUB    = re.compile(r"^\s*Proc \[Database Id")
# Starts with a DML/DQL/parameter-list keyword → actual SQL.
_DML_START    = re.compile(r"^\s*(?:SELECT|INSERT|UPDATE|DELETE|EXEC|WITH|MERGE|@\w)", re.I)


# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------

@dataclass
class DeadlockProcess:
    # Incident identity
    deadlock_id:     str = ""
    deadlock_victim: str = ""   # space-joined for multi-victim incidents
    splunk_time:     str = ""
    splunk_host:     str = ""
    is_victim:       bool = False

    # Process attributes (from the process-list block)
    pid:             str = ""
    spid:            str = ""
    kpid:            str = ""
    logused:         str = ""
    waitresource:    str = ""
    waittime:        str = ""
    lockMode:        str = ""
    trancount:       str = ""
    transactionname: str = ""
    lasttranstarted: str = ""
    isolationlevel:  str = ""
    loginname:       str = ""
    clientapp:       str = ""
    apphost:         str = ""
    currentdbname:   str = ""
    lockTimeout:     str = ""
    status:          str = ""

    # SQL extraction
    proc_name:       str = ""   # stored-proc name (if any)
    exec_sql:        str = ""   # SQL found inside executionStack (stored-proc body)
    inputbuf_sql:    str = ""   # SQL found after inputbuf marker (adhoc / JDBC)
    sql_text:        str = ""   # best candidate → stored as query_details

    def to_extra_metadata(self) -> str:
        """Serialise deadlock-specific fields to a JSON string for extra_metadata."""
        fields = {
            "deadlock_id":     self.deadlock_id,
            "deadlock_victim": self.deadlock_victim,
            "pid":             self.pid,
            "is_victim":       self.is_victim,
            "spid":            self.spid,
            "kpid":            self.kpid,
            "logused":         self.logused,
            "waitresource":    self.waitresource,
            "waittime":        self.waittime,
            "lockMode":        self.lockMode,
            "trancount":       self.trancount,
            "transactionname": self.transactionname,
            "lasttranstarted": self.lasttranstarted,
            "currentdbname":   self.currentdbname,
            "isolationlevel":  self.isolationlevel,
            "loginname":       self.loginname,
            "clientapp":       self.clientapp,
            "apphost":         self.apphost,
            "lockTimeout":     self.lockTimeout,
            "status":          self.status,
            "proc_name":       self.proc_name,
            "exec_sql":        self.exec_sql,
            "inputbuf_sql":    self.inputbuf_sql,
        }
        return json.dumps({k: v for k, v in fields.items() if v not in ("", False, None)},
                          ensure_ascii=False)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _strip_ts_prefix(raw: str) -> str:
    """
    Remove the per-line Splunk timestamp+spid prefix and deduplicate lines
    that differ only in leading whitespace (interleaved-spid artefact from
    3-way deadlocks).
    """
    stripped_raw = _TS_PREFIX.sub("", raw)
    lines = stripped_raw.splitlines()

    deduped: list[str] = []
    prev_core: str | None = None
    for line in lines:
        core = line.strip()
        if core != prev_core:
            deduped.append(line)
            prev_core = core
    return "\n".join(deduped)


def _clean_sql(fragments: list[str]) -> str:
    """Join fragments and normalise internal whitespace."""
    joined = " ".join(fragments)
    joined = re.sub(r"[\r\n\t]+", " ", joined)
    joined = re.sub(r"\s{2,}", " ", joined)
    return joined.strip()


def _extract_sql(block_lines: list[str]) -> tuple[str, str, str, str]:
    """
    State-machine pass over the lines of a process block (everything after
    the attribute line) to extract proc_name, exec_sql, inputbuf_sql, sql_text.

    States
    ------
    IDLE         → initial state, looking for section markers
    EXEC_STACK   → inside executionStack, watching for frame lines
    REAL_FRAME   → after a real (stored-proc) frame, collecting SQL lines
    INPUTBUF     → after 'inputbuf', collecting SQL lines
    """
    IDLE, EXEC_STACK, REAL_FRAME, INPUTBUF = range(4)
    state = IDLE

    proc_name:       str       = ""
    exec_sql_lines:  list[str] = []
    inputbuf_lines:  list[str] = []

    for line in block_lines:
        stripped = line.strip()

        # -- Section transitions (highest priority) ----------------------------
        if stripped == "executionStack":
            state = EXEC_STACK
            continue

        if stripped == "inputbuf":
            state = INPUTBUF
            continue

        # Resource-list or a new process block ends our scan.
        if stripped == "resource-list" or stripped.startswith("process id="):
            break

        # -- EXEC_STACK / REAL_FRAME state ------------------------------------
        if state in (EXEC_STACK, REAL_FRAME):
            if _ANY_FRAME.match(line):
                # New frame — determine if it's a real (stored-proc) frame.
                m = _REAL_FRAME.search(line)
                if m:
                    if not proc_name:
                        proc_name = m.group(1)
                    exec_sql_lines = []   # reset: collect SQL for THIS frame only
                    state = REAL_FRAME
                else:
                    # adhoc / unknown frame — stop collecting exec_sql
                    state = EXEC_STACK
                continue

            if _UNKNOWN_ONLY.match(line):
                continue   # skip blank placeholder lines

            if state == REAL_FRAME and stripped:
                exec_sql_lines.append(stripped)

        # -- INPUTBUF state ---------------------------------------------------
        elif state == INPUTBUF:
            if stripped and not _PROC_STUB.match(line) and not _UNKNOWN_ONLY.match(line):
                inputbuf_lines.append(stripped)

    exec_sql     = _clean_sql(exec_sql_lines)
    inputbuf_sql = _clean_sql(inputbuf_lines)

    # Best-candidate selection:
    #   1. inputbuf with recognisable DML/DQL (adhoc / JDBC path)
    #   2. exec_sql with recognisable DML/DQL (stored-proc body)
    #   3. non-empty inputbuf (param list + partial SQL)
    #   4. non-empty exec_sql
    #   5. stored-proc name as fallback
    sql_text = ""
    if inputbuf_sql and _DML_START.match(inputbuf_sql):
        sql_text = inputbuf_sql
    elif exec_sql and _DML_START.match(exec_sql):
        sql_text = exec_sql
    elif inputbuf_sql:
        sql_text = inputbuf_sql
    elif exec_sql:
        sql_text = exec_sql
    elif proc_name:
        sql_text = f"Proc: {proc_name}"

    return proc_name, exec_sql, inputbuf_sql, sql_text


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_raw(raw: str, splunk_time: str, splunk_host: str) -> list[DeadlockProcess]:
    """
    Parse one Splunk ``_raw`` deadlock event into one ``DeadlockProcess`` per
    process.  Returns an empty list when the event cannot be parsed.

    Parameters
    ----------
    raw:          The full ``_raw`` field value from Splunk.
    splunk_time:  The ``_time`` field (ISO-8601 string).
    splunk_host:  The ``host`` field (SQL Server node name).
    """
    if not raw or "deadlock" not in raw.lower():
        return []

    clean = _strip_ts_prefix(raw)

    # Extract all victim process IDs from this event.
    victims: set[str] = set(re.findall(r"deadlock victim=(\S+)", clean))
    if not victims:
        return []

    # Stable deadlock_id: hash of time + host + sorted victims.
    victim_key  = "|".join(sorted(victims))
    deadlock_id = hashlib.md5(
        f"{splunk_time}|{splunk_host}|{victim_key}".encode()
    ).hexdigest()[:16]

    # Split into process blocks on "process id=" anchors.
    # parts[0] = preamble (deadlock-list / process-list header)
    # parts[1+] = each process block (content *after* "process id=")
    parts = _PROCESS_SPLIT.split(clean)

    results: list[DeadlockProcess] = []
    seen_pids: set[str] = set()   # deduplicate process blocks from interleaved spids

    for part in parts[1:]:
        lines = [ln for ln in part.splitlines() if ln.strip()]
        if not lines:
            continue

        # First line = everything after "process id=" on the process attribute line.
        attr_line = lines[0]
        pid_match = re.match(r"(\S+)", attr_line)
        if not pid_match:
            continue
        pid = pid_match.group(1)

        if pid in seen_pids:
            continue   # interleaved-spid duplicate
        seen_pids.add(pid)

        # Build full attribute string for pattern matching.
        full_attr = f"process id={attr_line}"

        proc = DeadlockProcess(
            deadlock_id=deadlock_id,
            deadlock_victim=" ".join(sorted(victims)),
            splunk_time=splunk_time,
            splunk_host=splunk_host,
            is_victim=(pid in victims),
            pid=pid,
        )

        # Extract all structured attributes from the attribute line.
        for attr_name, pattern in _ATTRS.items():
            m = pattern.search(full_attr)
            if m:
                setattr(proc, attr_name, m.group(1).strip())

        # Skip tempdb processes — they produce no actionable information.
        if proc.currentdbname == "tempdb":
            continue

        # Extract SQL from the remaining lines of this block.
        proc.proc_name, proc.exec_sql, proc.inputbuf_sql, proc.sql_text = (
            _extract_sql(lines[1:])
        )

        # UpdateQPStats: SQL Server internal auto-stats update deadlock.
        # These never have SQL text — use transactionname as the canonical label.
        if not proc.sql_text and proc.transactionname == "UpdateQPStats":
            proc.sql_text = "UpdateQPStats (auto-stats deadlock)"

        # Skip rows with no meaningful SQL and no useful label at all.
        if not proc.sql_text:
            continue

        results.append(proc)

    return results
