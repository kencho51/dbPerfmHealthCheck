"""
Unit tests for api/services/deadlock_parser.py.

Tests are grouped by scenario to mirror the real-data patterns observed in the
February 2026 Splunk CSVs:

  TestParseRaw           — core parse_raw() contract
  TestStoredProcPattern  — SQL extracted from executionStack (wagering_control)
  TestAdhocPattern       — SQL extracted from inputbuf (JDBC / .NET adhoc)
  TestUpdateQPStats      — Internal SQL Server auto-stats deadlock (no SQL text)
  TestThreeWayDeadlock   — 3-way deadlock with interleaved spid lines
  TestDeadlockProcess    — DeadlockProcess.to_extra_metadata() JSON output
  TestEdgeCases          — Empty / malformed / missing fields

Run:
    uv run pytest tests/test_deadlock_parser.py -v
"""
from __future__ import annotations

import json
import re

import pytest

from api.services.deadlock_parser import DeadlockProcess, parse_raw

# ---------------------------------------------------------------------------
# Shared raw event fixtures (simplified but structurally faithful to real data)
# ---------------------------------------------------------------------------

# Two-way deadlock: stored-proc pattern (wagering_control style).
# Both processes are running the same stored proc; victim is the second process.
_RAW_STORED_PROC = """\
2026-02-28 17:57:48.24 spid19s     deadlock-list
2026-02-28 17:57:48.24 spid19s      deadlock victim=processAAA
2026-02-28 17:57:48.24 spid19s       process-list
2026-02-28 17:57:48.24 spid19s        process id=processAAA taskpriority=0 logused=0 waitresource=KEY: 8:72057594065256448 (fe3f) waittime=3040 ownerId=801105423 transactionname=user_transaction lasttranstarted=2026-02-28T17:57:45.177 lockMode=X schedulerid=9 kpid=12832 status=suspended spid=371 sbid=0 ecid=0 trancount=1 clientapp=wc_odds_req hostname=wgclnxhv01 hostpid=4050 loginname=WCS_DM\\wc_app_svc isolationlevel=read committed (2) xactid=801105423 currentdb=8 currentdbname=wagering_control lockTimeout=4294967295
2026-02-28 17:57:48.24 spid19s         executionStack
2026-02-28 17:57:48.24 spid19s          frame procname=wagering_control.dbo.wcu_ins_collation line=301 stmtstart=24158 sqlhandle=0x0300080029AA
2026-02-28 17:57:48.24 spid19s     select @dummy = 1 from wagering_control..latest_pool_totals with (xlock, rowlock)
2026-02-28 17:57:48.24 spid19s     where meeting_id = @meeting_id and race_no = @race_no
2026-02-28 17:57:48.24 spid19s          frame procname=adhoc line=1 stmtstart=24 sqlhandle=0x0100050005a2
2026-02-28 17:57:48.24 spid19s     execute wagering_control..wcu_ins_collation  @P0
2026-02-28 17:57:48.24 spid19s         inputbuf
2026-02-28 17:57:48.24 spid19s     (@P0 bigint)execute wagering_control..wcu_ins_collation  @P0
2026-02-28 17:57:48.24 spid19s        process id=processBBB taskpriority=0 logused=0 waitresource=KEY: 8:72057594065256448 (157d) waittime=3040 ownerId=801105207 transactionname=user_transaction lasttranstarted=2026-02-28T17:57:45.163 lockMode=X schedulerid=3 kpid=15136 status=suspended spid=353 sbid=0 ecid=0 trancount=1 clientapp=wc_odds_req hostname=wgclnxhv03 hostpid=9126 loginname=WCS_DM\\wc_app_svc isolationlevel=read committed (2) xactid=801105207 currentdb=8 currentdbname=wagering_control lockTimeout=4294967295
2026-02-28 17:57:48.24 spid19s         executionStack
2026-02-28 17:57:48.24 spid19s          frame procname=wagering_control.dbo.wcu_ins_collation line=301 stmtstart=24158 sqlhandle=0x0300080029AA
2026-02-28 17:57:48.24 spid19s     select @dummy = 1 from wagering_control..latest_pool_totals with (xlock, rowlock)
2026-02-28 17:57:48.24 spid19s     where meeting_id = @meeting_id and race_no = @race_no
2026-02-28 17:57:48.24 spid19s         inputbuf
2026-02-28 17:57:48.24 spid19s     (@P0 bigint)execute wagering_control..wcu_ins_collation  @P0
2026-02-28 17:57:48.24 spid19s       resource-list
"""

# Two-way deadlock: adhoc / JDBC pattern (sps_db style).
# SQL appears inside inputbuf; executionStack has only adhoc frame + "unknown".
_RAW_ADHOC = """\
2026-02-22 04:01:25.64 spid36s     deadlock-list
2026-02-22 04:01:25.64 spid36s      deadlock victim=processCCC
2026-02-22 04:01:25.64 spid36s       process-list
2026-02-22 04:01:25.64 spid36s        process id=processCCC taskpriority=0 logused=25496 waitresource=PAGE: 5:1:282896 waittime=450 ownerId=16388467964 transactionname=implicit_transaction lasttranstarted=2026-02-22T04:01:25.173 lockMode=S schedulerid=23 kpid=12296 status=suspended spid=1395 sbid=0 ecid=0 trancount=1 clientapp=Microsoft JDBC Driver for SQL Server hostname=winfospsapphv14 hostpid=20279 loginname=WIN_DM\\win_sps_svc isolationlevel=read committed (2) xactid=16388467964 currentdb=5 currentdbname=sps_db lockTimeout=4294967295
2026-02-22 04:01:25.64 spid36s         executionStack
2026-02-22 04:01:25.64 spid36s          frame procname=adhoc line=1 stmtstart=156 sqlhandle=0x02000000fbbe88
2026-02-22 04:01:25.64 spid36s     unknown
2026-02-22 04:01:25.64 spid36s         inputbuf
2026-02-22 04:01:25.64 spid36s     (@P0 bigint,@P1 bigint)SELECT e.id, e.version FROM events e WHERE e.id = @P0
2026-02-22 04:01:25.64 spid36s        process id=processDDD taskpriority=0 logused=32456 waitresource=PAGE: 5:1:282897 waittime=449 ownerId=16388467958 transactionname=implicit_transaction lasttranstarted=2026-02-22T04:01:25.173 lockMode=S schedulerid=29 kpid=4520 status=suspended spid=768 sbid=0 ecid=0 trancount=1 clientapp=Microsoft JDBC Driver for SQL Server hostname=winfospsapphv14 hostpid=20279 loginname=WIN_DM\\win_sps_svc isolationlevel=read committed (2) xactid=16388467958 currentdb=5 currentdbname=sps_db lockTimeout=4294967295
2026-02-22 04:01:25.64 spid36s         executionStack
2026-02-22 04:01:25.64 spid36s          frame procname=adhoc line=1 stmtstart=156 sqlhandle=0x02000000fbbe88
2026-02-22 04:01:25.64 spid36s     unknown
2026-02-22 04:01:25.64 spid36s         inputbuf
2026-02-22 04:01:25.64 spid36s     (@P0 bigint,@P1 bigint)SELECT e.id, e.version FROM events e WHERE e.id = @P0
2026-02-22 04:01:25.64 spid36s       resource-list
"""

# UpdateQPStats: internal SQL Server background deadlock — no user SQL at all.
_RAW_AUTOSTATS = """\
2026-02-28 10:58:27.82 spid16s     deadlock-list
2026-02-28 10:58:27.82 spid16s      deadlock victim=processEEE
2026-02-28 10:58:27.82 spid16s       process-list
2026-02-28 10:58:27.82 spid16s        process id=processEEE taskpriority=20 logused=0 waitresource=METADATA: database_id = 5 STATS(object_id = 940230800, stats_id = 8) waittime=289 ownerId=2820712730 transactionname=UpdateQPStats lasttranstarted=2026-02-28T10:58:27.483 lockMode=Sch-M schedulerid=20 kpid=6312 status=background spid=471 sbid=0 ecid=0 trancount=1 currentdb=5 currentdbname=sps_db
2026-02-28 10:58:27.82 spid16s         executionStack
2026-02-28 10:58:27.82 spid16s         inputbuf
2026-02-28 10:58:27.82 spid16s        process id=processFFF taskpriority=-20 logused=0 waitresource=METADATA: database_id = 5 STATS(object_id = 940230800, stats_id = 8) waittime=289 transactionname=UpdateQPStats lockMode=Sch-M schedulerid=21 kpid=11692 status=background spid=201 sbid=0 ecid=0 trancount=0 currentdb=5 currentdbname=sps_db
2026-02-28 10:58:27.82 spid16s         executionStack
2026-02-28 10:58:27.82 spid16s         inputbuf
2026-02-28 10:58:27.82 spid16s       resource-list
"""

# 3-way deadlock: lines interleaved from two scheduler spids (spid43s, spid416s).
# Both SELECT processes wait on the same resource; the UPDATE holds the lock.
_RAW_THREE_WAY = """\
2026-02-28 10:11:36.05 spid43s     deadlock-list
2026-02-28 10:11:36.05 spid43s      deadlock victim=processGGG
2026-02-28 10:11:36.05 spid416s     deadlock victim=processIII
2026-02-28 10:11:36.05 spid43s       process-list
2026-02-28 10:11:36.05 spid43s        process id=processGGG taskpriority=0 logused=0 waitresource=KEY: 5:72057594042449920 (6620) waittime=549 ownerId=17329023631 transactionname=SELECT lasttranstarted=2026-02-28T10:11:35.357 lockMode=S schedulerid=15 kpid=1028 status=suspended spid=391 sbid=0 ecid=5 trancount=0 clientapp=.Net SqlClient Data Provider hostname=QCTOSCAAGT13 hostpid=3768 isolationlevel=read committed (2) currentdb=5 currentdbname=K2_Custom lockTimeout=4294967295
2026-02-28 10:11:36.05 spid416s       process id=processGGG taskpriority=0 logused=0 waitresource=KEY: 5:72057594042449920 (6620) waittime=549 ownerId=17329023631 transactionname=SELECT lasttranstarted=2026-02-28T10:11:35.357 lockMode=S schedulerid=15 kpid=1028 status=suspended spid=391 sbid=0 ecid=5 trancount=0 clientapp=.Net SqlClient Data Provider hostname=QCTOSCAAGT13 hostpid=3768 isolationlevel=read committed (2) currentdb=5 currentdbname=K2_Custom lockTimeout=4294967295
2026-02-28 10:11:36.05 spid43s         executionStack
2026-02-28 10:11:36.05 spid416s        executionStack
2026-02-28 10:11:36.05 spid43s          frame procname=adhoc line=1 sqlhandle=0x0200000093AA
2026-02-28 10:11:36.05 spid416s         frame procname=adhoc line=1 sqlhandle=0x0200000093AA
2026-02-28 10:11:36.05 spid43s     unknown
2026-02-28 10:11:36.05 spid416s    unknown
2026-02-28 10:11:36.05 spid43s         inputbuf
2026-02-28 10:11:36.05 spid416s        inputbuf
2026-02-28 10:11:36.05 spid43s     SELECT DISTINCT task_id FROM task_log WHERE name = 'Review'
2026-02-28 10:11:36.05 spid416s    SELECT DISTINCT task_id FROM task_log WHERE name = 'Review'
2026-02-28 10:11:36.05 spid43s        process id=processHHH taskpriority=0 logused=29240 waitresource=KEY: 5:72057594044481536 (2912) waittime=572 ownerId=17329029214 transactionname=user_transaction lasttranstarted=2026-02-28T10:11:35.483 lockMode=X schedulerid=28 kpid=15748 status=suspended spid=408 sbid=2 ecid=0 trancount=3 clientapp=Core .Net SqlClient Data Provider hostname=WINFOICSWFST11 hostpid=980 loginname=WIN_DM\\win_ics_svc isolationlevel=read uncommitted (1) xactid=17329029214 currentdb=5 currentdbname=K2_Custom lockTimeout=4294967295
2026-02-28 10:11:36.05 spid416s       process id=processHHH taskpriority=0 logused=29240 waitresource=KEY: 5:72057594044481536 (2912) waittime=572 ownerId=17329029214 transactionname=user_transaction lasttranstarted=2026-02-28T10:11:35.483 lockMode=X schedulerid=28 kpid=15748 status=suspended spid=408 sbid=2 ecid=0 trancount=3 clientapp=Core .Net SqlClient Data Provider hostname=WINFOICSWFST11 hostpid=980 loginname=WIN_DM\\win_ics_svc isolationlevel=read uncommitted (1) xactid=17329029214 currentdb=5 currentdbname=K2_Custom lockTimeout=4294967295
2026-02-28 10:11:36.05 spid43s         executionStack
2026-02-28 10:11:36.05 spid416s        executionStack
2026-02-28 10:11:36.05 spid43s          frame procname=K2_Custom.dbo.update_task_log line=31 stmtstart=1332 sqlhandle=0x030005009b87
2026-02-28 10:11:36.05 spid416s         frame procname=K2_Custom.dbo.update_task_log line=31 stmtstart=1332 sqlhandle=0x030005009b87
2026-02-28 10:11:36.05 spid43s     UPDATE [dbo].[task_log] SET [is_completed] = ISNULL(@isCompleted, [is_completed]) WHERE [task_id] = @task_id
2026-02-28 10:11:36.05 spid416s    UPDATE [dbo].[task_log] SET [is_completed] = ISNULL(@isCompleted, [is_completed]) WHERE [task_id] = @task_id
2026-02-28 10:11:36.05 spid43s         inputbuf
2026-02-28 10:11:36.05 spid43s     Proc [Database Id = 5 Object Id = 1301579675]
2026-02-28 10:11:36.05 spid416s        inputbuf
2026-02-28 10:11:36.05 spid43s        process id=processIII taskpriority=0 logused=0 waitresource=KEY: 5:72057594042449920 (6620) waittime=562 ownerId=17329023000 transactionname=SELECT lasttranstarted=2026-02-28T10:11:35.400 lockMode=S schedulerid=22 kpid=9908 status=suspended spid=120 sbid=0 ecid=3 trancount=0 clientapp=.Net SqlClient Data Provider hostname=QCTOSCAAGT13 hostpid=4588 isolationlevel=read committed (2) currentdb=5 currentdbname=K2_Custom lockTimeout=4294967295
2026-02-28 10:11:36.05 spid43s         executionStack
2026-02-28 10:11:36.05 spid43s          frame procname=adhoc line=1 sqlhandle=0x020000005501
2026-02-28 10:11:36.05 spid43s     unknown
2026-02-28 10:11:36.05 spid43s         inputbuf
2026-02-28 10:11:36.05 spid43s     SELECT DISTINCT task_id FROM task_log WHERE name = 'Submit'
2026-02-28 10:11:36.05 spid43s       resource-list
"""


# ---------------------------------------------------------------------------
# TestParseRaw — core contract
# ---------------------------------------------------------------------------

class TestParseRaw:
    def test_returns_empty_for_empty_string(self):
        assert parse_raw("", "2026-02-01T00:00:00.000+0800", "HOST1") == []

    def test_returns_empty_for_non_deadlock_text(self):
        assert parse_raw("some random log line", "2026-02-01T00:00:00.000+0800", "HOST1") == []

    def test_returns_empty_when_no_victim_marker(self):
        raw = "2026-01-01 00:00:00.00 spid1s   deadlock-list\n"
        assert parse_raw(raw, "2026-01-01T00:00:00.000+0800", "HOST1") == []

    def test_two_way_returns_two_processes(self):
        procs = parse_raw(_RAW_STORED_PROC, "2026-02-28T17:57:48.240+0800", "WGCSRV32")
        assert len(procs) == 2

    def test_all_items_are_deadlock_process_instances(self):
        procs = parse_raw(_RAW_STORED_PROC, "2026-02-28T17:57:48.240+0800", "WGCSRV32")
        assert all(isinstance(p, DeadlockProcess) for p in procs)

    def test_both_processes_share_same_deadlock_id(self):
        procs = parse_raw(_RAW_STORED_PROC, "2026-02-28T17:57:48.240+0800", "WGCSRV32")
        assert len({p.deadlock_id for p in procs}) == 1

    def test_deadlock_id_is_16_hex_chars(self):
        procs = parse_raw(_RAW_STORED_PROC, "2026-02-28T17:57:48.240+0800", "WGCSRV32")
        assert re.fullmatch(r"[0-9a-f]{16}", procs[0].deadlock_id)

    def test_deadlock_id_stable_for_same_input(self):
        """Same input must always produce the same deadlock_id (deterministic hash)."""
        t, h = "2026-02-28T17:57:48.240+0800", "WGCSRV32"
        id1 = parse_raw(_RAW_STORED_PROC, t, h)[0].deadlock_id
        id2 = parse_raw(_RAW_STORED_PROC, t, h)[0].deadlock_id
        assert id1 == id2

    def test_splunk_time_and_host_propagated(self):
        procs = parse_raw(_RAW_ADHOC, "2026-02-22T04:01:25.640+0800", "WINFODB05HV12")
        assert all(p.splunk_time == "2026-02-22T04:01:25.640+0800" for p in procs)
        assert all(p.splunk_host == "WINFODB05HV12" for p in procs)

    def test_exactly_one_victim_per_two_way(self):
        procs = parse_raw(_RAW_STORED_PROC, "2026-02-28T17:57:48.240+0800", "WGCSRV32")
        victims = [p for p in procs if p.is_victim]
        assert len(victims) == 1

    def test_victim_pid_matches_deadlock_victim_field(self):
        procs = parse_raw(_RAW_STORED_PROC, "2026-02-28T17:57:48.240+0800", "WGCSRV32")
        victim = next(p for p in procs if p.is_victim)
        assert victim.pid == "processAAA"

    def test_non_victim_is_not_flagged(self):
        procs = parse_raw(_RAW_STORED_PROC, "2026-02-28T17:57:48.240+0800", "WGCSRV32")
        waiter = next(p for p in procs if not p.is_victim)
        assert waiter.pid == "processBBB"


# ---------------------------------------------------------------------------
# TestStoredProcPattern
# ---------------------------------------------------------------------------

class TestStoredProcPattern:
    @pytest.fixture
    def procs(self):
        return parse_raw(_RAW_STORED_PROC, "2026-02-28T17:57:48.240+0800", "WGCSRV32")

    def test_proc_name_extracted(self, procs):
        for p in procs:
            assert "wcu_ins_collation" in p.proc_name

    def test_exec_sql_contains_select(self, procs):
        for p in procs:
            assert p.exec_sql.lower().startswith("select")

    def test_exec_sql_references_correct_table(self, procs):
        for p in procs:
            assert "latest_pool_totals" in p.exec_sql

    def test_sql_text_is_exec_sql_for_stored_proc(self, procs):
        """sql_text should be the exec_sql (from executionStack), not the inputbuf EXEC stub."""
        for p in procs:
            assert "latest_pool_totals" in p.sql_text
            assert p.sql_text == p.exec_sql

    def test_currentdbname_extracted(self, procs):
        for p in procs:
            assert p.currentdbname == "wagering_control"

    def test_lock_attributes_extracted(self, procs):
        for p in procs:
            assert p.lockMode == "X"
            assert p.waittime == "3040"
            assert p.lockTimeout == "4294967295"

    def test_isolation_level_extracted(self, procs):
        for p in procs:
            assert "read committed" in p.isolationlevel

    def test_loginname_extracted(self, procs):
        for p in procs:
            assert p.loginname == "WCS_DM\\wc_app_svc"

    def test_clientapp_extracted(self, procs):
        for p in procs:
            assert p.clientapp == "wc_odds_req"


# ---------------------------------------------------------------------------
# TestAdhocPattern
# ---------------------------------------------------------------------------

class TestAdhocPattern:
    @pytest.fixture
    def procs(self):
        return parse_raw(_RAW_ADHOC, "2026-02-22T04:01:25.640+0800", "WINFODB05HV12")

    def test_two_processes_returned(self, procs):
        assert len(procs) == 2

    def test_inputbuf_sql_extracted(self, procs):
        for p in procs:
            assert p.inputbuf_sql.startswith("(@P")
            assert "SELECT" in p.inputbuf_sql.upper()

    def test_sql_text_is_inputbuf_sql(self, procs):
        for p in procs:
            assert p.sql_text == p.inputbuf_sql

    def test_proc_name_empty_for_adhoc(self, procs):
        """No stored proc involved — proc_name must be empty."""
        for p in procs:
            assert p.proc_name == ""

    def test_currentdbname_is_sps_db(self, procs):
        for p in procs:
            assert p.currentdbname == "sps_db"

    def test_victim_is_processCCC(self, procs):
        victim = next(p for p in procs if p.is_victim)
        assert victim.pid == "processCCC"

    def test_waitresource_contains_page(self, procs):
        for p in procs:
            assert p.waitresource.startswith("PAGE:")


# ---------------------------------------------------------------------------
# TestUpdateQPStats
# ---------------------------------------------------------------------------

class TestUpdateQPStats:
    @pytest.fixture
    def procs(self):
        return parse_raw(_RAW_AUTOSTATS, "2026-02-28T10:58:27.820+0800", "WINDB01ST02N")

    def test_two_processes_returned(self, procs):
        assert len(procs) == 2

    def test_sql_text_is_autostats_label(self, procs):
        for p in procs:
            assert "UpdateQPStats" in p.sql_text

    def test_transactionname_is_updateqpstats(self, procs):
        for p in procs:
            assert p.transactionname == "UpdateQPStats"

    def test_lockmode_is_sch_m(self, procs):
        for p in procs:
            assert p.lockMode == "Sch-M"

    def test_waitresource_contains_metadata(self, procs):
        for p in procs:
            assert "METADATA" in p.waitresource

    def test_exec_sql_is_empty(self, procs):
        """No user SQL should be extracted for auto-stats deadlocks."""
        for p in procs:
            assert p.exec_sql == ""


# ---------------------------------------------------------------------------
# TestThreeWayDeadlock
# ---------------------------------------------------------------------------

class TestThreeWayDeadlock:
    @pytest.fixture
    def procs(self):
        return parse_raw(_RAW_THREE_WAY, "2026-02-28T10:11:36.050+0800", "WINFODB06ST11")

    def test_returns_three_processes(self, procs):
        """3-way deadlock must produce exactly 3 process rows (no duplicates from interleaving)."""
        assert len(procs) == 3

    def test_pids_are_unique(self, procs):
        pids = [p.pid for p in procs]
        assert len(pids) == len(set(pids)), "Duplicate PIDs from interleaved spid lines"

    def test_two_victims_in_three_way(self, procs):
        """3-way deadlock has two victims in the raw data."""
        victims = [p for p in procs if p.is_victim]
        assert len(victims) == 2

    def test_processGGG_is_victim(self, procs):
        victim = next(p for p in procs if p.pid == "processGGG")
        assert victim.is_victim is True

    def test_processIII_is_victim(self, procs):
        victim = next(p for p in procs if p.pid == "processIII")
        assert victim.is_victim is True

    def test_processHHH_is_not_victim(self, procs):
        waiter = next(p for p in procs if p.pid == "processHHH")
        assert waiter.is_victim is False

    def test_update_sql_extracted_for_processHHH(self, procs):
        waiter = next(p for p in procs if p.pid == "processHHH")
        assert "UPDATE" in waiter.sql_text.upper()
        assert "task_log" in waiter.sql_text

    def test_stored_proc_name_for_processHHH(self, procs):
        waiter = next(p for p in procs if p.pid == "processHHH")
        assert "update_task_log" in waiter.proc_name

    def test_select_sql_extracted_for_select_processes(self, procs):
        selects = [p for p in procs if p.pid in ("processGGG", "processIII")]
        for p in selects:
            assert "SELECT" in p.sql_text.upper()

    def test_all_in_same_database(self, procs):
        assert all(p.currentdbname == "K2_Custom" for p in procs)

    def test_all_share_same_deadlock_id(self, procs):
        assert len({p.deadlock_id for p in procs}) == 1


# ---------------------------------------------------------------------------
# TestDeadlockProcess — to_extra_metadata()
# ---------------------------------------------------------------------------

class TestDeadlockProcess:
    @pytest.fixture
    def process(self):
        procs = parse_raw(_RAW_STORED_PROC, "2026-02-28T17:57:48.240+0800", "WGCSRV32")
        return next(p for p in procs if p.is_victim)

    def test_to_extra_metadata_returns_string(self, process):
        result = process.to_extra_metadata()
        assert isinstance(result, str)

    def test_to_extra_metadata_is_valid_json(self, process):
        result = json.loads(process.to_extra_metadata())
        assert isinstance(result, dict)

    def test_mandatory_fields_present(self, process):
        meta = json.loads(process.to_extra_metadata())
        for field in ("deadlock_id", "pid", "lockMode", "waitresource", "waittime",
                      "currentdbname", "isolationlevel", "transactionname"):
            assert field in meta, f"Missing field in extra_metadata: {field}"

    def test_is_victim_true_in_metadata(self, process):
        meta = json.loads(process.to_extra_metadata())
        assert meta["is_victim"] is True

    def test_empty_fields_excluded_from_metadata(self):
        """Fields with empty string values must not appear in the JSON output."""
        proc = DeadlockProcess(deadlock_id="abc123", pid="proc1", sql_text="SELECT 1")
        meta = json.loads(proc.to_extra_metadata())
        # is_victim=False is falsy — must also be excluded
        assert "apphost" not in meta     # empty string → excluded
        assert "proc_name" not in meta   # empty string → excluded

    def test_deadlock_id_matches_process_attribute(self, process):
        meta = json.loads(process.to_extra_metadata())
        assert meta["deadlock_id"] == process.deadlock_id

    def test_metadata_lockmode_matches_attribute(self, process):
        meta = json.loads(process.to_extra_metadata())
        assert meta["lockMode"] == process.lockMode


# ---------------------------------------------------------------------------
# TestEdgeCases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_tempdb_processes_excluded(self):
        """Processes with currentdbname=tempdb must be silently dropped."""
        raw = """\
2026-01-01 00:00:00.00 spid1s   deadlock-list
2026-01-01 00:00:00.00 spid1s    deadlock victim=proc1
2026-01-01 00:00:00.00 spid1s     process-list
2026-01-01 00:00:00.00 spid1s      process id=proc1 logused=0 waitresource=KEY: 2:1 waittime=5 transactionname=user_transaction lockMode=X spid=10 kpid=1 trancount=1 currentdbname=tempdb lockTimeout=10
2026-01-01 00:00:00.00 spid1s        executionStack
2026-01-01 00:00:00.00 spid1s        inputbuf
2026-01-01 00:00:00.00 spid1s    SELECT 1 FROM tempdb
2026-01-01 00:00:00.00 spid1s      resource-list
"""
        procs = parse_raw(raw, "2026-01-01T00:00:00.000+0800", "HOST")
        assert procs == []

    def test_no_sql_without_transactionname_excluded(self):
        """A process with no SQL and no UpdateQPStats label is excluded (no useful info)."""
        raw = """\
2026-01-01 00:00:00.00 spid1s   deadlock-list
2026-01-01 00:00:00.00 spid1s    deadlock victim=proc1
2026-01-01 00:00:00.00 spid1s     process-list
2026-01-01 00:00:00.00 spid1s      process id=proc1 logused=0 waitresource=KEY: 2:1 waittime=5 transactionname=unknown_txn lockMode=X spid=10 kpid=1 trancount=1 currentdbname=mydb lockTimeout=10
2026-01-01 00:00:00.00 spid1s        executionStack
2026-01-01 00:00:00.00 spid1s        inputbuf
2026-01-01 00:00:00.00 spid1s      resource-list
"""
        procs = parse_raw(raw, "2026-01-01T00:00:00.000+0800", "HOST")
        assert procs == []

    def test_different_events_produce_different_deadlock_ids(self):
        t1, t2 = "2026-02-01T10:00:00.000+0800", "2026-02-01T10:00:01.000+0800"
        id1 = parse_raw(_RAW_STORED_PROC, t1, "HOST")[0].deadlock_id
        id2 = parse_raw(_RAW_STORED_PROC, t2, "HOST")[0].deadlock_id
        assert id1 != id2

    def test_sql_normalised_no_extra_whitespace(self):
        """Multi-line SQL extracted from executionStack must be a single clean string."""
        procs = parse_raw(_RAW_STORED_PROC, "2026-02-28T17:57:48.240+0800", "WGCSRV32")
        for p in procs:
            assert "\n" not in p.sql_text
            assert "\t" not in p.sql_text
            assert "  " not in p.sql_text   # no double-spaces

    def test_param_placeholders_preserved(self):
        """@P0, @P1 style parameter placeholders must survive extraction."""
        procs = parse_raw(_RAW_ADHOC, "2026-02-22T04:01:25.640+0800", "WINFODB05HV12")
        assert any("@P" in p.inputbuf_sql for p in procs)
