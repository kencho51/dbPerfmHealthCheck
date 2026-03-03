# HKJC Database Performance Analysis - January 2026

**Analysis Period:** January 1-31, 2026  
**Data Sources:** 15 CSV performance monitoring files from Splunk  
**Analysis Date:** February 26, 2026  
**Analysis Scope:** SQL Server + MongoDB in production and SAT environments  

---

## �📊 Monitoring Data Sources

| CSV File | Line Count | Analysis Focus | Critical Findings |
|----------|------------|----------------|-------------------|
| maxElapsedQueriesProdJan26.csv | 20,288 | Long-running SQL queries | 1,100.59s bulk operations |
| maxElapsedQueriesSatJan26.csv | 5,627 | SAT environment queries | 2,480.08s DELETE operations |
| mongodbSlowQueriesProdJan26.csv | 2,156 | Production MongoDB | 25.15s collection scans |
| mongodbSlowQueriesSatJan26.csv | 10,002 | SAT MongoDB operations | 65.8s with 2.4M docs |
| blockersProdJan26.csv | 52 | Production blocking events | CREATE PROCEDURE blocking |
| blockersSatJan26.csv | 20 | SAT blocking events | UPDATE operations blocking |
| deadlocksProdJan26.csv | 67 | Production deadlocks | wagering.combination conflicts |
| deadlocksSatJan26.csv | 264 | SAT deadlocks | **47,807 K2_Custom deadlocks** |
| dataFileSizeProdJan26.csv | N/A | Storage utilization | 95-99% capacity utilization |
| dataFileSizeSatJan26.csv | N/A | SAT storage patterns | High utilization patterns |
| mongodbDataFilleSizeProdJan26.csv | 33 | MongoDB storage trends | 6-7GB stable utilization |
| mongodbDataFileSizeAggregatedSatJan26.csv | N/A | SAT MongoDB storage | 4GB utilization patterns |

## 🚨 Critical Performance Crisis Overview

**Status: DISASTER LEVEL - Immediate Executive Attention Required**

Line-by-line analysis of **12 CSV monitoring files** reveals **catastrophic database failures** across all environments with system-threatening performance degradation.

### 📊 Critical Crisis Summary

| Issue Category | Environment | Max Duration | Critical Line Reference | Business Impact |
|----------------|-------------|---------------|------------------------|-----------------|
| **Archive DELETE Crisis** | SAT | **2,480.08 seconds** | maxElapsedQueriesSatJan26.csv:7 | 41-minute database locks |
| **Bulk Archive Operations** | Production | **1,100.59 seconds** | maxElapsedQueriesProdJan26.csv:2 | 18-minute database blocks |
| **Analytics Aggregation** | Production | **813.85 seconds** | maxElapsedQueriesProdJan26.csv:3-12 | 13-minute business hour outages |
| **MongoDB Collection Scans** | SAT | **65.8 seconds** | mongodbSlowQueriesSatJan26.csv:2 | 2.4M document examinations |
| **Production Audit Scans** | Production | **25.15 seconds** | mongodbSlowQueriesProdJan26.csv:3 | 1.2M document COLLSCAN operations |
| **K2_Custom Deadlock DISASTER** | SAT | **47,807 deadlocks** | deadlocksSatJan26.csv:2 | **SYSTEMIC APPLICATION FAILURE** |
| **Wagering Deadlocks** | Production | **67 deadlock events** | deadlocksProdJan26.csv | Transaction consistency risk |

---

## 🔍 Critical Query Pattern Analysis

### SQL Server Performance Breakdown

#### 🔴 **Pattern 1: SAT Environment DELETE Operation Crisis** 
- **Location**: [maxElapsedQueriesSatJan26.csv:7](maxElapsedQueriesSatJan26.csv)
- **Duration**: **2,480.08 seconds (41.3 minutes)**
- **Database**: application_control
- **query**: "delete wagering_control..willpay_progression from wagering_control..willpay_progression r, wagering_control..meeting m where r.meeting_id = m.meeting_id and m.meeting_status = 6"


#### 🔴 **Pattern 2: Archive Bulk Operation Crisis**
- **Location**: [maxElapsedQueriesProdJan26.csv:2](maxElapsedQueriesProdJan26.csv)
- **Duration**: **1,100.59 seconds (18.3 minutes)**
- **Database**: acp_archive_db
- **Query**: 
```
insert bulk dbo.previous_day_activities([arc_src_system_id] tinyint,[activity_id] bigint,[activity_bizdate] int,[activity_time] datetime2(7),[activity_code] int,[terminal_id] varchar(10) collate Latin1_General_BIN2,[terminal_msn] int,[centre_no] int,[window_no] smallint,[account_no] varchar(16) collate Latin1_General_BIN2,[account_trxn_no] smallint,[source_system_id] tinyint,[source_activity_id] bigint,[dest_system_id] tinyint,[terminal_session_id] bigint,[customer_session_id] bigint,[bet_category] tinyint,[ticket_type] int,[ticket_id] bigint,[child_ticket_id] bigint,[is_prelog] tinyint,[is_timeout] tinyint,[is_rcv] tinyint,[pocket_type] tinyint,[result_code] int,[result_text] varchar(1000) collate Latin1_General_BIN2,[result_binary] varbinary(8000),[error_text] varchar(500) collate Latin1_General_BIN2,[data_grouping_bizdate] int)
```

#### 🔴 **Pattern 3: Analytics Aggregation Crisis**
- **Location**: [maxElapsedQueriesProdJan26.csv:3](maxElapsedQueriesProdJan26.csv)
- **Duration**: **813.72 seconds (13.6 minutes)**
- **Database**: oi_analytics_db
- **Query**: 
```
INSERT INTO @aggregate_daily_event_bet_type_account
                   (sports_id
                   ,event_type
                   ,event_id
                   ,bet_type
                   ,is_inplay
                   ,account_no)
            SELECT b.sports_id
                  ,b.event_type
                  ,p.event_id
                  ,b.bet_type
                  ,b.is_inplay
                  ,b.account_no
            FROM   dbo.ticket_bet_single b WITH(NOLOCK)
                   INNER JOIN dbo.ticket_status s WITH(NOLOCK) ON b.system_id = s.system_id AND b.ticket_id = s.ticket_id AND s.is_cancel = 0 AND s.is_reverse = 0
                   INNER JOIN @settled_lv2_pools p ON b.pool_id = p.pool_id
            WHERE  b.account_no IS NOT NULL
```

### MongoDB Performance Breakdown

#### 🔴 **Pattern 4: SAT MongoDB Audit Log COLLSCAN Crisis**
- **Location**: [mongodbSlowQueriesSatJan26.csv:2](mongodbSlowQueriesSatJan26.csv)
- **Duration**: **65.80 seconds (1.1 minutes)**
- **Database**: ptrm_cpc_db.audit_log
- **Query**: "db.audit_log.deleteMany({\"requestTime\": {\"$lt\": ISODate(\"2025-01-26T20:00:02.264Z\")}})"

#### 🔴 **Pattern 5: Production Audit Log COLLSCAN Operations**
- **Location**: [mongodbSlowQueriesProdJan26.csv:2](mongodbSlowQueriesProdJan26.csv)
- **Duration**: **25.15 seconds**
- **Database**: ptrm_cpc_db.audit_log
- **Query**: "db.audit_log.deleteMany({\"requestTime\": {\"$lt\": ISODate(\"2025-01-01T20:00:29.210Z\")}})"

#### 🔴 **Pattern 6: Report Document Lookup Inefficiency Crisis**
- **Location**: [mongodbSlowQueriesProdJan26.csv:4](mongodbSlowQueriesProdJan26.csv)
- **Duration**: **17.15 seconds**
- **Database**: ptrm_cpc_rpt.reportDocument
- **Query**: "db.reportDocument.findOne({\"task\": ObjectId(\"697a9559274b7618d53582e2\")})"

### Blocking Events Analysis

#### 🟠 **Pattern 9: Production Head Blocker Analysis**
- **Location**: [blockersProdJan26.csv](blockersProdJan26.csv)
- **Total Head Blockers**: **51 blocking sessions** causing **78 blocked sessions** with **5,946,923ms (99 min)** total wait time
- **Critical Impact**: `insert bulk dbo.previous_day_activities` (59 sessions blocked, 5.1M ms wait)
- **Databases Affected**: oi_analytics_db, acp_archive_db, agp1_db, agp2_db, agp3_db, agp4_db

**Top Head Blockers by Session Impact:**

| Head Blocker Query | Blocked Sessions | Wait Time (ms) | Database |
|-------------------|------------------|----------------|----------|
| `insert bulk dbo.previous_day_activities` | 59 | 5,097,893 | acp_archive_db |
| `CREATE PROCEDURE usp_agp_get_edabi_rpt_1522` | 10 | 368,081 | agp3_db |
| `CREATE PROCEDURE usp_get_ba_rpt_1072` | 7 | 382,590 | agp1_db |
| `CREATE PROCEDURE usp_get_ba_rpt_1331` | 2 | 98,359 | agp4_db |

#### 🟡 **Pattern 10: SAT Head Blocker Analysis**  
- **Location**: [blockersSatJan26.csv](blockersSatJan26.csv)
- **Total Head Blockers**: **19 blocking sessions** causing **16 blocked sessions** with **914,388ms (15 min)** total wait time
- **Primary Blocker**: `CREATE PROCEDURE dbo.uspx_log_bet_updater` (16 sessions blocked, 914K ms wait)
- **Databases Affected**: oi_analytics_db, cos_db, sps_db, rs_db

**Critical Head Blocker Operations:**

| Head Blocker Query | Blocked Sessions | Wait Time (ms) | Database |
|-------------------|------------------|----------------|----------|
| `CREATE PROCEDURE dbo.uspx_log_bet_updater` | 16 | 914,388 | oi_analytics_db |
| `update rt1_0 set status=@P0 from revert_transaction` | 0 | 0 | cos_db |
| Settlement/Ticket procedures (various) | 0 | 0 | oi_analytics_db |

### Storage Growth Analysis

#### 📈 **Critical Storage Utilization**
- **oi_analytics_db**: 95.45-99.99% utilization across multiple data files
- **acp_archive_db**: 96.61-99.70% utilization in archive partitions
- **asc_db**: 99.90% utilization in month partition files
- **QFM databases**: 93.32-99.98% utilization indicating storage pressure

#### 📊 **MongoDB Storage Status**
- **Production Clusters**: ptrmmdbhv01/02 showing stable 6-7GB utilization
- **SAT Clusters**: winodpsmdbst01-03 showing 4GB utilization
- **OM Clusters**: Various utilization patterns from 13-86GB across environments

### Deadlock Crisis Analysis

#### ⚠️ **Pattern 7: K2_Custom Deadlock Storm DISASTER (SAT)**
- **Location**: [deadlocksSatJan26.csv:2](deadlocksSatJan26.csv)
- **Duration**: **47,807 deadlocks in 4.4 days**
- **Database**: K2_Custom
- **Query**: "frame procname=K2_Custom.dbo.unlock_user_task line=19 stmtstart=660 stmtend=1224" (Stored procedure causing massive resource contention)

#### 🔴 **Pattern 8: Wagering Platform Deadlocks (Production)**
- **Location**: [deadlocksProdJan26.csv:2](deadlocksProdJan26.csv)
- **Duration**: **67 deadlock events**
- **Database**: fb_db_v2
- **Query**: "select c1_0.combination_id,c1_0.line_id,c1_0.pool_id,c1_0.combination_str,c1_0.created_dt..." vs "update wagering.combination set combination_str=@P0,created_dt=@P1..." (Concurrent SELECT/UPDATE conflicts)


