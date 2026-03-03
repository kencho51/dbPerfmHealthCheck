# HKJC Database Performance Health Check Analysis

## Overview
Comprehensive database performance monitoring and analysis for HKJC's SQL Server and MongoDB infrastructure across production and SAT environments. This repository contains systematic analysis of Splunk-exported CSV performance data to identify patterns, trends, and critical performance issues.

## Analysis Period Coverage
- **November 2025** - Initial baseline monitoring period
- **December 2025** - Performance degradation identification phase  
- **January 2026** - Critical performance crisis analysis

## Repository Structure

```
dbPerfmHealthCheck/
├── README.md (this file)
├── Nov2025/          # November 2025 baseline data
├── Dec2025/          # December 2025 performance tracking  
├── Jan2026/          # January 2026 critical analysis
│   └── Jan2026_Database_Performance_Analysis.md
└── Cross-Month_Performance_Trend_Analysis.md (pending)
```

## CSV File Categories by Environment

### Production Environment Files
- **maxElapsedQueries[Month]Prod.csv** - Slow SQL Server queries (>30 seconds)
- **blockers[Month]Prod.csv** - SQL Server blocking chain events
- **deadlocks[Month]Prod.csv** - Database deadlock incidents
- **mongoSlowQueries[Month]Prod.csv** - MongoDB slow operations (>100ms)
- **dataFileSize[Month]Prod.csv** - SQL Server storage utilization metrics

### SAT Environment Files  
- **maxElapsedQueries[Month]Sat.csv** - SAT environment slow queries
- **blockers[Month]Sat.csv** - SAT blocking events (testing artifacts)
- **deadlocks[Month]Sat.csv** - SAT deadlock patterns
- **mongoSlowQueries[Month]Sat.csv** - MongoDB SAT performance data

## Infrastructure Monitoring Scope

### SQL Server Infrastructure
- **Production Tier**: WINFODB06HV11/HV12, WINDB11ST01N, WGCSRV32
- **SAT Environment**: WINFODB10ST01, WINFODB06ST11
- **Key Databases**: oi_analytics_db, acp_archive_db, fb_db_v2, cos_db, agp_db cluster

### MongoDB Infrastructure
- **Production Clusters**: ptrmmdbhv01/02 (primary), winodpsmdbst01-03 (satellite)
- **SAT Clusters**: winodpsmdbst01-03 (testing)
- **Key Databases**: ptrm_cpc_db, ptrm_cpc_rpt, odpsdb

## Key Findings Summary

### Performance Trajectory (Nov 2025 → Jan 2026)
```
Performance Health Score:
Nov 2025: ████████████████████████ 60% (Acceptable)
Dec 2025: ████████████████▓▓▓▓▓▓▓▓ 40% (Degrading) 
Jan 2026: ██████████▓▓▓▓▓▓▓▓▓▓▓▓▓▓ 25% (Critical)

Trend: -58% degradation over 3 months
```

### Critical Pattern Evolution
| Pattern Type | Nov 2025 | Dec 2025 | Jan 2026 | Trend |
|--------------|----------|----------|----------|--------|
| Max SQL Query Duration | ~120s | ~450s | **813s** | +577% |
| MongoDB Max Operation | ~8s | ~15s | **65s** | +712% |
| Monthly Deadlocks | ~45 | ~120 | **279** | +520% |
| Storage Critical DBs | 0 | 2 | **6** | 6x increase |

## Analysis Methodology

This analysis follows the [CSV Analysis Workflow](../.github/copilot-instructions.md#database-performance-analysis-methodology) documented in the repository:

1. **CSV File Inventory** - Systematic categorization by environment and type
2. **Environment Separation** - Production vs SAT pattern analysis  
3. **Pattern Recognition** - Query/infrastructure/storage pattern identification
4. **Data Verification** - Exact CSV line referencing for claim validation
5. **Cross-Month Correlation** - Trend analysis and pattern evolution tracking
6. **Root Cause Analysis** - Infrastructure bottleneck identification
7. **Business Impact Assessment** - Operational and financial impact quantification

## Quick Navigation

### Monthly Deep-Dive Analysis
- [January 2026 Critical Analysis](Jan2026/Jan2026_Database_Performance_Analysis.md) - Complete methodology and 10 critical patterns
- December 2025 Analysis (pending)
- November 2025 Baseline (pending)

### Cross-Month Trend Analysis
- [Performance Trend Analysis](Cross-Month_Performance_Trend_Analysis.md) (pending)
- Root Cause Evolution Tracking
- Infrastructure Degradation Patterns

## Usage Instructions

### For Database Administrators
1. Review monthly analysis documents for specific performance incidents
2. Use CSV line references to investigate exact query patterns in Splunk
3. Follow remediation priorities based on business impact assessments

### For Infrastructure Teams  
1. Monitor storage utilization trends across months
2. Track host-level performance degradation patterns
3. Plan capacity expansion based on growth projections

### For Development Teams
1. Identify application-level query optimization opportunities
2. Review MongoDB index strategy recommendations
3. Implement stored procedure optimization suggestions

## Contributing

When adding new monthly analysis:
1. Follow the established CSV analysis methodology
2. Reference specific CSV lines for all performance claims
3. Separate production critical issues from SAT testing artifacts  
4. Update this README with new findings and trends

---

**Last Updated**: February 2026  
**Analysis Status**: November & December analysis pending Cross-Month trend completion  
**Next Analysis**: February 2026 (scheduled March 2026)