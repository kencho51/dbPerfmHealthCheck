# Database Performance Patterns Query Extractor - Refactored

## Overview
The refactored `extract_all_queries_refactored.py` script allows processing either a single CSV file or a directory of CSV files, transforming them into a unified master CSV format with standardized columns.

## Master CSV Structure
The output `Database_Performance_Patterns.csv` has the following columns:
- **time**: Timestamp from original CSV
- **source**: `sql` or `mongodb` 
- **host**: Database server hostname
- **db_name**: Database name
- **environment**: `prod` or `sat` (extracted from filename)
- **type**: `slow_query`, `blocker`, or `deadlock`
- **query_details**: Normalized query text

## Usage

### Process a Single CSV File
```bash
uv run extract_all_queries_refactored.py --file Jan2026/maxElapsedQueriesProdJan26.csv
```

### Process All CSV Files in a Directory
```bash
uv run extract_all_queries_refactored.py --directory Jan2026
```

### Process Current Directory
```bash
uv run extract_all_queries_refactored.py --directory .
```

## Supported CSV File Types

### 1. SQL Slow Queries (`maxElapsed*.csv`)
**Input columns**: `creation_time`, `host`, `db_name`, `query_final`, etc.
**Mapping**:
- time ← creation_time or last_execution_time
- host ← host
- db_name ← db_name
- query_details ← query_final

### 2. MongoDB Slow Queries (`mongodbSlow*.csv`) 
**Input columns**: `t.$date`, `host`, `attr.ns`, `_raw`, etc.
**Mapping**:
- time ← t.$date
- host ← host  
- db_name ← extracted from attr.ns (before first dot)
- query_details ← MongoDB command extracted from _raw JSON

### 3. SQL Blockers (`block*.csv`)
**Input columns**: `_time`, `host`, `database_name`, `query_text`, etc.
**Mapping**:
- time ← _time
- host ← host
- db_name ← database_name
- query_details ← query_text

### 4. SQL Deadlocks (`deadlock*.csv`)
**Input columns**: `earliest`, `currentdbname`, `all_query`, etc.
**Mapping**:
- time ← earliest
- host ← (empty - not provided in this CSV type)
- db_name ← currentdbname  
- query_details ← all_query

## Key Features

### Environment Detection
- Files with "prod" in name → environment = "prod"
- Files with "sat" in name → environment = "sat"

### Query Normalization
- SQL parameters @P0, @P1, etc. replaced with @P?
- Whitespace normalized
- Long queries truncated to 5000 chars with "... [TRUNCATED]" 

### MongoDB Command Extraction
- Parses _raw JSON field to extract command structure
- Handles parsing errors gracefully
- Returns simplified JSON for commands

### Error Handling
- Continues processing other files if one file fails
- Reports processing statistics
- Skips data file size CSVs (not query-related)

## Output

### Master CSV File
Created in the base directory as `Database_Performance_Patterns.csv`

### Summary Statistics
The script prints:
- Summary by source and environment
- Summary by database
- Total records processed
- File processing status

## Example Output
```
=== Processing Directory: Jan2026 ===
Processing maxElapsedQueriesProdJan26.csv: 1,001 rows
Processing mongodbSlowQueriesProdJan26.csv: 2,155 rows
Processing blockersProdJan26.csv: 51 rows
Processing deadlocksProdJan26.csv: 66 rows
...

=== Master CSV Creation Complete ===
Output file: Database_Performance_Patterns.csv
Total records: 3,567

=== Summary by Source and Environment ===
source   environment  type       
mongodb  prod         slow_query    2155
sql      prod         blocker         51
sql      prod         deadlock        66
sql      prod         slow_query    1001
sql      sat          blocker         20
...
```

## Next Steps
After creating the master CSV, you can:
1. Import into analysis tools (Excel, Python pandas, etc.)
2. Create pivot tables by database, environment, type
3. Analyze query patterns and performance trends
4. Generate monthly reports based on standardized format