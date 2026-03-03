"""
Database Performance Patterns - Query Extraction Tool
Refactored to allow user input for either a single CSV file or a directory of CSV files.
All rows will be appended into the master patterns CSV table with new column structure.

Master CSV columns: time, source, host, db_name, environment, type, query_details

Directory structure:
  dbPerfmHealthCheck/
  ├── data/           <- input CSV files organised by month (e.g. data/Jan2026/)
  ├── output/         <- master CSV is always written here
  ├── reports/
  └── scripts/        <- this script

Author: kencho
Date: February 24, 2026
"""
import pandas as pd
import json
import re
import argparse
import sys
from typing import List, Dict, Optional
from pathlib import Path
from datetime import datetime

# Project root is the parent of the directory that contains this script (scripts/../)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"


class QueryExtractor:
    """Extracts and processes database queries from various CSV file types."""
    
    def __init__(self, output_dir: Path = OUTPUT_DIR):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.master_data: List[Dict[str, str]] = []
        
    def extract_environment(self, filename: str) -> str:
        """Extract environment (prod or sat) from filename."""
        filename_lower = filename.lower()
        if 'prod' in filename_lower:
            return 'prod'
        elif 'sat' in filename_lower:
            return 'sat'
        return 'unknown'
    
    def extract_query_type(self, filename: str) -> str:
        """Extract query type from filename."""
        filename_lower = filename.lower()
        if 'maxelapsed' in filename_lower or 'slow' in filename_lower:
            return 'slow_query'
        elif 'blocker' in filename_lower:
            return 'blocker'
        elif 'deadlock' in filename_lower:
            return 'deadlock'
        return 'unknown'
    
    def clean_query_text(self, query: str) -> str:
        """Clean and normalize query text."""
        if not query or pd.isna(query):
            return ""
        
        # Convert to string and strip
        query_str = str(query).strip()
        
        # Replace @P0, @P1, etc. with @P? for better normalization
        query_str = re.sub(r'@P\d+', '@P?', query_str)
        
        # Normalize whitespace
        query_str = re.sub(r'\s+', ' ', query_str)
            
        return query_str
    
    def extract_mongodb_command(self, raw_json: str) -> str:
        """Extract MongoDB command from the _raw JSON field."""
        try:
            if pd.isna(raw_json) or not raw_json:
                return ""
                
            # Parse the JSON
            data = json.loads(str(raw_json))
            
            # Extract the command from attr.command
            if 'attr' in data and 'command' in data['attr']:
                command = data['attr']['command']
                return json.dumps(command, separators=(',', ':'))
            
            # If no command found, try to extract operation type
            if 'attr' in data and 'type' in data['attr']:
                return f'{{"type": "{data["attr"]["type"]}"}}'
                
        except (json.JSONDecodeError, KeyError, TypeError):
            # Return a simplified version if JSON parsing fails
            return f'{{"error":"parse_failed","raw_prefix":"{str(raw_json)[:100]}"}}'
            
        return ""
    
    def process_sql_slow_queries(self, file_path: Path) -> List[Dict[str, str]]:
        """Process maxElapsedQueries*.csv files."""
        results = []
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
            print(f"Processing {file_path.name}: {len(df)} rows")
            
            environment = self.extract_environment(file_path.name)
            
            for _, row in df.iterrows():
                # Extract time from creation_time or last_execution_time
                time_value = ""
                if 'creation_time' in row and not pd.isna(row['creation_time']):
                    time_value = str(row['creation_time'])
                elif 'last_execution_time' in row and not pd.isna(row['last_execution_time']):
                    time_value = str(row['last_execution_time'])
                
                # Extract other fields
                host = str(row.get('host', '')) if not pd.isna(row.get('host', '')) else ''
                db_name = str(row.get('db_name', '')) if not pd.isna(row.get('db_name', '')) else ''
                query_text = str(row.get('query_final', '')) if not pd.isna(row.get('query_final', '')) else ''
                
                results.append({
                    'time': time_value,
                    'source': 'sql',
                    'host': host,
                    'db_name': db_name,
                    'environment': environment,
                    'type': 'slow_query',
                    'query_details': self.clean_query_text(query_text)
                })
                
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            
        return results
    
    def process_sql_blockers(self, file_path: Path) -> List[Dict[str, str]]:
        """Process blockers*.csv files."""
        results = []
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
            print(f"Processing {file_path.name}: {len(df)} rows")
            
            environment = self.extract_environment(file_path.name)
            
            for _, row in df.iterrows():
                # Extract time from _time
                time_value = str(row.get('_time', '')) if not pd.isna(row.get('_time', '')) else ''
                
                # Extract other fields
                host = str(row.get('host', '')) if not pd.isna(row.get('host', '')) else ''
                db_name = str(row.get('database_name', '')) if not pd.isna(row.get('database_name', '')) else ''
                query_text = str(row.get('query_text', '')) if not pd.isna(row.get('query_text', '')) else ''
                
                results.append({
                    'time': time_value,
                    'source': 'sql',
                    'host': host,
                    'db_name': db_name,
                    'environment': environment,
                    'type': 'blocker',
                    'query_details': self.clean_query_text(query_text)
                })
                
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            
        return results
    
    def process_sql_deadlocks(self, file_path: Path) -> List[Dict[str, str]]:
        """Process deadlocks*.csv files."""
        results = []
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
            print(f"Processing {file_path.name}: {len(df)} rows")
            
            environment = self.extract_environment(file_path.name)
            
            for _, row in df.iterrows():
                # Extract time from earliest or _time column
                time_value = ""
                if 'earliest' in row and not pd.isna(row['earliest']):
                    time_value = str(row['earliest'])
                elif '_time' in row and not pd.isna(row['_time']):
                    time_value = str(row['_time'])
                
                # Extract other fields - deadlock files may have different column names
                host = str(row.get('host', '')) if not pd.isna(row.get('host', '')) else ''
                db_name = str(row.get('currentdbname', row.get('database_name', row.get('db_name', '')))) if not pd.isna(row.get('currentdbname', row.get('database_name', row.get('db_name', '')))) else ''
                
                # Try multiple column names for query text
                query_text = ""
                for col in ['all_query', 'query_text', 'statement', 'sql_text', 'deadlock_graph']:
                    if col in row and not pd.isna(row[col]):
                        query_text = str(row[col])
                        break
                
                results.append({
                    'time': time_value,
                    'source': 'sql',
                    'host': host,
                    'db_name': db_name,
                    'environment': environment,
                    'type': 'deadlock',
                    'query_details': self.clean_query_text(query_text)
                })
                
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            
        return results
    
    def process_mongodb_queries(self, file_path: Path) -> List[Dict[str, str]]:
        """Process mongo*.csv files."""
        results = []
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
            print(f"Processing {file_path.name}: {len(df)} rows")
            
            environment = self.extract_environment(file_path.name)
            
            for _, row in df.iterrows():
                # Extract time from t.$date
                time_value = str(row.get('t.$date', '')) if not pd.isna(row.get('t.$date', '')) else ''
                
                # Extract other fields
                host = str(row.get('host', '')) if not pd.isna(row.get('host', '')) else ''
                
                # Extract database name from attr.ns
                db_name = ""
                ns = row.get('attr.ns', '')
                if not pd.isna(ns) and ns:
                    ns_parts = str(ns).split('.')
                    if len(ns_parts) >= 2:
                        db_name = ns_parts[0]
                
                # Extract command details from _raw JSON
                raw_json = row.get('_raw', '')
                query_details = self.extract_mongodb_command(raw_json) if not pd.isna(raw_json) else ''
                
                results.append({
                    'time': time_value,
                    'source': 'mongodb',
                    'host': host,
                    'db_name': db_name,
                    'environment': environment,
                    'type': 'slow_query',
                    'query_details': query_details
                })
                
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            
        return results
    
    def process_single_csv(self, csv_path: Path) -> None:
        """Process a single CSV file and append to master data."""
        filename = csv_path.name.lower()
        
        print(f"\n=== Processing: {csv_path.name} ===")
        
        # Determine file type and process accordingly
        if 'maxelapsed' in filename:
            new_data = self.process_sql_slow_queries(csv_path)
        elif 'blocker' in filename:
            new_data = self.process_sql_blockers(csv_path)
        elif 'deadlock' in filename:
            new_data = self.process_sql_deadlocks(csv_path)
        elif 'mongodb' in filename and 'slow' in filename:
            new_data = self.process_mongodb_queries(csv_path)
        else:
            print(f"Unknown file type: {filename}, skipping...")
            return
        
        # Append to master data
        self.master_data.extend(new_data)
        print(f"Added {len(new_data)} records")
    
    def process_directory(self, directory_path: Path) -> None:
        """Process all CSV files in a directory."""
        print(f"\n=== Processing Directory: {directory_path} ===")
        
        # Find all CSV files
        csv_files = list(directory_path.glob('*.csv'))
        
        if not csv_files:
            print(f"No CSV files found in {directory_path}")
            return
        
        print(f"Found {len(csv_files)} CSV files:")
        for csv_file in csv_files:
            print(f"  - {csv_file.name}")
        
        # Process each CSV file
        for csv_file in csv_files:
            # Skip data file size files as they don't contain query data
            if 'datafilesize' in csv_file.name.lower():
                print(f"Skipping data file: {csv_file.name}")
                continue
                
            self.process_single_csv(csv_file)
    
    def create_master_csv(self) -> None:
        """Create the master Database_Performance_Patterns.csv file in output/."""
        if not self.master_data:
            print("No data to write to master CSV")
            return
        
        # Convert to DataFrame
        df = pd.DataFrame(self.master_data)
        
        # Sort by time, source, environment, host
        df = df.sort_values(['time', 'source', 'environment', 'host'])
        
        # Always write to the dedicated output directory
        output_path = self.output_dir / "Database_Performance_Patterns.csv"
        df.to_csv(output_path, index=False, encoding='utf-8')
        
        print(f"\n=== Master CSV Creation Complete ===")
        print(f"Output file: {output_path}")
        print(f"Total records: {len(df)}")
        
        # Print summary statistics
        print("\n=== Summary by Source and Environment ===")
        summary = df.groupby(['source', 'environment', 'type']).size().unstack(fill_value=0)
        print(summary)
        
        print("\n=== Summary by Database ===")
        db_summary = df.groupby(['db_name', 'source']).size().unstack(fill_value=0)
        print(db_summary.head(20))  # Show top 20 databases

def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Extract database performance patterns from CSV files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Project layout (auto-detected from script location):
  Project root : {PROJECT_ROOT}
  Data dir     : {DATA_DIR}
  Output dir   : {OUTPUT_DIR}

Examples (run from the project root or anywhere):
  # Process a single CSV file
  uv run scripts/extract_all_queries_refactored.py --file data/Jan2026/maxElapsedQueriesProdJan26.csv

  # Process all CSV files in a month directory
  uv run scripts/extract_all_queries_refactored.py --directory data/Jan2026

  # Process all CSV files in a directory with a custom output location
  uv run scripts/extract_all_queries_refactored.py --directory data/Jan2026 --output output/Jan2026
        """
    )
    
    # Create mutually exclusive group for file or directory input
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        '--file', '-f',
        type=str,
        help='Path to a single CSV file to process (absolute or relative to cwd)'
    )
    input_group.add_argument(
        '--directory', '-d',
        type=str,
        help='Path to a directory containing CSV files (absolute or relative to cwd)'
    )
    parser.add_argument(
        '--output', '-o',
        type=str,
        default=None,
        help=f'Output directory for the master CSV (default: {OUTPUT_DIR})'
    )

    # Parse arguments
    args = parser.parse_args()

    # Resolve input path
    if args.file:
        input_path = Path(args.file)
        if not input_path.is_absolute():
            input_path = Path.cwd() / input_path
    else:
        input_path = Path(args.directory)
        if not input_path.is_absolute():
            input_path = Path.cwd() / input_path

    # Resolve output directory
    output_dir = Path(args.output).resolve() if args.output else OUTPUT_DIR

    # Validate input path exists
    if not input_path.exists():
        print(f"Error: Path does not exist: {input_path}")
        sys.exit(1)

    # Create extractor (output dir is created automatically)
    extractor = QueryExtractor(output_dir)
    
    try:
        # Process input
        if args.file:
            if not input_path.is_file() or not input_path.name.endswith('.csv'):
                print(f"Error: Must specify a CSV file: {input_path}")
                sys.exit(1)
            extractor.process_single_csv(input_path)
        else:
            if not input_path.is_dir():
                print(f"Error: Must specify a directory: {input_path}")
                sys.exit(1)
            extractor.process_directory(input_path)
        
        # Create master CSV
        extractor.create_master_csv()
        
        print("\nQuery extraction complete!")
        
    except Exception as e:
        print(f"Error during processing: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()