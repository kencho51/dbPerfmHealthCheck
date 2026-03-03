"""
Database Performance Patterns - Query Extraction Tool
Systematically extracts ALL unique queries from CSV files across Nov2025, Dec2025, Jan2026
and creates a comprehensive master Database_Performance_Patterns.csv file.

Author: Database Performance Analysis Team
Date: February 23, 2026
"""

import pandas as pd
import json
import os
import re
import hashlib
from typing import Set, Dict, List, Tuple, Optional
from pathlib import Path

class QueryExtractor:
    """Extracts and processes database queries from various CSV file types."""
    
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.unique_queries: Set[str] = set()
        self.master_data: List[Dict[str, str]] = []
        
        # Months to process
        self.months = ['Nov2025', 'Dec2025', 'Jan2026']
        
    def clean_sql_parameters(self, query: str) -> str:
        """Clean SQL queries by replacing parameter markers with placeholders for better deduplication."""
        if not query or pd.isna(query):
            return ""
            
        # Replace @P0, @P1, etc. with @P? for deduplication
        cleaned = re.sub(r'@P\d+', '@P?', str(query).strip())
        
        # Remove extra whitespace and normalize line endings
        cleaned = re.sub(r'\s+', ' ', cleaned)
        cleaned = cleaned.replace('\n', ' ').replace('\r', ' ')
        
        # Truncate very long queries but preserve essential structure
        if len(cleaned) > 4000:
            cleaned = cleaned[:4000] + "... [TRUNCATED]"
            
        return cleaned
    
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
                # Convert command dict back to JSON string for storage
                return json.dumps(command, separators=(',', ':'))
            
            # If no command found, try to extract operation type
            if 'attr' in data and 'type' in data['attr']:
                op_type = data['attr']['type']
                ns = data.get('attr', {}).get('ns', 'unknown')
                return f"{{\"operation\":\"{op_type}\",\"ns\":\"{ns}\"}}"
                
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            # Return a simplified version if JSON parsing fails
            return f"{{\"error\":\"parse_failed\",\"raw_prefix\":\"{str(raw_json)[:100]}\"}}"
            
        return ""
    
    def get_query_hash(self, query: str) -> str:
        """Generate a hash for query deduplication."""
        return hashlib.md5(query.encode('utf-8')).hexdigest()
    
    def process_sql_slow_queries(self, file_path: Path) -> List[Dict[str, str]]:
        """Process maxElapsedQueries*.csv files."""
        results = []
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
            print(f"Processing {file_path.name}: {len(df)} rows")
            
            for _, row in df.iterrows():
                query = self.clean_sql_parameters(row.get('query_final', ''))
                if query and query.strip():
                    query_hash = self.get_query_hash(query)
                    if query_hash not in self.unique_queries:
                        self.unique_queries.add(query_hash)
                        
                        # Extract environment from filename (Prod/Sat)
                        env = 'prod' if 'Prod' in file_path.name else 'sat'
                        
                        results.append({
                            'database': str(row.get('db_name', 'unknown')),
                            'host': str(row.get('host', 'unknown')),
                            'type': f'sql_slow_query_{env}',
                            'full_query': query,
                            'source_file': file_path.name,
                            'month': self.get_month_from_path(file_path)
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
            
            for _, row in df.iterrows():
                query = self.clean_sql_parameters(row.get('query_text', ''))
                if query and query.strip():
                    query_hash = self.get_query_hash(query)
                    if query_hash not in self.unique_queries:
                        self.unique_queries.add(query_hash)
                        
                        # Extract environment from filename (Prod/Sat)
                        env = 'prod' if 'Prod' in file_path.name else 'sat'
                        
                        results.append({
                            'database': str(row.get('database_name', 'unknown')),
                            'host': str(row.get('host', 'unknown')),
                            'type': f'sql_blocker_{env}',
                            'full_query': query,
                            'source_file': file_path.name,
                            'month': self.get_month_from_path(file_path)
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
            
            for _, row in df.iterrows():
                query = self.clean_sql_parameters(row.get('all_query', ''))
                if query and query.strip():
                    query_hash = self.get_query_hash(query)
                    if query_hash not in self.unique_queries:
                        self.unique_queries.add(query_hash)
                        
                        # Extract environment from filename (Prod/Sat)
                        env = 'prod' if 'Prod' in file_path.name else 'sat'
                        
                        results.append({
                            'database': str(row.get('currentdbname', 'unknown')),
                            'host': 'unknown',  # deadlock files don't have explicit host column
                            'type': f'sql_deadlock_{env}',
                            'full_query': query,
                            'source_file': file_path.name,
                            'month': self.get_month_from_path(file_path)
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
            
            for _, row in df.iterrows():
                raw_json = row.get('_raw', '')
                mongo_command = self.extract_mongodb_command(raw_json)
                
                if mongo_command and mongo_command.strip():
                    query_hash = self.get_query_hash(mongo_command)
                    if query_hash not in self.unique_queries:
                        self.unique_queries.add(query_hash)
                        
                        # Extract database from namespace (database.collection)
                        namespace = str(row.get('attr.ns', 'unknown'))
                        database = namespace.split('.')[0] if '.' in namespace else 'unknown'
                        
                        # Extract environment from filename (Prod/Sat)
                        env = 'prod' if 'Prod' in file_path.name else 'sat'
                        
                        results.append({
                            'database': database,
                            'host': str(row.get('host', 'unknown')),
                            'type': f'mongodb_slow_query_{env}',
                            'full_query': mongo_command,
                            'source_file': file_path.name,
                            'month': self.get_month_from_path(file_path)
                        })
                        
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            
        return results
    
    def get_month_from_path(self, file_path: Path) -> str:
        """Extract month from file path."""
        for month in self.months:
            if month in str(file_path):
                return month
        return 'unknown'
    
    def get_csv_files_for_month(self, month: str) -> Dict[str, List[Path]]:
        """Get all CSV files for a specific month, categorized by type."""
        month_path = self.base_path / month
        
        if not month_path.exists():
            print(f"Warning: Month directory {month_path} does not exist")
            return {}
            
        csv_files = {
            'sql_slow': [],
            'sql_blockers': [],
            'sql_deadlocks': [],
            'mongodb': []
        }
        
        # Find all CSV files in the month directory
        for csv_file in month_path.glob('*.csv'):
            filename = csv_file.name.lower()
            
            if 'maxelapsedqueries' in filename:
                csv_files['sql_slow'].append(csv_file)
            elif 'blocker' in filename and not 'dedup' in filename:
                # Use non-dedup versions as primary, or latest dedup if no original
                csv_files['sql_blockers'].append(csv_file)
            elif 'deadlock' in filename and not ('updated' in filename and not 'updated2' in filename):
                # Use most recent version
                csv_files['sql_deadlocks'].append(csv_file)
            elif 'mongo' in filename and 'slow' in filename:
                csv_files['mongodb'].append(csv_file)
                
        return csv_files
    
    def process_all_months(self) -> None:
        """Process CSV files from all months systematically."""
        print("=== Starting comprehensive query extraction ===")
        print(f"Base path: {self.base_path}")
        print(f"Processing months: {', '.join(self.months)}")
        print()
        
        for month in self.months:
            print(f"--- Processing {month} ---")
            csv_files = self.get_csv_files_for_month(month)
            
            # Process SQL slow queries
            for file_path in csv_files['sql_slow']:
                results = self.process_sql_slow_queries(file_path)
                self.master_data.extend(results)
                print(f"  Added {len(results)} unique slow queries from {file_path.name}")
            
            # Process SQL blockers
            for file_path in csv_files['sql_blockers']:
                results = self.process_sql_blockers(file_path)
                self.master_data.extend(results)
                print(f"  Added {len(results)} unique blockers from {file_path.name}")
            
            # Process SQL deadlocks
            for file_path in csv_files['sql_deadlocks']:
                results = self.process_sql_deadlocks(file_path)
                self.master_data.extend(results)
                print(f"  Added {len(results)} unique deadlocks from {file_path.name}")
            
            # Process MongoDB queries
            for file_path in csv_files['mongodb']:
                results = self.process_mongodb_queries(file_path)
                self.master_data.extend(results)
                print(f"  Added {len(results)} unique MongoDB queries from {file_path.name}")
            
            print()
    
    def create_master_csv(self) -> None:
        """Create the master Database_Performance_Patterns.csv file."""
        if not self.master_data:
            print("No data to write!")
            return
            
        # Convert to DataFrame
        df = pd.DataFrame(self.master_data)
        
        # Remove temporary columns used for processing
        final_columns = ['database', 'host', 'type', 'full_query']
        df_final = df[final_columns]
        
        # Sort by database, then type, then host
        df_final = df_final.sort_values(['database', 'type', 'host'])
        
        # Write to CSV
        output_path = self.base_path / "Database_Performance_Patterns.csv"
        df_final.to_csv(output_path, index=False, encoding='utf-8')
        
        print("=== Master CSV Creation Complete ===")
        print(f"Output file: {output_path}")
        print(f"Total unique queries: {len(self.unique_queries)}")
        print(f"Total records: {len(df_final)}")
        print()
        
        # Print summary statistics
        print("=== Summary by Database ===")
        db_summary = df.groupby(['database', 'type']).size().unstack(fill_value=0)
        print(db_summary)
        print()
        
        print("=== Summary by Environment ===")
        # Extract environment from type
        df['environment'] = df['type'].apply(lambda x: 'prod' if 'prod' in x else 'sat')
        env_summary = df.groupby(['database', 'environment']).size().unstack(fill_value=0)
        print(env_summary)
        print()

def main():
    """Main execution function."""
    base_path = r"c:\Users\kenlcho\Desktop\obsidian\kencho-vault\hkjc\dbPerfmHealthCheck"
    
    extractor = QueryExtractor(base_path)
    
    # Process all months
    extractor.process_all_months()
    
    # Create master CSV
    extractor.create_master_csv()
    
    print("Query extraction complete!")

if __name__ == "__main__":
    main()