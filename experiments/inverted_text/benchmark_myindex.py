"""
MyIndex Benchmark Script
=========================
Tests MyIndex inverted text index with optimizations.
Uses SQL parser to execute queries without frontend.

Usage:
    python benchmark_myindex.py

This script tests the optimized inverted index implementation with:
- Single file open per search (not per term)
- Lazy loading of IDF and doc_norms
- Only vocabulary kept in RAM permanently
"""

import sys
import time
import shutil
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Dict

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from indexes.core.database_manager import DatabaseManager
from indexes.core.record import Table
from sql_parser.parser import parse
from sql_parser.executor import Executor

# Data paths
DATA_DIR = PROJECT_ROOT / "data" / "datasets" / "amazon_reviews"
BENCHMARK_DB_DIR = PROJECT_ROOT / "experiments" / "inverted_text" / "benchmark_db"

# Dataset sizes to test
DATASET_SIZES = [1000, 64000]

# Test queries - diverse set covering different scenarios
TEST_QUERIES = [
    "excellent product quality",      # Multi-word positive review
    "disappointed terrible service",  # Multi-word negative review
    "amazing",                        # Single word high frequency
    "fast shipping great experience", # Longer positive phrase
    "waste money returned",           # Negative experience phrase
]


def clean_benchmark_db():
    """Remove existing benchmark database"""
    if BENCHMARK_DB_DIR.exists():
        import time
        max_retries = 3
        for i in range(max_retries):
            try:
                shutil.rmtree(BENCHMARK_DB_DIR)
                print("[OK] Cleaned previous benchmark database")
                break
            except PermissionError:
                if i < max_retries - 1:
                    print(f"[WARNING] Files in use, retrying in 2 seconds... ({i+1}/{max_retries})")
                    time.sleep(2)
                else:
                    print("[WARNING] Could not remove old database, will create new instance")


def create_database():
    """Create fresh database for benchmarking"""
    BENCHMARK_DB_DIR.mkdir(parents=True, exist_ok=True)
    db = DatabaseManager(
        database_name="benchmark_db",
        base_path=str(BENCHMARK_DB_DIR.parent)
    )
    print("[OK] Created database manager")
    return db


def execute_sql(db: DatabaseManager, sql: str):
    """Execute SQL statement using parser and executor"""
    executor = Executor(db)
    plans = parse(sql)

    results = []
    for plan in plans:
        result = executor.execute(plan)
        results.append(result)

    return results




def create_inverted_index(db: DatabaseManager):
    """Create inverted text index on text field"""
    # IMPORTANT: Use 'english' since Amazon reviews are in English
    sql_index = """
        CREATE INDEX ON Reviews (text) USING INVERTED_TEXT LANGUAGE "english";
    """

    start_time = time.time()
    execute_sql(db, sql_index)
    index_time = (time.time() - start_time) * 1000
    print(f"[OK] Created INVERTED_TEXT index with language=english (took {index_time:.2f} ms)")


def run_search(db: DatabaseManager, query: str) -> Tuple[float, float, float, List]:
    """Run full-text search using MyIndex (returns all results, ordered by relevance)

    Returns:
        Tuple[total_time_ms, inverted_time_ms, primary_lookup_time_ms, result_data]
    """
    # Warm-up run (to load IDF and doc_norms into cache)
    sql_warmup = f"""
        SELECT * FROM Reviews WHERE text @@ "{query}";
    """
    execute_sql(db, sql_warmup)

    # Actual timed run
    results = execute_sql(db, sql_warmup)

    # Extract results and breakdown from OperationResult
    result_data = []
    total_time_ms = 0
    inverted_time_ms = 0
    primary_lookup_time_ms = 0

    if results and hasattr(results[0], 'data'):
        result_data = results[0].data

        # Use the execution_time from OperationResult (already calculated internally)
        total_time_ms = results[0].execution_time_ms

        # Extract breakdown if available
        if hasattr(results[0], 'operation_breakdown') and results[0].operation_breakdown:
            breakdown = results[0].operation_breakdown
            inverted_time_ms = breakdown.get('secondary_metrics', {}).get('time_ms', 0)
            primary_lookup_time_ms = breakdown.get('primary_metrics', {}).get('time_ms', 0)

    return total_time_ms, inverted_time_ms, primary_lookup_time_ms, result_data


def benchmark_dataset(db: DatabaseManager, n_rows: int) -> Dict:
    """Run benchmark for a specific dataset size"""
    print(f"\n{'='*60}")
    print(f"Benchmarking N = {n_rows}")
    print(f"{'='*60}")

    # Create table and load data (measure load time)
    csv_file = DATA_DIR / f"corpus_{n_rows}.csv"

    # Drop table if exists
    if "Reviews" in db.tables:
        db.drop_table("Reviews")

    # Create table
    sql_create = """
        CREATE TABLE Reviews (
            doc_id INT KEY INDEX SEQUENTIAL,
            text VARCHAR[5000]
        );
    """
    execute_sql(db, sql_create)
    print(f"[OK] Created table 'Reviews'")

    # Load data and measure time
    sql_load = f"""
        LOAD DATA FROM FILE "{csv_file}" INTO Reviews;
    """
    start_load = time.time()
    execute_sql(db, sql_load)
    load_time = (time.time() - start_load) * 1000
    print(f"[OK] Loaded {n_rows} rows (took {load_time:.2f} ms)")

    # Create inverted index
    start_index = time.time()
    create_inverted_index(db)
    index_time = (time.time() - start_index) * 1000

    # Run queries
    query_results = []
    for query in TEST_QUERIES:
        total_time, inverted_time, lookup_time, results = run_search(db, query)
        query_results.append({
            'query': query,
            'total_time_ms': total_time,
            'inverted_time_ms': inverted_time,
            'primary_lookup_time_ms': lookup_time,
            'num_results': len(results)
        })
        print(f"  Query: '{query[:30]}...' -> Total: {total_time:.3f} ms (Inverted: {inverted_time:.3f} ms, Lookup: {lookup_time:.3f} ms, Results: {len(results)})")

    avg_total_time = sum(q['total_time_ms'] for q in query_results) / len(query_results)
    avg_inverted_time = sum(q['inverted_time_ms'] for q in query_results) / len(query_results)
    avg_lookup_time = sum(q['primary_lookup_time_ms'] for q in query_results) / len(query_results)

    print(f"\n  Average times:")
    print(f"    Total: {avg_total_time:.3f} ms")
    print(f"    Inverted Index: {avg_inverted_time:.3f} ms")
    print(f"    Primary Lookup: {avg_lookup_time:.3f} ms")

    return {
        'n_documents': n_rows,
        'data_load_time_ms': load_time,
        'index_creation_time_ms': index_time,
        'avg_total_search_time_ms': avg_total_time,
        'avg_inverted_search_time_ms': avg_inverted_time,
        'avg_primary_lookup_time_ms': avg_lookup_time,
        'queries': query_results
    }


def save_results_to_files(results: List[Dict], output_dir: Path):
    """Save benchmark results to CSV and JSON files"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Save detailed JSON
    json_file = output_dir / f"myindex_results_{timestamp}.json"
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump({
            'timestamp': timestamp,
            'system': 'MyIndex',
            'optimizations': [
                'Single file open per search',
                'Lazy loading of IDF and doc_norms',
                'Only vocabulary in RAM'
            ],
            'results': results
        }, f, indent=2)
    print(f"\n[OK] Detailed results saved to: {json_file.name}")


def main():
    print("=" * 60)
    print("MyIndex (Optimized) Full-Text Search Benchmark")
    print("=" * 60)
    print(f"Using datasets from: {DATA_DIR}")
    print(f"Database path: {BENCHMARK_DB_DIR}")
    print()
    print("Optimizations enabled:")
    print("  [OK] Single file open per search (not per term)")
    print("  [OK] Lazy loading of IDF and doc_norms")
    print("  [OK] Only vocabulary kept in RAM permanently")
    print()

    # Clean previous benchmark database
    clean_benchmark_db()

    # Create database
    db = create_database()

    # Run benchmarks
    results = []
    for n in DATASET_SIZES:
        result = benchmark_dataset(db, n)
        if result is not None:
            results.append(result)

    # Print summary
    print("\n" + "=" * 120)
    print("BENCHMARK RESULTS SUMMARY")
    print("=" * 120)
    print(f"{'N':<10} {'Load (ms)':<15} {'Index (ms)':<15} {'Total Search (ms)':<20} {'Inverted (ms)':<18} {'Lookup (ms)':<15}")
    print("-" * 120)
    for result in results:
        print(f"{result['n_documents']:<10} "
              f"{result['data_load_time_ms']:<15.3f} "
              f"{result['index_creation_time_ms']:<15.3f} "
              f"{result['avg_total_search_time_ms']:<20.3f} "
              f"{result['avg_inverted_search_time_ms']:<18.3f} "
              f"{result['avg_primary_lookup_time_ms']:<15.3f}")

    # Save results to files
    output_dir = Path(__file__).parent
    save_results_to_files(results, output_dir)

    print("\n[OK] Benchmark completed!")
    print(f"\nBenchmark database saved at: {BENCHMARK_DB_DIR}")
    print(f"Results saved in: {output_dir}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nBenchmark interrupted by user")
    except Exception as e:
        print(f"\n[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()
