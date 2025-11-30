"""
PostgreSQL Benchmark Script
============================
Tests PostgreSQL full-text search with ts_rank and OR operator.
Connects to local PostgreSQL instance, loads data, creates index, and runs benchmark.

Usage:
    python benchmark_postgres.py

Requirements:
    - PostgreSQL running locally
    - psycopg2 installed: pip install psycopg2-binary
"""

import psycopg2
import time
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Dict

# Configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'postgres',  # Change if needed
    'user': 'postgres',       # Change to your username
    'password': 'postgres'    # Change to your password
}

# Data paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "datasets" / "amazon_reviews"

# Dataset sizes to test
DATASET_SIZES = [1000, 2000, 4000, 8000, 16000, 32000, 64000]

# Test queries - diverse set covering different scenarios
TEST_QUERIES = [
    "excellent product quality",      # Multi-word positive review
    "disappointed terrible service",  # Multi-word negative review
    "amazing",                        # Single word high frequency
    "fast shipping great experience", # Longer positive phrase
    "waste money returned",           # Negative experience phrase
]


def connect_to_postgres():
    """Connect to PostgreSQL database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print(f"[OK] Connected to PostgreSQL at {DB_CONFIG['host']}:{DB_CONFIG['port']}")
        return conn
    except Exception as e:
        print(f"[ERROR] Failed to connect to PostgreSQL: {e}")
        print("\nPlease ensure:")
        print("  1. PostgreSQL is running")
        print("  2. Update DB_CONFIG with your credentials")
        exit(1)


def create_table(conn):
    """Create Reviews table if it doesn't exist"""
    cursor = conn.cursor()

    # Drop table if exists
    cursor.execute("DROP TABLE IF EXISTS Reviews CASCADE;")

    # Create table
    cursor.execute("""
        CREATE TABLE Reviews (
            id INTEGER PRIMARY KEY,
            review TEXT
        );
    """)

    conn.commit()
    print("[OK] Created table 'Reviews'")




def create_gin_index(conn):
    """Create GIN index for full-text search"""
    cursor = conn.cursor()

    # Drop index if exists
    cursor.execute("DROP INDEX IF EXISTS idx_review_fts;")

    # Create GIN index
    start_time = time.time()
    cursor.execute("""
        CREATE INDEX idx_review_fts
        ON Reviews
        USING GIN(to_tsvector('english', review));
    """)
    conn.commit()

    index_time = (time.time() - start_time) * 1000
    print(f"[OK] Created GIN index (took {index_time:.2f} ms)")


def run_search(conn, query: str) -> Tuple[float, float, float, List]:
    """Run full-text search with ts_rank and OR operator (returns all results, ordered by relevance)

    Returns:
        Tuple[total_time_ms, index_scan_time_ms, heap_scan_time_ms, results]
    """
    cursor = conn.cursor()

    # Convert query to OR format (same as MyIndex behavior)
    # Use to_tsquery with explicit OR operators
    query_terms = query.split()
    tsquery_str = ' | '.join(query_terms)  # OR operator in PostgreSQL

    sql = """
        SELECT id, ts_rank_cd(to_tsvector('english', review),
                           to_tsquery('english', %s)) as score
        FROM Reviews
        WHERE to_tsvector('english', review) @@ to_tsquery('english', %s)
        ORDER BY score DESC;
    """

    # Warm-up run (to load into cache)
    cursor.execute(sql, (tsquery_str, tsquery_str))
    cursor.fetchall()

    # Run with EXPLAIN ANALYZE to get detailed timing
    explain_sql = f"""
        EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        SELECT id, ts_rank_cd(to_tsvector('english', review),
                           to_tsquery('english', %s)) as score
        FROM Reviews
        WHERE to_tsvector('english', review) @@ to_tsquery('english', %s)
        ORDER BY score DESC;
    """

    cursor.execute(explain_sql, (tsquery_str, tsquery_str))
    explain_result = cursor.fetchone()[0][0]

    # Extract timings from EXPLAIN ANALYZE
    total_time_ms = explain_result['Execution Time']

    # Extract separate timings for index scan and heap scan
    index_scan_time = 0
    heap_scan_time = 0
    plan = explain_result.get('Plan', {})

    def extract_timings(node):
        nonlocal index_scan_time, heap_scan_time
        node_type = node.get('Node Type', '')

        # GIN index scan time
        if 'Bitmap Index Scan' in node_type or 'Index Scan' in node_type:
            index_scan_time += node.get('Actual Total Time', 0)

        # Heap scan time (fetching actual rows from table)
        if 'Bitmap Heap Scan' in node_type or 'Seq Scan' in node_type:
            heap_scan_time += node.get('Actual Total Time', 0)
            # Subtract child times to avoid double counting
            if 'Plans' in node:
                for child in node['Plans']:
                    heap_scan_time -= child.get('Actual Total Time', 0)

        if 'Plans' in node:
            for child in node['Plans']:
                extract_timings(child)

    extract_timings(plan)

    # Now run actual query to get results
    cursor.execute(sql, (tsquery_str, tsquery_str))
    results = cursor.fetchall()

    return total_time_ms, index_scan_time, heap_scan_time, results


def benchmark_dataset(conn, n_rows: int) -> Dict:
    """Run benchmark for a specific dataset size"""
    print(f"\n{'='*60}")
    print(f"Benchmarking N = {n_rows}")
    print(f"{'='*60}")

    # Load data and measure time
    csv_file = DATA_DIR / f"corpus_{n_rows}.csv"

    if not csv_file.exists():
        print(f"[ERROR] File not found: {csv_file}")
        return None

    cursor = conn.cursor()

    # Clear existing data
    cursor.execute("DELETE FROM Reviews;")

    # Load data
    start_load = time.time()
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            rows.append((int(row['doc_id']), row['text']))

        cursor.executemany(
            "INSERT INTO Reviews (id, review) VALUES (%s, %s)",
            rows
        )

    conn.commit()
    load_time = (time.time() - start_load) * 1000
    print(f"[OK] Loaded {n_rows} rows (took {load_time:.2f} ms)")

    # Create index
    start_index = time.time()
    create_gin_index(conn)
    index_time = (time.time() - start_index) * 1000

    # Run queries
    query_results = []
    for query in TEST_QUERIES:
        total_time, index_time_ms, heap_time_ms, results = run_search(conn, query)
        query_results.append({
            'query': query,
            'total_time_ms': total_time,
            'index_scan_time_ms': index_time_ms,
            'heap_scan_time_ms': heap_time_ms,
            'num_results': len(results)
        })
        print(f"  Query: '{query[:30]}...' -> Total: {total_time:.3f} ms (Index: {index_time_ms:.3f} ms, Heap: {heap_time_ms:.3f} ms, Results: {len(results)})")

    avg_total_time = sum(q['total_time_ms'] for q in query_results) / len(query_results)
    avg_index_time = sum(q['index_scan_time_ms'] for q in query_results) / len(query_results)
    avg_heap_time = sum(q['heap_scan_time_ms'] for q in query_results) / len(query_results)

    print(f"\n  Average times:")
    print(f"    Total: {avg_total_time:.3f} ms")
    print(f"    Index Scan: {avg_index_time:.3f} ms")
    print(f"    Heap Scan: {avg_heap_time:.3f} ms")

    return {
        'n_documents': n_rows,
        'data_load_time_ms': load_time,
        'index_creation_time_ms': index_time,
        'avg_total_search_time_ms': avg_total_time,
        'avg_index_scan_time_ms': avg_index_time,
        'avg_heap_scan_time_ms': avg_heap_time,
        'queries': query_results
    }


def save_results_to_files(results: List[Dict], output_dir: Path):
    """Save benchmark results to CSV and JSON files"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Save detailed JSON
    json_file = output_dir / f"postgres_results_{timestamp}.json"
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump({
            'timestamp': timestamp,
            'system': 'PostgreSQL',
            'config': {
                'host': DB_CONFIG['host'],
                'port': DB_CONFIG['port'],
                'database': DB_CONFIG['database']
            },
            'index_type': 'GIN',
            'language': 'english',
            'results': results
        }, f, indent=2)
    print(f"\n[OK] Detailed results saved to: {json_file.name}")


def main():
    print("=" * 60)
    print("PostgreSQL Full-Text Search Benchmark")
    print("=" * 60)
    print(f"Using datasets from: {DATA_DIR}")
    print(f"Connection: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    print()

    # Connect to database
    conn = connect_to_postgres()

    # Create table
    create_table(conn)

    # Run benchmarks
    results = []
    for n in DATASET_SIZES:
        result = benchmark_dataset(conn, n)
        if result is not None:
            results.append(result)

    # Print summary
    print("\n" + "=" * 120)
    print("BENCHMARK RESULTS SUMMARY")
    print("=" * 120)
    print(f"{'N':<10} {'Load (ms)':<15} {'Index (ms)':<15} {'Total Search (ms)':<20} {'Index Scan (ms)':<18} {'Heap Scan (ms)':<15}")
    print("-" * 120)
    for result in results:
        print(f"{result['n_documents']:<10} "
              f"{result['data_load_time_ms']:<15.3f} "
              f"{result['index_creation_time_ms']:<15.3f} "
              f"{result['avg_total_search_time_ms']:<20.3f} "
              f"{result['avg_index_scan_time_ms']:<18.3f} "
              f"{result['avg_heap_scan_time_ms']:<15.3f}")

    # Save results to files
    output_dir = Path(__file__).parent
    save_results_to_files(results, output_dir)

    # Close connection
    conn.close()
    print("\n[OK] Benchmark completed!")
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
