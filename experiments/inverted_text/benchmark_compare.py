"""
Comparative Benchmark Script
=============================
Runs both MyIndex and PostgreSQL benchmarks and generates comparison table.

Usage:
    python benchmark_compare.py

Note: PostgreSQL must be running for this script to work.
"""

import subprocess
import sys
import json
import csv
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent


def run_benchmark(script_name: str):
    """Run a benchmark script and capture output"""
    script_path = PROJECT_ROOT / script_name

    print(f"\n{'='*70}")
    print(f"Running {script_name}...")
    print(f"{'='*70}\n")

    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=False,
            text=True,
            check=True
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n[ERROR] Error running {script_name}")
        return False
    except KeyboardInterrupt:
        print(f"\n\n[ERROR] {script_name} interrupted by user")
        return False


def generate_comparison_csv():
    """Generate comparison CSV from both result files"""
    # Find latest results
    myindex_files = list(PROJECT_ROOT.glob("myindex_results_*.json"))
    postgres_files = list(PROJECT_ROOT.glob("postgres_results_*.json"))

    if not myindex_files or not postgres_files:
        print("\n[ERROR] Could not find result files for comparison")
        return

    myindex_file = max(myindex_files, key=lambda p: p.stat().st_mtime)
    postgres_file = max(postgres_files, key=lambda p: p.stat().st_mtime)

    print(f"\n[OK] Reading {myindex_file.name}")
    print(f"[OK] Reading {postgres_file.name}")

    # Load data
    with open(myindex_file, 'r') as f:
        myindex_data = json.load(f)
    with open(postgres_file, 'r') as f:
        postgres_data = json.load(f)

    # Create comparison CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_file = PROJECT_ROOT / f"comparison_{timestamp}.csv"

    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['N', 'MyIndex_Total_ms', 'MyIndex_Inverted_ms', 'MyIndex_Lookup_ms', 'PostgreSQL_Total_ms', 'PostgreSQL_Index_ms', 'PostgreSQL_Heap_ms', 'PG_Speedup_vs_Total', 'Index_Comparison'])

        myindex_results = {r['n_documents']: r for r in myindex_data['results']}
        postgres_results = {r['n_documents']: r for r in postgres_data['results']}

        for n in sorted(myindex_results.keys()):
            if n not in postgres_results:
                continue

            my_total = myindex_results[n].get('avg_total_search_time_ms', myindex_results[n].get('avg_search_time_ms', 0))
            my_inverted = myindex_results[n].get('avg_inverted_search_time_ms', 0)
            my_lookup = myindex_results[n].get('avg_primary_lookup_time_ms', 0)

            pg_total = postgres_results[n].get('avg_total_search_time_ms', postgres_results[n].get('avg_search_time_ms', 0))
            pg_index = postgres_results[n].get('avg_index_scan_time_ms', 0)
            pg_heap = postgres_results[n].get('avg_heap_scan_time_ms', 0)

            speedup_total = my_total / pg_total if pg_total > 0 else 0
            index_comparison = my_inverted / pg_index if pg_index > 0 else 0

            writer.writerow([n, f"{my_total:.3f}", f"{my_inverted:.3f}", f"{my_lookup:.3f}", f"{pg_total:.3f}", f"{pg_index:.3f}", f"{pg_heap:.3f}", f"{speedup_total:.2f}", f"{index_comparison:.2f}"])

    print(f"\n[OK] Comparison CSV saved to: {csv_file.name}")

    # Print comparison table
    print("\n" + "="*160)
    print("COMPARACION DE TIEMPOS DE BUSQUEDA")
    print("="*160)
    print(f"{'N':<8} {'MyIdx Total':<13} {'MyIdx Inv':<12} {'MyIdx Lookup':<14} {'PG Total':<11} {'PG Index':<11} {'PG Heap':<11} {'Total Speedup':<15} {'Index Comp':<12}")
    print("-"*160)

    for n in sorted(myindex_results.keys()):
        if n not in postgres_results:
            continue

        my_total = myindex_results[n].get('avg_total_search_time_ms', myindex_results[n].get('avg_search_time_ms', 0))
        my_inverted = myindex_results[n].get('avg_inverted_search_time_ms', 0)
        my_lookup = myindex_results[n].get('avg_primary_lookup_time_ms', 0)

        pg_total = postgres_results[n].get('avg_total_search_time_ms', postgres_results[n].get('avg_search_time_ms', 0))
        pg_index = postgres_results[n].get('avg_index_scan_time_ms', 0)
        pg_heap = postgres_results[n].get('avg_heap_scan_time_ms', 0)

        speedup_total = my_total / pg_total if pg_total > 0 else 0
        index_comparison = my_inverted / pg_index if pg_index > 0 else 0

        print(f"{n:<8} {my_total:<13.3f} {my_inverted:<12.3f} {my_lookup:<14.3f} {pg_total:<11.3f} {pg_index:<11.3f} {pg_heap:<11.3f} {speedup_total:<13.2f}x {index_comparison:<10.2f}x")


def main():
    print("=" * 70)
    print("COMPARATIVE BENCHMARK: MyIndex vs PostgreSQL")
    print("=" * 70)
    print("\nThis script will run both benchmarks sequentially.")
    print("Make sure PostgreSQL is running before continuing.")
    print()

    input("Press ENTER to start benchmarking...")

    # Run MyIndex benchmark
    print("\n\n" + "🔷" * 35)
    print("PHASE 1: MyIndex Benchmark")
    print("🔷" * 35)

    myindex_success = run_benchmark("benchmark_myindex.py")

    if not myindex_success:
        print("\n✗ MyIndex benchmark failed. Stopping.")
        return

    # Run PostgreSQL benchmark
    print("\n\n" + "🔶" * 35)
    print("PHASE 2: PostgreSQL Benchmark")
    print("🔶" * 35)

    postgres_success = run_benchmark("benchmark_postgres.py")

    if not postgres_success:
        print("\n✗ PostgreSQL benchmark failed.")
        return

    # Generate comparison CSV
    generate_comparison_csv()

    # Summary
    print("\n\n" + "=" * 70)
    print("BENCHMARK COMPLETED")
    print("=" * 70)
    print("\n[OK] Both benchmarks completed successfully!")
    print("\nCheck the comparison CSV file for detailed results.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nBenchmark interrupted by user")
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
