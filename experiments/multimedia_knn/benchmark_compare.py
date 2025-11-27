import subprocess
import sys
import json
import csv
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent


def run_benchmark(script_name: str):
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
    seq_files = list(PROJECT_ROOT.glob("sequential_results_*.json"))
    inv_files = list(PROJECT_ROOT.glob("inverted_results_*.json"))
    pg_files = list(PROJECT_ROOT.glob("postgres_results_*.json"))

    if not seq_files or not inv_files:
        print("\n[ERROR] Could not find result files for comparison")
        return

    seq_file = max(seq_files, key=lambda p: p.stat().st_mtime)
    inv_file = max(inv_files, key=lambda p: p.stat().st_mtime)

    print(f"\n[OK] Reading {seq_file.name}")
    print(f"[OK] Reading {inv_file.name}")

    with open(seq_file, 'r') as f:
        seq_data = json.load(f)
    with open(inv_file, 'r') as f:
        inv_data = json.load(f)

    pg_data = None
    if pg_files:
        pg_file = max(pg_files, key=lambda p: p.stat().st_mtime)
        print(f"[OK] Reading {pg_file.name}")
        with open(pg_file, 'r') as f:
            pg_data = json.load(f)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_file = PROJECT_ROOT / f"comparison_{timestamp}.csv"

    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if pg_data:
            writer.writerow(['N', 'Sequential_Index_ms', 'Inverted_Index_ms', 'PostgreSQL_Index_ms',
                            'Sequential_k8_ms', 'Inverted_k8_ms', 'PostgreSQL_k8_ms',
                            'Speedup_Seq_vs_Inv', 'Speedup_Seq_vs_PG', 'Speedup_Inv_vs_PG'])
        else:
            writer.writerow(['N', 'Sequential_Index_ms', 'Inverted_Index_ms',
                            'Sequential_k8_ms', 'Inverted_k8_ms', 'Speedup_Seq_vs_Inv'])

        seq_results = {r['n_documents']: r for r in seq_data['results']}
        inv_results = {r['n_documents']: r for r in inv_data['results']}
        pg_results = {r['n_documents']: r for r in pg_data['results']} if pg_data else {}

        for n in sorted(seq_results.keys()):
            if n not in inv_results:
                continue

            seq_index = seq_results[n]['index_creation_time_ms']
            inv_index = inv_results[n]['index_creation_time_ms']
            seq_k8 = seq_results[n]['avg_search_time_ms']
            inv_k8 = inv_results[n]['avg_search_time_ms']

            speedup_seq_inv = seq_k8 / inv_k8 if inv_k8 > 0 else 0

            if pg_data and n in pg_results:
                pg_index = pg_results[n]['index_creation_time_ms']
                pg_k8 = pg_results[n]['avg_search_time_ms']
                speedup_seq_pg = seq_k8 / pg_k8 if pg_k8 > 0 else 0
                speedup_inv_pg = inv_k8 / pg_k8 if pg_k8 > 0 else 0

                writer.writerow([n,
                               f"{seq_index:.3f}", f"{inv_index:.3f}", f"{pg_index:.3f}",
                               f"{seq_k8:.3f}", f"{inv_k8:.3f}", f"{pg_k8:.3f}",
                               f"{speedup_seq_inv:.2f}", f"{speedup_seq_pg:.2f}", f"{speedup_inv_pg:.2f}"])
            else:
                writer.writerow([n,
                               f"{seq_index:.3f}", f"{inv_index:.3f}",
                               f"{seq_k8:.3f}", f"{inv_k8:.3f}",
                               f"{speedup_seq_inv:.2f}"])

    print(f"\n[OK] Comparison CSV saved to: {csv_file.name}")

    if pg_data:
        print("\n" + "="*140)
        print("COMPARISON: Sequential vs Inverted vs PostgreSQL (k=8)")
        print("="*140)
        print(f"{'N':<8} {'Seq Index':<12} {'Inv Index':<12} {'PG Index':<12} {'Seq k=8':<11} {'Inv k=8':<11} {'PG k=8':<11} {'Seq/Inv':<10} {'Seq/PG':<10} {'Inv/PG':<10}")
        print("-"*140)

        for n in sorted(seq_results.keys()):
            if n not in inv_results or n not in pg_results:
                continue

            seq_index = seq_results[n]['index_creation_time_ms']
            inv_index = inv_results[n]['index_creation_time_ms']
            pg_index = pg_results[n]['index_creation_time_ms']
            seq_k8 = seq_results[n]['avg_search_time_ms']
            inv_k8 = inv_results[n]['avg_search_time_ms']
            pg_k8 = pg_results[n]['avg_search_time_ms']

            speedup_seq_inv = seq_k8 / inv_k8 if inv_k8 > 0 else 0
            speedup_seq_pg = seq_k8 / pg_k8 if pg_k8 > 0 else 0
            speedup_inv_pg = inv_k8 / pg_k8 if pg_k8 > 0 else 0

            print(f"{n:<8} {seq_index:<12.1f} {inv_index:<12.1f} {pg_index:<12.1f} {seq_k8:<11.3f} {inv_k8:<11.3f} {pg_k8:<11.3f} {speedup_seq_inv:<8.2f}x {speedup_seq_pg:<8.2f}x {speedup_inv_pg:<8.2f}x")
    else:
        print("\n" + "="*100)
        print("COMPARISON: Sequential vs Inverted (k=8)")
        print("="*100)
        print(f"{'N':<8} {'Seq Index':<12} {'Inv Index':<12} {'Seq k=8':<11} {'Inv k=8':<11} {'Speedup':<10}")
        print("-"*100)

        for n in sorted(seq_results.keys()):
            if n not in inv_results:
                continue

            seq_index = seq_results[n]['index_creation_time_ms']
            inv_index = inv_results[n]['index_creation_time_ms']
            seq_k8 = seq_results[n]['avg_search_time_ms']
            inv_k8 = inv_results[n]['avg_search_time_ms']
            speedup = seq_k8 / inv_k8 if inv_k8 > 0 else 0

            print(f"{n:<8} {seq_index:<12.1f} {inv_index:<12.1f} {seq_k8:<11.3f} {inv_k8:<11.3f} {speedup:<8.2f}x")


def main():
    print("=" * 70)
    print("COMPARATIVE BENCHMARK: Sequential vs Inverted vs PostgreSQL")
    print("=" * 70)
    print("\nThis script will run all three benchmarks sequentially.")
    print()

    input("Press ENTER to start benchmarking...")

    print("\n\n" + "🔷" * 35)
    print("PHASE 1: Sequential Index Benchmark")
    print("🔷" * 35)

    seq_success = run_benchmark("benchmark_sequential.py")

    if not seq_success:
        print("\n✗ Sequential benchmark failed. Stopping.")
        return

    print("\n\n" + "🔶" * 35)
    print("PHASE 2: Inverted Index Benchmark")
    print("🔶" * 35)

    inv_success = run_benchmark("benchmark_inverted.py")

    if not inv_success:
        print("\n✗ Inverted benchmark failed.")
        return

    print("\n\n" + "🔸" * 35)
    print("PHASE 3: PostgreSQL + pgVector Benchmark")
    print("🔸" * 35)

    pg_success = run_benchmark("benchmark_postgres.py")

    if not pg_success:
        print("\n⚠ PostgreSQL benchmark failed or skipped.")
        print("Comparison will be generated without PostgreSQL results.")

    generate_comparison_csv()

    print("\n\n" + "=" * 70)
    print("BENCHMARK COMPLETED")
    print("=" * 70)
    print("\n[OK] All benchmarks completed!")
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
