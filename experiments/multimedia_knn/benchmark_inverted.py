import sys
import time
import shutil
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from indexes.core.database_manager import DatabaseManager
from sql_parser.parser import parse
from sql_parser.executor import Executor

DATA_DIR = PROJECT_ROOT / "data" / "datasets" / "styles"
IMAGES_DIR = "data/images/"
BENCHMARK_DB_DIR = PROJECT_ROOT / "experiments" / "multimedia_knn" / "benchmark_db_inv"

DATASET_SIZES = [1000, 2000, 4000, 8000, 16000, 32000, 44446]

TEST_QUERIES = [
    "10080.jpg",
    "15970.jpg",
    "28055.jpg",
    "39403.jpg",
    "47016.jpg",
]

K = 8


def clean_benchmark_db():
    if BENCHMARK_DB_DIR.exists():
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
    BENCHMARK_DB_DIR.mkdir(parents=True, exist_ok=True)
    db = DatabaseManager(
        database_name="benchmark_db_inv",
        base_path=str(BENCHMARK_DB_DIR.parent)
    )
    print("[OK] Created database manager")
    return db


def execute_sql(db: DatabaseManager, sql: str):
    executor = Executor(db)
    plans = parse(sql)
    results = []
    for plan in plans:
        result = executor.execute(plan)
        results.append(result)
    return results


def create_multimedia_index(db: DatabaseManager):
    sql_index = f"""
        CREATE INDEX ON Styles USING MULTIMEDIA_INV
        FEATURE "SIFT"
        DIRECTORY "{IMAGES_DIR}"
        PATTERN "{{id}}.jpg";
    """
    start_time = time.time()
    execute_sql(db, sql_index)
    index_time = (time.time() - start_time) * 1000
    print(f"[OK] Created MULTIMEDIA_INV index with SIFT (took {index_time:.2f} ms)")
    return index_time


def run_knn_search(db: DatabaseManager, query_image: str, k: int):
    sql_warmup = f"""
        SELECT * FROM Styles WHERE id <-> "{query_image}" LIMIT {k};
    """
    execute_sql(db, sql_warmup)

    results = execute_sql(db, sql_warmup)

    result_data = []
    total_time_ms = 0

    if results and hasattr(results[0], 'data'):
        result_data = results[0].data
        total_time_ms = results[0].execution_time_ms

    return total_time_ms, result_data


def benchmark_dataset(db: DatabaseManager, n_rows: int) -> Dict:
    print(f"\n{'='*60}")
    print(f"Benchmarking N = {n_rows}")
    print(f"{'='*60}")

    csv_file = DATA_DIR / f"styles_{n_rows}.csv"

    if "Styles" in db.tables:
        db.drop_table("Styles")

    sql_create = """
        CREATE TABLE Styles (
            id INT KEY INDEX SEQUENTIAL,
            gender VARCHAR[20],
            masterCategory VARCHAR[50],
            subCategory VARCHAR[50],
            articleType VARCHAR[50],
            baseColour VARCHAR[50],
            season VARCHAR[20],
            year INT,
            usage VARCHAR[20],
            productDisplayName VARCHAR[200]
        );
    """
    execute_sql(db, sql_create)
    print(f"[OK] Created table 'Styles'")

    sql_load = f"""
        LOAD DATA FROM FILE "{csv_file}" INTO Styles;
    """
    start_load = time.time()
    execute_sql(db, sql_load)
    load_time = (time.time() - start_load) * 1000
    print(f"[OK] Loaded {n_rows} rows (took {load_time:.2f} ms)")

    start_index = time.time()
    index_time = create_multimedia_index(db)

    query_results = []
    for query in TEST_QUERIES:
        search_time, results = run_knn_search(db, query, K)
        query_results.append({
            'query': query,
            'k': K,
            'search_time_ms': search_time,
            'num_results': len(results)
        })
        print(f"  Query: '{query}' (k={K}) -> {search_time:.3f} ms, Results: {len(results)}")

    avg_time = sum(q['search_time_ms'] for q in query_results) / len(query_results)
    print(f"  Average for k={K}: {avg_time:.3f} ms")

    return {
        'n_documents': n_rows,
        'data_load_time_ms': load_time,
        'index_creation_time_ms': index_time,
        'avg_search_time_ms': avg_time,
        'queries': query_results
    }


def save_results_to_files(results: List[Dict], output_dir: Path):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    json_file = output_dir / f"inverted_results_{timestamp}.json"
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump({
            'timestamp': timestamp,
            'system': 'MyIndex',
            'index_type': 'MULTIMEDIA_INV',
            'feature_type': 'SIFT',
            'results': results
        }, f, indent=2)
    print(f"\n[OK] Detailed results saved to: {json_file.name}")


def main():
    print("=" * 60)
    print("MyIndex Multimedia Inverted KNN Benchmark")
    print("=" * 60)
    print(f"Using datasets from: {DATA_DIR}")
    print(f"Using images from: {IMAGES_DIR}")
    print(f"Database path: {BENCHMARK_DB_DIR}")
    print()

    clean_benchmark_db()
    db = create_database()

    results = []
    for n in DATASET_SIZES:
        result = benchmark_dataset(db, n)
        if result is not None:
            results.append(result)

    print("\n" + "=" * 100)
    print("BENCHMARK RESULTS SUMMARY")
    print("=" * 100)
    print(f"{'N':<10} {'Load (ms)':<15} {'Index (ms)':<20} {'Search k=8 (ms)':<20}")
    print("-" * 100)
    for result in results:
        print(f"{result['n_documents']:<10} "
              f"{result['data_load_time_ms']:<15.3f} "
              f"{result['index_creation_time_ms']:<20.3f} "
              f"{result['avg_search_time_ms']:<20.3f}")

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
