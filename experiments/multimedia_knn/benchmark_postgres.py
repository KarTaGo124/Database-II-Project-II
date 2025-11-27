import sys
import psycopg2
import time
import csv
import json
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import List, Dict

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from indexes.multimedia_index.multimedia_base import MultimediaIndexBase

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'postgres'
}

DATA_DIR = PROJECT_ROOT / "data" / "datasets" / "styles"
IMAGES_DIR = PROJECT_ROOT / "data" / "images"
DATASET_SIZES = [1000, 2000, 4000, 8000, 16000, 32000, 44446]

TEST_QUERIES = [
    "10080.jpg",
    "15970.jpg",
    "28055.jpg",
    "39403.jpg",
    "47016.jpg",
]

K = 8


def connect_to_postgres():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print(f"[OK] Connected to PostgreSQL at {DB_CONFIG['host']}:{DB_CONFIG['port']}")
        return conn
    except Exception as e:
        print(f"[ERROR] Failed to connect to PostgreSQL: {e}")
        print("\nPlease ensure:")
        print("  1. PostgreSQL is running")
        print("  2. pgvector extension is installed: CREATE EXTENSION vector;")
        print("  3. Update DB_CONFIG with your credentials")
        exit(1)


def create_table(conn, vector_dim: int):
    cursor = conn.cursor()

    cursor.execute("CREATE EXTENSION IF NOT EXISTS vector;")

    cursor.execute("DROP TABLE IF EXISTS Styles CASCADE;")

    cursor.execute(f"""
        CREATE TABLE Styles (
            id INTEGER PRIMARY KEY,
            gender VARCHAR(20),
            master_category VARCHAR(50),
            sub_category VARCHAR(50),
            article_type VARCHAR(50),
            base_colour VARCHAR(50),
            season VARCHAR(20),
            year INTEGER,
            usage VARCHAR(20),
            product_display_name VARCHAR(200),
            feature_vector vector({vector_dim})
        );
    """)

    conn.commit()
    print(f"[OK] Created table 'Styles' with vector dimension {vector_dim}")


def extract_features_for_dataset(csv_file: str, n_clusters: int = 300):
    temp_base = MultimediaIndexBase(
        index_dir=str(PROJECT_ROOT / "temp_features"),
        files_dir=str(IMAGES_DIR),
        field_name="id",
        feature_type="SIFT",
        n_clusters=n_clusters,
        filename_pattern="{id}.jpg"
    )

    print(f"[OK] Extracting features for {csv_file}")

    image_ids = []
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            image_ids.append(int(row['id']))

    image_filenames = [f"{img_id}.jpg" for img_id in image_ids]

    if temp_base.codebook is None:
        print("Building codebook...")
        temp_base.build_codebook(filenames=image_filenames, n_workers=4, batch_size=100)

    features_dict = {}
    print(f"Extracting histograms for {len(image_filenames)} images...")
    for img_id, img_file in zip(image_ids, image_filenames):
        hist = temp_base.build_histogram(img_file, normalize=True)
        if hist is not None:
            features_dict[img_id] = hist

    return features_dict, temp_base.n_clusters


def load_data_with_vectors(conn, csv_file: str, features_dict: Dict[int, np.ndarray]):
    cursor = conn.cursor()

    cursor.execute("DELETE FROM Styles;")

    print(f"[OK] Loading data from {csv_file}")

    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            img_id = int(row['id'])
            if img_id in features_dict:
                vector = features_dict[img_id].tolist()
                rows.append((
                    img_id,
                    row['gender'],
                    row['masterCategory'],
                    row['subCategory'],
                    row['articleType'],
                    row['baseColour'],
                    row['season'],
                    int(row['year']) if row['year'] else None,
                    row['usage'],
                    row['productDisplayName'],
                    vector
                ))

        cursor.executemany("""
            INSERT INTO Styles (id, gender, master_category, sub_category, article_type,
                              base_colour, season, year, usage, product_display_name, feature_vector)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, rows)

    conn.commit()
    print(f"[OK] Loaded {len(rows)} rows with feature vectors")


def create_ivfflat_index(conn, n_lists: int = 100):
    cursor = conn.cursor()

    cursor.execute("DROP INDEX IF EXISTS idx_feature_vector;")

    start_time = time.time()
    cursor.execute(f"""
        CREATE INDEX idx_feature_vector
        ON Styles
        USING ivfflat (feature_vector vector_cosine_ops)
        WITH (lists = {n_lists});
    """)
    conn.commit()

    index_time = (time.time() - start_time) * 1000
    print(f"[OK] Created IVFFlat index with {n_lists} lists (took {index_time:.2f} ms)")
    return index_time


def run_knn_search(conn, query_vector: np.ndarray, k: int = K):
    cursor = conn.cursor()

    query_list = query_vector.tolist()

    sql = """
        SELECT id, 1 - (feature_vector <=> %s::vector) as similarity
        FROM Styles
        ORDER BY feature_vector <=> %s::vector
        LIMIT %s;
    """

    cursor.execute(sql, (query_list, query_list, k))
    cursor.fetchall()

    explain_sql = """
        EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        SELECT id, 1 - (feature_vector <=> %s::vector) as similarity
        FROM Styles
        ORDER BY feature_vector <=> %s::vector
        LIMIT %s;
    """

    start_time = time.time()
    cursor.execute(explain_sql, (query_list, query_list, k))
    explain_result = cursor.fetchone()[0][0]
    total_time_ms = (time.time() - start_time) * 1000

    execution_time = explain_result.get('Execution Time', total_time_ms)

    cursor.execute(sql, (query_list, query_list, k))
    results = cursor.fetchall()

    return execution_time, results


def benchmark_dataset(conn, n_rows: int, features_dict: Dict[int, np.ndarray], vector_dim: int) -> Dict:
    print(f"\n{'='*60}")
    print(f"Benchmarking N = {n_rows}")
    print(f"{'='*60}")

    csv_file = DATA_DIR / f"styles_{n_rows}.csv"

    if not csv_file.exists():
        print(f"[ERROR] File not found: {csv_file}")
        return None

    create_table(conn, vector_dim)

    start_load = time.time()
    load_data_with_vectors(conn, csv_file, features_dict)
    load_time = (time.time() - start_load) * 1000

    n_lists = min(300, max(30, n_rows // 100))
    index_time = create_ivfflat_index(conn, n_lists)

    query_results = []
    for query_img in TEST_QUERIES:
        query_id = int(query_img.replace('.jpg', ''))
        if query_id not in features_dict:
            continue

        query_vector = features_dict[query_id]
        search_time, results = run_knn_search(conn, query_vector, k=K)

        query_results.append({
            'query': query_img,
            'k': K,
            'search_time_ms': search_time,
            'num_results': len(results)
        })
        print(f"  Query: '{query_img}' (k={K}) -> {search_time:.3f} ms, Results: {len(results)}")

    avg_search_time = sum(q['search_time_ms'] for q in query_results) / len(query_results) if query_results else 0
    print(f"  Average search time: {avg_search_time:.3f} ms")

    return {
        'n_documents': n_rows,
        'data_load_time_ms': load_time,
        'index_creation_time_ms': index_time,
        'avg_search_time_ms': avg_search_time,
        'queries': query_results
    }


def save_results_to_files(results: List[Dict], output_dir: Path):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    json_file = output_dir / f"postgres_results_{timestamp}.json"
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump({
            'timestamp': timestamp,
            'system': 'PostgreSQL',
            'extension': 'pgvector',
            'index_type': 'IVFFlat',
            'feature_type': 'SIFT',
            'k': K,
            'results': results
        }, f, indent=2)
    print(f"\n[OK] Detailed results saved to: {json_file.name}")


def main():
    print("=" * 60)
    print("PostgreSQL + pgVector KNN Benchmark")
    print("=" * 60)
    print(f"Using datasets from: {DATA_DIR}")
    print(f"Using images from: {IMAGES_DIR}")
    print(f"Connection: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    print(f"K = {K}")
    print()

    conn = connect_to_postgres()

    print("\n[Step 1] Extracting features for all images...")
    max_csv = DATA_DIR / f"styles_{max(DATASET_SIZES)}.csv"
    features_dict, vector_dim = extract_features_for_dataset(max_csv)
    print(f"[OK] Extracted {len(features_dict)} feature vectors (dimension: {vector_dim})")

    results = []
    for n in DATASET_SIZES:
        result = benchmark_dataset(conn, n, features_dict, vector_dim)
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
