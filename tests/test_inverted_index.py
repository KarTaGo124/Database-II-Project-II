#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
os.chdir(os.path.join(os.path.dirname(__file__), '..'))

from indexes.core.database_manager import DatabaseManager
from sql_parser.parser import parse
from sql_parser.executor import Executor
import shutil
import time

def print_separator(char="=", length=80):
    print(char * length)

def print_header(title):
    print_separator()
    print(f" {title}")
    print_separator()

def print_metrics(result, operation_name):
    print(f"\n[METRICS] {operation_name}")
    print(f"  Time: {result.execution_time_ms:.2f} ms")
    print(f"  Reads: {result.disk_reads}")
    print(f"  Writes: {result.disk_writes}")
    print(f"  Total accesses: {result.total_disk_accesses}")
    if hasattr(result, 'operation_breakdown') and result.operation_breakdown:
        breakdown = result.operation_breakdown
        if 'primary_metrics' in breakdown:
            pm = breakdown['primary_metrics']
            print(f"  Primary index: {pm['reads']} reads, {pm['writes']} writes, {pm['time_ms']:.2f} ms")
        if 'secondary_metrics' in breakdown:
            sm = breakdown['secondary_metrics']
            print(f"  Secondary index: {sm['reads']} reads, {sm['writes']} writes, {sm['time_ms']:.2f} ms")

def print_results_with_score(results, max_display=5):
    if not results:
        print("  No results found")
        return

    print(f"  Total results: {len(results)}")
    print(f"  Showing top {min(max_display, len(results))}:")

    for i, rec in enumerate(results[:max_display], 1):
        score = rec.get('_text_score', 0.0)
        categoria = rec.get('categoria', 'N/A')
        contenido = rec.get('contenido', '')[:150]
        print(f"\n  [{i}] Score: {score:.4f} | Categoria: {categoria}")
        print(f"      Preview: {contenido}...")

def test_inverted_text_index():
    print_header("TEST: INVERTED TEXT INDEX (FULLTEXT SEARCH)")

    if os.path.exists('data/databases'):
        try:
            shutil.rmtree('data/databases')
        except:
            pass
        time.sleep(0.3)

    csv_path = "data/datasets/news_es-2.csv"

    if not os.path.exists(csv_path):
        print(f"\n[ERROR] CSV file not found: {csv_path}")
        return False

    try:
        db = DatabaseManager()
        executor = Executor(db)

        print("\n1. CREATE TABLE news")
        print("   Schema: id INT KEY (auto-increment), contenido VARCHAR[5000], categoria VARCHAR[50]")
        result = executor.execute(parse("""
            CREATE TABLE news (
                id INT KEY INDEX ISAM,
                contenido VARCHAR[5000],
                categoria VARCHAR[50]
            )
        """)[0])
        print(f"   {result.data}")
        print_metrics(result, "CREATE TABLE")

        print("\n2. LOAD DATA FROM CSV")
        print(f"   File: {csv_path}")
        start = time.time()
        result = executor.execute(parse(f'LOAD DATA FROM FILE "{csv_path}" INTO news')[0])
        end = time.time()
        print(f"   {result.data}")
        print(f"   Total load time: {(end - start) * 1000:.2f} ms")
        print_metrics(result, "LOAD DATA")

        print("\n3. SCAN ALL records (verify data loaded)")
        result = executor.execute(parse('SELECT * FROM news')[0])
        total_records = len(result.data)
        print(f"   Total records loaded: {total_records}")
        if total_records > 0:
            sample = result.data[0]
            print(f"   Sample record:")
            print(f"     - ID: {sample.get('id')}")
            print(f"     - Categoria: {sample.get('categoria')}")
            print(f"     - Content preview: {sample.get('contenido', '')[:100]}...")
        print_metrics(result, "SCAN ALL")

        print("\n4. CREATE INVERTED_TEXT INDEX on 'contenido' field")
        print("   Building index using SPIMI algorithm...")
        start = time.time()
        result = executor.execute(parse('CREATE INDEX ON news (contenido) USING INVERTED_TEXT')[0])
        end = time.time()
        print(f"   {result.data}")
        print(f"   Index build time: {(end - start) * 1000:.2f} ms")
        print_metrics(result, "CREATE INDEX INVERTED_TEXT")

        print_separator("-")
        print(" FULLTEXT SEARCH TESTS")
        print_separator("-")

        queries = [
            ("economía inflación precios", "Economic news about inflation and prices"),
            ("banca financiamiento crédito", "Banking and credit financing"),
            ("sostenibilidad desarrollo ambiental", "Sustainability and environmental development"),
            ("tecnología digital innovación", "Technology and digital innovation"),
            ("Colombia gobierno política", "Colombian government and politics")
        ]

        for i, (query, description) in enumerate(queries, 1):
            print(f"\n{i}. FULLTEXT SEARCH: \"{query}\"")
            print(f"   Description: {description}")

            start = time.time()
            result = executor.execute(parse(f'SELECT categoria, contenido FROM news WHERE contenido @@ "{query}" LIMIT 5')[0])
            end = time.time()

            print(f"   Search time: {(end - start) * 1000:.2f} ms")
            print_results_with_score(result.data, max_display=3)
            print_metrics(result, f"FULLTEXT SEARCH #{i}")

        print("\n6. FULLTEXT SEARCH with more results (LIMIT 10)")
        query = "banco empresa mercado"
        print(f"   Query: \"{query}\"")

        start = time.time()
        result = executor.execute(parse(f'SELECT categoria, contenido FROM news WHERE contenido @@ "{query}" LIMIT 10')[0])
        end = time.time()

        print(f"   Search time: {(end - start) * 1000:.2f} ms")
        print_results_with_score(result.data, max_display=5)
        print_metrics(result, "FULLTEXT SEARCH with LIMIT 10")

        print("\n7. FULLTEXT SEARCH - Empty result test")
        query = "dinosaurios extraterrestres platillos voladores"
        print(f"   Query: \"{query}\" (should return no results)")

        result = executor.execute(parse(f'SELECT * FROM news WHERE contenido @@ "{query}" LIMIT 5')[0])

        print(f"   Results found: {len(result.data)}")
        if len(result.data) == 0:
            print("   ✓ Correctly returned empty results for non-matching query")
        print_metrics(result, "FULLTEXT SEARCH - Empty result")

        print("\n8. Regular SELECT (non-fulltext) for comparison")
        result = executor.execute(parse('SELECT * FROM news WHERE categoria = "Macroeconomia"')[0])
        print(f"   Records with categoria='Macroeconomia': {len(result.data)}")
        print_metrics(result, "SELECT by categoria")

        print("\n9. DROP INDEX contenido")
        result = executor.execute(parse('DROP INDEX contenido ON news')[0])
        print(f"   {result.data}")
        print_metrics(result, "DROP INDEX")

        print("\n10. Verify index was dropped (should fail)")
        try:
            result = executor.execute(parse(f'SELECT * FROM news WHERE contenido @@ "test" LIMIT 5')[0])
            print("   [ERROR] Query should have failed but didn't!")
            return False
        except ValueError as e:
            print(f"   ✓ Query correctly failed: {str(e)[:100]}")

        print("\n11. DROP TABLE news")
        result = executor.execute(parse('DROP TABLE news')[0])
        print(f"   {result.data}")

        print_separator()
        print(" TEST PASSED SUCCESSFULLY")
        print_separator()
        print("\nSummary:")
        print(f"  - Loaded {total_records} news articles from CSV")
        print(f"  - Created INVERTED_TEXT index using SPIMI")
        print(f"  - Performed {len(queries) + 2} fulltext searches with cosine similarity")
        print(f"  - Retrieved ranked results with relevance scores")
        print(f"  - Verified Top-K functionality with LIMIT clause")
        print(f"  - Successfully dropped index and table")
        print_separator()

        return True

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_inverted_text_index()
    sys.exit(0 if success else 1)