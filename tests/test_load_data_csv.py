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

def print_metrics(result, operation_name):
    print(f"\n[METRICS] {operation_name}")
    print(f"  Time: {result.execution_time_ms:.2f} ms")
    print(f"  Reads: {result.disk_reads}")
    print(f"  Writes: {result.disk_writes}")
    print(f"  Total accesses: {result.total_disk_accesses}")
    if hasattr(result, 'operation_breakdown') and result.operation_breakdown:
        print(f"  Breakdown: {result.operation_breakdown}")

def test_load_data_csv():
    print("=" * 70)
    print("TEST: LOAD DATA FROM CSV FILE")
    print("=" * 70)

    if os.path.exists('data/database'):
        try:
            shutil.rmtree('data/database')
        except:
            pass
        time.sleep(0.5)

    # Crear CSV grande de prueba
    csv_path = "data/test_ventas_large.csv"
    os.makedirs("data", exist_ok=True)

    print("\n1. Generando CSV con 20 registros...")
    with open(csv_path, "w", encoding="utf-8") as f:
        f.write("id,producto,categoria,precio,stock\n")
        for i in range(1, 21):
            categoria = ["Electronica", "Hogar", "Oficina", "Deporte"][i % 4]
            f.write(f"{i},Producto_{i},{categoria},{10.5 + i * 2.3},{50 + i}\n")
    print(f"   CSV creado: {csv_path}")

    try:
        db = DatabaseManager()
        executor = Executor(db)

        print("\n2. CREATE TABLE")
        result = executor.execute(parse("""
            CREATE TABLE ventas (
                id INT KEY INDEX SEQUENTIAL,
                producto VARCHAR[30],
                categoria VARCHAR[20],
                precio FLOAT,
                stock INT
            )
        """)[0])
        print(f"   {result.data}")
        print_metrics(result, "CREATE TABLE")

        print("\n3. CREATE SECONDARY INDEX en categoria")
        result = executor.execute(parse('CREATE INDEX ON ventas (categoria) USING HASH')[0])
        print(f"   Secondary index creado")
        print_metrics(result, "CREATE INDEX")

        print("\n4. LOAD DATA FROM FILE (20 registros)")
        start = time.time()
        result = executor.execute(parse(f'LOAD DATA FROM FILE "{csv_path}" INTO ventas')[0])
        end = time.time()
        print(f"   {result.data}")
        print(f"   Tiempo total de carga: {(end - start) * 1000:.2f} ms")
        print_metrics(result, "LOAD DATA")

        print("\n5. Verificar datos cargados (SCAN ALL)")
        result = executor.execute(parse('SELECT * FROM ventas')[0])
        print(f"   Total registros: {len(result.data)}")
        print(f"   Primeros 3:")
        for rec in result.data[:3]:
            print(f"     - ID {rec['id']}: {rec['producto']}, {rec['categoria']}, ${rec['precio']}, stock={rec['stock']}")
        print_metrics(result, "SCAN ALL")

        print("\n6. SELECT por PRIMARY KEY")
        result = executor.execute(parse('SELECT * FROM ventas WHERE id = 10')[0])
        print(f"   Encontrado: {result.data[0]['producto']} - ${result.data[0]['precio']}")
        print_metrics(result, "SELECT by PRIMARY KEY")

        print("\n7. SELECT BETWEEN")
        result = executor.execute(parse('SELECT * FROM ventas WHERE id BETWEEN 5 AND 10')[0])
        print(f"   Encontrados: {len(result.data)} productos")
        print_metrics(result, "SELECT BETWEEN")

        print("\n8. SELECT por SECONDARY INDEX (categoria)")
        result = executor.execute(parse('SELECT * FROM ventas WHERE categoria = "Electronica"')[0])
        print(f"   Productos en Electronica: {len(result.data)}")
        for rec in result.data[:3]:
            print(f"     - {rec['producto']}: ${rec['precio']}")
        print_metrics(result, "SELECT by SECONDARY INDEX")

        print("\n9. SELECT por otra categoria")
        result = executor.execute(parse('SELECT * FROM ventas WHERE categoria = "Hogar"')[0])
        print(f"   Productos en Hogar: {len(result.data)}")
        print_metrics(result, "SELECT by SECONDARY INDEX")

        print("\n10. DELETE múltiples registros")
        result = executor.execute(parse('DELETE FROM ventas WHERE id BETWEEN 15 AND 18')[0])
        print(f"   Eliminados: {result.data}")
        print_metrics(result, "DELETE BETWEEN")

        print("\n11. Verificar total después de DELETE")
        result = executor.execute(parse('SELECT * FROM ventas')[0])
        print(f"   Total registros restantes: {len(result.data)}")
        print_metrics(result, "SCAN ALL after DELETE")

        print("\n" + "=" * 70)
        print("TEST LOAD DATA CSV PASSED")
        print("=" * 70)

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
    finally:
        if os.path.exists(csv_path):
            os.remove(csv_path)
            print(f"\nCSV de prueba eliminado")

if __name__ == "__main__":
    test_load_data_csv()
