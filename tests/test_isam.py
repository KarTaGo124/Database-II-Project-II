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

def test_isam():
    print("=" * 70)
    print("TEST: ISAM PRIMARY INDEX")
    print("=" * 70)

    if os.path.exists('data/database'):
        try:
            shutil.rmtree('data/database')
        except:
            pass
        time.sleep(0.5)

    db = DatabaseManager()
    executor = Executor(db)

    print("\n1. CREATE TABLE con ISAM")
    result = executor.execute(parse("""
        CREATE TABLE estudiantes (
            id INT KEY INDEX ISAM,
            nombre VARCHAR[40],
            edad INT,
            promedio FLOAT
        )
    """)[0])
    print(f"   {result.data}")
    print_metrics(result, "CREATE TABLE")

    print("\n2. INSERT 15 registros")
    estudiantes = [
        (101, "Alice Johnson", 20, 3.8),
        (102, "Bob Smith", 22, 3.5),
        (103, "Charlie Brown", 21, 3.9),
        (104, "Diana Prince", 23, 4.0),
        (105, "Eve Davis", 20, 3.7),
        (106, "Frank Miller", 24, 3.6),
        (107, "Grace Lee", 19, 3.85),
        (108, "Henry Wang", 22, 3.75),
        (109, "Iris Chen", 21, 3.95),
        (110, "Jack Wilson", 23, 3.65),
        (111, "Kate Anderson", 20, 3.88),
        (112, "Leo Martinez", 22, 3.72),
        (113, "Maria Garcia", 21, 3.92),
        (114, "Nathan Kim", 24, 3.68),
        (115, "Olivia Taylor", 19, 3.98),
    ]

    total_time = 0
    total_writes = 0
    for eid, nombre, edad, promedio in estudiantes:
        result = executor.execute(parse(f'INSERT INTO estudiantes VALUES ({eid}, "{nombre}", {edad}, {promedio})')[0])
        total_time += result.execution_time_ms
        total_writes += result.disk_writes
    print(f"   Insertados: 15 estudiantes")
    print(f"   Total time: {total_time:.2f} ms")
    print(f"   Total writes: {total_writes}")

    print("\n3. SELECT por PRIMARY KEY (búsqueda en árbol ISAM)")
    result = executor.execute(parse('SELECT * FROM estudiantes WHERE id = 108')[0])
    print(f"   Encontrado: {result.data[0]['nombre']} - Promedio: {result.data[0]['promedio']}")
    print_metrics(result, "SELECT by PRIMARY KEY")

    print("\n4. SELECT BETWEEN (range query en ISAM)")
    result = executor.execute(parse('SELECT * FROM estudiantes WHERE id BETWEEN 105 AND 110')[0])
    print(f"   Encontrados: {len(result.data)} estudiantes")
    for rec in result.data:
        print(f"     - ID {rec['id']}: {rec['nombre']}, edad {rec['edad']}")
    print_metrics(result, "SELECT BETWEEN")

    print("\n5. SELECT * (scan all)")
    result = executor.execute(parse('SELECT * FROM estudiantes')[0])
    print(f"   Total: {len(result.data)} estudiantes")
    print_metrics(result, "SCAN ALL")

    print("\n6. DELETE por PRIMARY KEY")
    result = executor.execute(parse('DELETE FROM estudiantes WHERE id = 105')[0])
    print(f"   Eliminado: {result.data}")
    print_metrics(result, "DELETE by PRIMARY KEY")

    print("\n7. DELETE BETWEEN (múltiples deletes)")
    result = executor.execute(parse('DELETE FROM estudiantes WHERE id BETWEEN 112 AND 114')[0])
    print(f"   Eliminados: {result.data}")
    print_metrics(result, "DELETE BETWEEN")

    print("\n8. Verificar deletes")
    result = executor.execute(parse('SELECT * FROM estudiantes')[0])
    print(f"   Total después de deletes: {len(result.data)} estudiantes")
    print_metrics(result, "SCAN ALL after DELETE")

    print("\n" + "=" * 70)
    print("TEST ISAM PASSED")
    print("=" * 70)

if __name__ == "__main__":
    try:
        test_isam()
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
