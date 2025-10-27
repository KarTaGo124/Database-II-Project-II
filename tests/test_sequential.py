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

def test_sequential():
    print("=" * 70)
    print("TEST: SEQUENTIAL FILE PRIMARY INDEX")
    print("=" * 70)

    if os.path.exists('data/database'):
        try:
            shutil.rmtree('data/database')
        except:
            pass
        time.sleep(0.5)

    db = DatabaseManager()
    executor = Executor(db)

    print("\n1. CREATE TABLE con SEQUENTIAL")
    result = executor.execute(parse("""
        CREATE TABLE productos (
            id INT KEY INDEX SEQUENTIAL,
            nombre VARCHAR[50],
            stock INT,
            precio FLOAT
        )
    """)[0])
    print(f"   {result.data}")
    print_metrics(result, "CREATE TABLE")

    print("\n2. INSERT 10 registros")
    productos = [
        (1, "Laptop Dell", 15, 850.99),
        (2, "Mouse Logitech", 50, 25.50),
        (3, "Teclado Mecánico", 30, 120.00),
        (4, "Monitor LG 24", 20, 180.00),
        (5, "Webcam HD", 25, 45.99),
        (6, "Headset Gamer", 18, 75.50),
        (7, "SSD 500GB", 40, 65.00),
        (8, "RAM 16GB", 35, 95.00),
        (9, "GPU RTX 3060", 10, 450.00),
        (10, "Cooler RGB", 60, 35.99),
    ]

    total_time = 0
    total_writes = 0
    for pid, nombre, stock, precio in productos:
        result = executor.execute(parse(f'INSERT INTO productos VALUES ({pid}, "{nombre}", {stock}, {precio})')[0])
        total_time += result.execution_time_ms
        total_writes += result.disk_writes
    print(f"   Insertados: 10 productos")
    print(f"   Total time: {total_time:.2f} ms")
    print(f"   Total writes: {total_writes}")

    print("\n3. SELECT por PRIMARY KEY (búsqueda binaria)")
    result = executor.execute(parse('SELECT * FROM productos WHERE id = 5')[0])
    print(f"   Encontrado: {result.data[0]['nombre']} - ${result.data[0]['precio']}")
    print_metrics(result, "SELECT by PRIMARY KEY")

    print("\n4. SELECT BETWEEN (range query)")
    result = executor.execute(parse('SELECT * FROM productos WHERE id BETWEEN 3 AND 7')[0])
    print(f"   Encontrados: {len(result.data)} productos")
    for rec in result.data:
        print(f"     - ID {rec['id']}: {rec['nombre']}")
    print_metrics(result, "SELECT BETWEEN")

    print("\n5. SELECT * (scan all)")
    result = executor.execute(parse('SELECT * FROM productos')[0])
    print(f"   Total: {len(result.data)} productos")
    print_metrics(result, "SCAN ALL")

    print("\n6. DELETE por PRIMARY KEY")
    result = executor.execute(parse('DELETE FROM productos WHERE id = 3')[0])
    print(f"   Eliminado: {result.data}")
    print_metrics(result, "DELETE by PRIMARY KEY")

    print("\n7. Verificar DELETE")
    result = executor.execute(parse('SELECT * FROM productos')[0])
    print(f"   Total después de delete: {len(result.data)} productos")
    print_metrics(result, "SCAN ALL after DELETE")

    print("\n" + "=" * 70)
    print("TEST SEQUENTIAL PASSED")
    print("=" * 70)

if __name__ == "__main__":
    try:
        test_sequential()
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
