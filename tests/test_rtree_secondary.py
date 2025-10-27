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

def test_rtree_secondary():
    print("=" * 70)
    print("TEST: R-TREE SECONDARY INDEX")
    print("=" * 70)

    if os.path.exists('data/database'):
        try:
            shutil.rmtree('data/database')
        except:
            pass
        time.sleep(0.5)

    db = DatabaseManager()
    executor = Executor(db)

    print("\n1. CREATE TABLE con ISAM primary y campo ARRAY para R-Tree")
    result = executor.execute(parse("""
        CREATE TABLE restaurantes (
            id INT KEY INDEX ISAM,
            nombre VARCHAR[50],
            categoria VARCHAR[30],
            ubicacion ARRAY[FLOAT, 2],
            rating FLOAT
        )
    """)[0])
    print(f"   {result.data}")
    print_metrics(result, "CREATE TABLE")

    print("\n2. CREATE SECONDARY INDEX (RTREE) en ubicacion")
    result = executor.execute(parse('CREATE INDEX ON restaurantes (ubicacion) USING RTREE')[0])
    print(f"   R-Tree index creado en 'ubicacion' (2D)")
    print_metrics(result, "CREATE INDEX")

    print("\n3. INSERT restaurantes con coordenadas (Lima, Perú)")
    restaurantes = [
        (1, "Pizza Hut Centro", "Pizza", [-12.0464, -77.0428], 4.2),
        (2, "KFC San Isidro", "Comida Rápida", [-12.0500, -77.0300], 3.8),
        (3, "McDonalds Miraflores", "Comida Rápida", [-12.1200, -77.0280], 4.0),
        (4, "La Mar Cebichería", "Mariscos", [-12.1180, -77.0290], 4.7),
        (5, "Central Restaurante", "Fine Dining", [-12.1100, -77.0350], 4.9),
        (6, "Astrid y Gastón", "Fine Dining", [-12.1050, -77.0320], 4.8),
        (7, "Isolina Taberna", "Criolla", [-12.1250, -77.0300], 4.6),
        (8, "Maido", "Nikkei", [-12.1080, -77.0340], 4.9),
        (9, "Submarino Amarillo", "Bar", [-12.1150, -77.0310], 4.3),
        (10, "Tanta Larcomar", "Fusión", [-12.1300, -77.0270], 4.1),
    ]

    total_time = 0
    total_writes = 0
    for rest_id, nombre, categoria, ubicacion, rating in restaurantes:
        lat, lng = ubicacion
        result = executor.execute(parse(f'INSERT INTO restaurantes VALUES ({rest_id}, "{nombre}", "{categoria}", ({lat}, {lng}), {rating})')[0])
        total_time += result.execution_time_ms
        total_writes += result.disk_writes
    print(f"   Insertados: {len(restaurantes)} restaurantes")
    print(f"   Total time: {total_time:.2f} ms")
    print(f"   Total writes: {total_writes}")

    print("\n4. RADIUS SEARCH - Restaurantes cerca de (-12.1100, -77.0320) con radio 0.01")
    center_point = (-12.1100, -77.0320)
    radius = 0.01
    result = executor.execute(parse(f'SELECT * FROM restaurantes WHERE ubicacion IN (({center_point[0]}, {center_point[1]}), {radius})')[0])
    print(f"   Encontrados: {len(result.data)} restaurantes en radio {radius}")
    for rec in result.data:
        print(f"     - {rec['nombre']} ({rec['categoria']}) - Rating: {rec['rating']}")
    print_metrics(result, "RADIUS SEARCH")

    print("\n5. KNN SEARCH - 3 restaurantes más cercanos a (-12.1200, -77.0280)")
    query_point = (-12.1200, -77.0280)
    k = 3
    result = executor.execute(parse(f'SELECT * FROM restaurantes WHERE ubicacion NEAREST (({query_point[0]}, {query_point[1]}), {k})')[0])
    print(f"   Encontrados: {len(result.data)} restaurantes más cercanos (k={k})")
    for rec in result.data:
        print(f"     - {rec['nombre']} ({rec['categoria']}) - Rating: {rec['rating']}")
    print_metrics(result, "KNN SEARCH")

    print("\n6. RADIUS SEARCH con radio más grande - Radio 0.02")
    radius_large = 0.02
    result = executor.execute(parse(f'SELECT * FROM restaurantes WHERE ubicacion IN (({center_point[0]}, {center_point[1]}), {radius_large})')[0])
    print(f"   Encontrados: {len(result.data)} restaurantes en radio {radius_large}")
    for rec in result.data:
        print(f"     - {rec['nombre']} ({rec['categoria']}) - Rating: {rec['rating']}")
    print_metrics(result, "RADIUS SEARCH (Large)")

    print("\n7. KNN SEARCH con k=5")
    k_large = 5
    result = executor.execute(parse(f'SELECT * FROM restaurantes WHERE ubicacion NEAREST (({query_point[0]}, {query_point[1]}), {k_large})')[0])
    print(f"   Encontrados: {len(result.data)} restaurantes más cercanos (k={k_large})")
    for rec in result.data:
        print(f"     - {rec['nombre']} ({rec['categoria']}) - Rating: {rec['rating']}")
    print_metrics(result, "KNN SEARCH (k=5)")

    print("\n8. DELETE por primary key")
    result = executor.execute(parse('DELETE FROM restaurantes WHERE id = 1')[0])
    print(f"   Eliminado: Pizza Hut Centro")
    print_metrics(result, "DELETE by PRIMARY KEY")

    print("\n9. Verificar que R-Tree se actualizó - RADIUS SEARCH después del DELETE")
    result = executor.execute(parse(f'SELECT * FROM restaurantes WHERE ubicacion IN (({center_point[0]}, {center_point[1]}), {radius_large})')[0])
    print(f"   Encontrados después del DELETE: {len(result.data)} restaurantes")
    for rec in result.data:
        print(f"     - {rec['nombre']} ({rec['categoria']}) - Rating: {rec['rating']}")
    print_metrics(result, "RADIUS SEARCH after DELETE")

    print("\n10. SELECT por campo normal (no espacial)")
    result = executor.execute(parse('SELECT * FROM restaurantes WHERE categoria = "Fine Dining"')[0])
    print(f"   Encontrados: {len(result.data)} restaurantes Fine Dining")
    for rec in result.data:
        print(f"     - {rec['nombre']} - Rating: {rec['rating']}")
    print_metrics(result, "SELECT by CATEGORIA")

    print("\n" + "=" * 70)
    print("TEST R-TREE COMPLETADO EXITOSAMENTE")
    print("=" * 70)

if __name__ == "__main__":
    try:
        test_rtree_secondary()
        print("\n✅ TODOS LOS TESTS PASARON")
    except Exception as e:
        print(f"\n❌ ERROR EN TEST: {e}")
        import traceback
        traceback.print_exc()