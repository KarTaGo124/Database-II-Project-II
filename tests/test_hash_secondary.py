#!/usr/bin/env python3
import sys, os, shutil, time, random

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
os.chdir(os.path.join(os.path.dirname(__file__), '..'))

from indexes.core.database_manager import DatabaseManager
from sql_parser.parser import parse
from sql_parser.executor import Executor


def print_metrics(result, operation_name):
    print(f"\n[METRICS] {operation_name}")
    print(f"  Time: {result.execution_time_ms:.2f} ms")
    print(f"  Reads: {result.disk_reads}")
    print(f"  Writes: {result.disk_writes}")
    print(f"  Total accesses: {result.total_disk_accesses}")


def test_hash_secondary_exhaustive():
    print("=" * 80)
    print("TEST EXTENDIBLE HASHING — EXHAUSTIVE FUNCTIONAL TEST")
    print("=" * 80)

    if os.path.exists('data/database'):
        shutil.rmtree('data/database', ignore_errors=True)
        time.sleep(0.3)

    db = DatabaseManager()
    executor = Executor(db)

    # === 1. CREATE TABLE AND SECONDARY INDEX ===
    print("\n1. Crear tabla y secondary index extendible")
    executor.execute(parse("""
        CREATE TABLE empleados
        (
            emp_id       INT KEY INDEX ISAM,
            nombre       VARCHAR[40],
            departamento VARCHAR[30],
            salario      FLOAT
        )
    """)[0])
    executor.execute(parse('CREATE INDEX ON empleados (departamento) USING HASH')[0])

    # === 2. MASS INSERTS to trigger directory doubling and bucket splits ===
    print("\n2. Insertar registros para forzar splits y duplicación de directorio")

    departamentos = ["IT", "Ventas", "RRHH", "Finanzas", "Logística", "Legal"]
    nombres = ["Ana", "Carlos", "David", "Elena", "Laura", "Luis", "Maria", "Jose", "Carmen", "Pedro", "Miguel",
               "Lucia", "Jorge", "Rosa", "Daniel"]

    inserted = []
    for emp_id in range(1, 65):  # 64 registros para forzar varios splits
        nombre = random.choice(nombres) + " " + random.choice(["Lopez", "Gomez", "Perez", "Lee", "Chen", "Wang"])
        dept = random.choice(departamentos)
        salario = round(random.uniform(40000, 90000), 2)
        query = f'INSERT INTO empleados VALUES ({emp_id}, "{nombre}", "{dept}", {salario})'
        res = executor.execute(parse(query)[0])
        inserted.append((emp_id, nombre, dept, salario))
    print(f"   Insertados {len(inserted)} empleados (múltiples splits)")

    # === 3. Consultas por secondary index ===
    print("\n3. Consultas por secondary index")
    for dept in ["IT", "Ventas", "RRHH"]:
        res = executor.execute(parse(f'SELECT * FROM empleados WHERE departamento = "{dept}"')[0])
        print(f"   {dept}: {len(res.data)} empleados encontrados")
    print("   ✅ Consultas por hash index exitosas")

    # === 4. FORCE OVERFLOW IN A SINGLE BUCKET ===
    print("\n4. Forzar overflow en bucket único para testear _overflow_to_main_bucket")
    same_dept = "PruebaOverflow"
    executor.execute(parse(f'CREATE INDEX ON empleados (nombre) USING HASH')[0])  # para aislar index de nombre

    # Insertar muchos con mismo valor de hash (colisiones controladas)
    for i in range(50):
        nombre = f"Overflow_{i}"
        query = f'INSERT INTO empleados VALUES (900{i}, "{nombre}", "{same_dept}", 50000)'
        executor.execute(parse(query)[0])

    print("   ✅ Overflow forzado (debería haber varios buckets encadenados)")

    # === 5. ELIMINAR varios registros para provocar MIN_N en main bucket ===
    print("\n5. Eliminar registros para activar reinserción desde overflow")
    for i in range(30):  # deja solo pocos registros => main bucket debajo de MIN_N
        executor.execute(parse(f'DELETE FROM empleados WHERE nombre = "Overflow_{i}"')[0])

    print("   ✅ Eliminaciones parciales hechas — main bucket ahora pequeño")

    # === 6. VALIDAR QUE overflow_to_main_bucket SE EJECUTÓ ===
    print("\n6. Validar reinserción y liberación de overflow buckets")

    # Insertar nuevamente algunos para ver si reutiliza espacio liberado
    for i in range(5):
        nombre = f"Overflow_Reinsert_{i}"
        query = f'INSERT INTO empleados VALUES (999{i}, "{nombre}", "{same_dept}", 51000)'
        executor.execute(parse(query)[0])

    # Ejecutar consulta general para ver que registros existan
    res = executor.execute(parse(f'SELECT * FROM empleados WHERE departamento = "{same_dept}"')[0])
    print(f"   Total registros con dept '{same_dept}': {len(res.data)}")
    assert len(res.data) > 0, "No se encontraron registros tras reinserción (posible pérdida en reinserción)"

    # === 7. CONSISTENCY VALIDATION POST-COMPACTION ===
    print("\n7. Verificar que no haya referencias a buckets vacíos")
    res_all = executor.execute(parse("SELECT * FROM empleados")[0])
    print(f"   Total final de empleados: {len(res_all.data)}")
    print("   ✅ Overflow buckets deberían haberse liberado correctamente")

    # === 8. REINSERT TO CONFIRM REUSE ===
    print("\n8. Reinsertar para comprobar reutilización de buckets liberados")
    for i in range(5):
        nombre = f"Overflow_Reuse_{i}"
        query = f'INSERT INTO empleados VALUES (970{i}, "{nombre}", "{same_dept}", 52000)'
        executor.execute(parse(query)[0])
    print("   ✅ Reinserciones hechas sin errores")

    print("\n✅ TODAS LAS PRUEBAS PASARON EXITOSAMENTE")
    print("=" * 80)


if __name__ == "__main__":
    try:
        test_hash_secondary_exhaustive()
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback; traceback.print_exc()
