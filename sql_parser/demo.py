#!/usr/bin/env python3
import argparse
import time
from pathlib import Path
from typing import Any, Dict, List, Iterable, Optional
import csv
import tempfile
import shutil

from .parser import parse
from .executor import Executor
from indexes.core.database_manager import DatabaseManager


# ================= util UI =================
def banner(title: str) -> None:
    print(f"\n== {title} ==\n")


def format_ms(ms: float | None) -> str:
    if ms is None:
        return "[— ms]"
    try:
        return f"[{float(ms):.1f} ms]"
    except Exception:
        return f"[{ms} ms]"


def print_rows(rows: List[Dict[str, Any]], limit: int = 25) -> None:
    n = len(rows)
    if n == 0:
        print("Output: []")
        return
    show = rows[:limit]
    print(f"Output ({min(n, limit)} de {n} filas):")
    for i, r in enumerate(show, 1):
        print(f"  {i:>3}: {r}")
    if n > limit:
        print(f"... ({n - limit} más)")


class Stopwatch:
    def __enter__(self):
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, *exc):
        self.ms = (time.perf_counter() - self.t0) * 1000.0


def _print_opresult(out: Any, row_print_limit: int) -> None:
    if hasattr(out, "data"):
        data = out.data
        if isinstance(data, list) and (not data or isinstance(data[0], dict)):
            print_rows(data, limit=row_print_limit)
        else:
            print(f"Output: {data}")
        if hasattr(out, "disk_reads") or hasattr(out, "disk_writes") or hasattr(out, "execution_time_ms"):
            dr = getattr(out, "disk_reads", None)
            dw = getattr(out, "disk_writes", None)
            et = getattr(out, "execution_time_ms", None)
            rb = getattr(out, "rebuild_triggered", None)
            bd = getattr(out, "operation_breakdown", None)
            print(f"Stats: reads={dr}, writes={dw}, time={format_ms(et)}, rebuild={rb}")
            if bd:
                print(f"Breakdown: {bd}")
        print()
        return

    if isinstance(out, list) and (not out or isinstance(out[0], dict)):
        print_rows(out, limit=row_print_limit)
        print()
        return

    print(f"Output: {out}\n")


def run_block(title: str, stmts: Iterable[str], execu: Executor, row_print_limit: int = 25) -> None:
    banner(title)

    for sql in stmts:
        print(f"SQL: {sql}")
        try:
            print("  -> Parsing…")
            plans = parse(sql)
            print(f"  -> Parsed {len(plans)} plan(es).")
        except Exception as e:
            print(f"Parse error: {e}\n")
            continue

        for i, plan in enumerate(plans, 1):
            try:
                print(f"  -> Executing plan {i}/{len(plans)}: {type(plan).__name__}")
                with Stopwatch() as sw:
                    out = execu.execute(plan)
                    _print_opresult(out, row_print_limit)
                print("  -> Done", format_ms(sw.ms), "\n")
            except Exception as e:
                print(f"Execution error: {e}\n")


# ================= helpers CSV =================
def _make_sample_csv(src: Path, n_lines: int) -> Path:
    # Copia header + primeras n líneas a un temporal
    tmp_dir = Path(tempfile.mkdtemp(prefix="csv_demo_"))
    dst = tmp_dir / f"{src.stem}_head{n_lines}{src.suffix}"
    with src.open("r", encoding="utf-8", errors="ignore", newline="") as f_in, dst.open(
        "w", encoding="utf-8", newline=""
    ) as f_out:
        reader = csv.reader(f_in)
        writer = csv.writer(f_out)
        try:
            header = next(reader)
        except StopIteration:
            writer.writerow([])
            return dst
        writer.writerow(header)
        for i, row in enumerate(reader, 1):
            writer.writerow(row)
            if i >= n_lines:
                break
    return dst


# ================= bloques de pruebas =================
def build_statements_user_block(table: str, csv_path: str) -> List[str]:
    """
    Bloque que nos pasaste: PK ISAM + secundarios BTREE y consultas/borrados de demo.
    Usa el nombre de tabla 'table' sin sufijos.
    """
    csv_posix = Path(csv_path).as_posix()
    return [
        (
            f'CREATE TABLE {table} ('
            f'  id INT KEY INDEX ISAM,'
            f'  nombre VARCHAR[50] INDEX BTREE,'
            f'  cantidad INT,'
            f'  precio FLOAT,'
            f'  fecha DATE INDEX BTREE'
            f');'
        ),
        f'LOAD DATA FROM FILE "{csv_posix}" INTO {table};',
        f'SELECT * FROM {table} WHERE id = 403;',
        f'SELECT * FROM {table} WHERE id = 56;',
        f'SELECT * FROM {table} WHERE nombre = "Laptop";',
        f'SELECT * FROM {table} WHERE nombre BETWEEN "C" AND "N";',
        f'SELECT * FROM {table} WHERE precio = 813.52;',
        f'SELECT * FROM {table} WHERE precio BETWEEN 700 AND 900;',
        f'SELECT * FROM {table} WHERE fecha = "2024-07-30";',
        f'DELETE FROM {table} WHERE nombre = "Laptop";',
        f'SELECT * FROM {table} WHERE nombre = "Laptop";',
        f'SELECT * FROM {table} WHERE id = 403;',
        f'DELETE FROM {table} WHERE id = 403;',
        f'SELECT * FROM {table} WHERE id = 403;',
    ]


def build_statements_hash(table: str, csv_path: str, hash_col: str = "nombre") -> List[str]:
    """
    HASH como índice secundario (igualdad + delete).
    Crea tabla <table>_hash con PK ISAM y luego índice HASH en `hash_col`.
    """
    t = f"{table}_hash"
    csv_posix = Path(csv_path).as_posix()
    return [
        (
            f'CREATE TABLE {t} ('
            f'  id INT KEY INDEX ISAM,'
            f'  nombre VARCHAR[50],'            # índice HASH lo creamos luego
            f'  cantidad INT,'
            f'  precio FLOAT,'
            f'  fecha DATE'
            f');'
        ),
        f'LOAD DATA FROM FILE "{csv_posix}" INTO {t};',
        f'CREATE INDEX ON {t} ({hash_col}) USING HASH;',
        f'SELECT * FROM {t} WHERE {hash_col} = "Laptop";',
        f'DELETE FROM {t} WHERE {hash_col} = "Laptop";',
        f'SELECT * FROM {t} WHERE {hash_col} = "Laptop";',
    ]


def build_statements_sequential(table: str, csv_path: str) -> List[str]:
    """
    SEQUENTIAL como índice primario (PK), más secundario BTREE en nombre.
    Tabla <table>_seq.
    """
    t = f"{table}_seq"
    csv_posix = Path(csv_path).as_posix()
    return [
        (
            f'CREATE TABLE {t} ('
            f'  id INT KEY INDEX SEQUENTIAL,'
            f'  nombre VARCHAR[50] INDEX BTREE,'
            f'  cantidad INT,'
            f'  precio FLOAT,'
            f'  fecha DATE'
            f');'
        ),
        f'LOAD DATA FROM FILE "{csv_posix}" INTO {t};',
        f'SELECT * FROM {t} WHERE id = 403;',
        f'SELECT * FROM {t} WHERE id BETWEEN 50 AND 70;',
        f'DELETE FROM {t} WHERE id = 403;',
        f'SELECT * FROM {t} WHERE id = 403;',
    ]


def build_statements_btree(table: str, csv_path: str) -> List[str]:
    """
    BTREE como primario (clustered) + secundarios BTREE.
    Tabla <table>_btree para no colisionar con el bloque del usuario.
    """
    t = f"{table}_btree"
    csv_posix = Path(csv_path).as_posix()
    return [
        (
            f'CREATE TABLE {t} ('
            f'  id INT KEY INDEX BTREE,'
            f'  nombre VARCHAR[50] INDEX BTREE,'
            f'  cantidad INT,'
            f'  precio FLOAT,'
            f'  fecha DATE INDEX BTREE'
            f');'
        ),
        f'LOAD DATA FROM FILE "{csv_posix}" INTO {t};',
        f'SELECT * FROM {t} WHERE id = 403;',
        f'SELECT * FROM {t} WHERE id BETWEEN 50 AND 70;',
        f'SELECT * FROM {t} WHERE nombre = "Laptop";',
        f'SELECT * FROM {t} WHERE nombre BETWEEN "C" AND "N";',
        f'SELECT * FROM {t} WHERE precio = 813.52;',
        f'SELECT * FROM {t} WHERE precio BETWEEN 700 AND 900;',
        f'SELECT * FROM {t} WHERE fecha = "2024-07-30";',
        f'SELECT * FROM {t} WHERE fecha BETWEEN "2024-07-01" AND "2024-07-31";',
        f'DELETE FROM {t} WHERE nombre = "Laptop";',
        f'SELECT * FROM {t} WHERE nombre = "Laptop";',
        f'SELECT * FROM {t} WHERE id = 403;',
        f'DELETE FROM {t} WHERE id = 403;',
        f'SELECT * FROM {t} WHERE id = 403;',
    ]


# ================= main =================
def main():
    parser = argparse.ArgumentParser(description="SQL demo engine (CSV real, físico) — FULL")
    parser.add_argument(
        "--csv",
        default=str(Path("data/datasets/sales_dataset_unsorted.csv")),
        help="Ruta al CSV (por defecto: data/datasets/sales_dataset_unsorted.csv)",
    )
    parser.add_argument("--table", default="Ventas", help="Nombre base de tabla (se usarán sufijos para otros bloques)")
    parser.add_argument("--limit", type=int, default=25, help="Límite de filas a imprimir por SELECT (por defecto: 25)")
    parser.add_argument(
        "--sample",
        type=int,
        default=0,
        help="Si >0, usa un CSV temporal con las primeras N líneas para depurar LOAD DATA.",
    )
    parser.add_argument(
        "--hash-col",
        default="nombre",
        help='Columna para el índice HASH secundario (por defecto: "nombre")',
    )
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"[ERROR] No existe el CSV: {csv_path}")
        return

    # Opcional: CSV de muestra más pequeño
    sample_csv: Optional[Path] = None
    if args.sample and args.sample > 0:
        sample_csv = _make_sample_csv(csv_path, args.sample)
        print(f"[INFO] Usando CSV de muestra con {args.sample} líneas: {sample_csv}")
        csv_path = sample_csv

    # Nueva BD
    db = DatabaseManager(database_name="demo_db_full")
    execu = Executor(db)

    try:
        # BLOQUE 0: Tu bloque (PK ISAM + secundarios BTREE) usando el nombre base 'table'
        stmts_user = build_statements_user_block(args.table, str(csv_path))
        run_block(f"BLOQUE: {args.table} (CSV real, PK ISAM + secundarios BTREE)", stmts_user, execu, row_print_limit=args.limit)

        # BLOQUE 1: HASH secundario (en columna args.hash_col, por defecto nombre)
        stmts_hash = build_statements_hash(args.table, str(csv_path), hash_col=args.hash_col)
        run_block(f"BLOQUE: {args.table}_hash (PK ISAM + índice HASH en {args.hash_col})", stmts_hash, execu, row_print_limit=args.limit)

        # BLOQUE 2: SEQUENTIAL primario
        stmts_seq = build_statements_sequential(args.table, str(csv_path))
        run_block(f"BLOQUE: {args.table}_seq (PK SEQUENTIAL)", stmts_seq, execu, row_print_limit=args.limit)

        # BLOQUE 3: BTREE primario (tabla con sufijo _btree)
        stmts_btree = build_statements_btree(args.table, str(csv_path))
        run_block(f"BLOQUE: {args.table}_btree (PK BTREE + secundarios BTREE)", stmts_btree, execu, row_print_limit=args.limit)

    finally:
        if sample_csv and sample_csv.exists():
            try:
                # limpiar el tmp si lo generamos
                if sample_csv.parent.exists():
                    try:
                        shutil.rmtree(sample_csv.parent)
                    except Exception:
                        sample_csv.unlink(missing_ok=True)
                else:
                    sample_csv.unlink(missing_ok=True)
            except Exception:
                pass


if __name__ == "__main__":
    main()