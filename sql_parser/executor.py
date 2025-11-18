import csv
from typing import Dict, Any, List, Tuple, Optional
from .plan_types import (
    CreateTablePlan, LoadDataPlan, SelectPlan, InsertPlan, DeletePlan,
    CreateIndexPlan, DropTablePlan, DropIndexPlan,
    ColumnDef, ColumnType, PredicateEq, PredicateBetween, PredicateInPointRadius, PredicateKNN, PredicateFulltext
)
from indexes.core.record import Table, Record
from indexes.core.performance_tracker import OperationResult
from indexes.core.database_manager import DatabaseManager


class Executor:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    def execute(self, plan):
        if isinstance(plan, CreateTablePlan):
            return self._create_table(plan)
        elif isinstance(plan, LoadDataPlan):
            return self._load_data(plan)
        elif isinstance(plan, SelectPlan):
            return self._select(plan)
        elif isinstance(plan, InsertPlan):
            return self._insert(plan)
        elif isinstance(plan, DeletePlan):
            return self._delete(plan)
        elif isinstance(plan, CreateIndexPlan):
            return self._create_index(plan)
        elif isinstance(plan, DropTablePlan):
            return self._drop_table(plan)
        elif isinstance(plan, DropIndexPlan):
            return self._drop_index(plan)
        else:
            raise NotImplementedError(f"Plan no soportado: {type(plan)}")

    def _col_to_physical(self, c: ColumnDef) -> Optional[Tuple[str, str, int]]:
        name = c.name
        kind = c.type.kind
        if kind == "INT":
            return (name, "INT", 4)
        if kind == "FLOAT":
            return (name, "FLOAT", 4)
        if kind == "VARCHAR":
            ln = c.type.length or 32
            return (name, "CHAR", ln)
        if kind == "DATE":
            return (name, "CHAR", 10)  # YYYY-MM-DD
        if kind == "ARRAY":
            dimensions = c.type.length or 2
            return (name, "ARRAY", dimensions)
        return None

    def _pick_primary(self, columns: List[ColumnDef]) -> Tuple[str, str]:
        pk_col = None
        for c in columns:
            if c.is_key:
                pk_col = c
                break
        if pk_col is None:
            for c in columns:
                if c.type.kind == "INT":
                    pk_col = c
                    break
        if pk_col is None:
            pk_col = columns[0]
        pk_name = pk_col.name
        idx_decl = (pk_col.index or "ISAM").upper()
        allowed_primary = {"ISAM", "SEQUENTIAL", "BTREE"}
        primary_index_type = idx_decl if idx_decl in allowed_primary else "BTREE"
        return pk_name, primary_index_type

    # ====== CREATE TABLE ======
    def _create_table(self, plan: CreateTablePlan):
        physical_fields: List[Tuple[str, str, int]] = []
        ignored_cols: List[str] = []
        secondary_decls: List[Tuple[str, str]] = []

        materialized = set()
        for c in plan.columns:
            phys = self._col_to_physical(c)
            if phys is None:
                ignored_cols.append(c.name)
            else:
                physical_fields.append(phys)
                materialized.add(c.name)
            if c.index and (not c.is_key) and (phys is not None):
                secondary_decls.append((c.name, c.index.upper()))

        if not physical_fields:
            raise ValueError("Ninguna columna soportada para almacenamiento físico")

        pk_field, primary_index_type = self._pick_primary(plan.columns)

        table = Table(
            table_name=plan.table,
            sql_fields=physical_fields,
            key_field=pk_field,
            extra_fields=None
        )
        self.db.create_table(table, primary_index_type=primary_index_type)

        unsupported: List[str] = []
        created_any = False
        for colname, idx_kind in secondary_decls:
            try:
                if self.db._validate_secondary_index(idx_kind):
                    self.db.create_index(plan.table, colname, idx_kind, scan_existing=False)
                    created_any = True
                else:
                    unsupported.append(f"{colname}:{idx_kind}")
            except NotImplementedError as e:
                unsupported.append(f"{colname}:{idx_kind}(NotImpl)")
            except Exception as e:
                unsupported.append(f"{colname}:{idx_kind}({str(e)[:30]})")

        msg_parts = [f"OK: tabla {plan.table} creada (primario={primary_index_type}, key={pk_field})"]
        if ignored_cols:
            msg_parts.append(f"— Columnas no soportadas (ignoradas): {', '.join(ignored_cols)}")
        if unsupported or (secondary_decls and not created_any):
            if not unsupported:  # declarados pero ninguno pudo crearse
                unsupported = [f"{c}:{k}" for c, k in secondary_decls]
            msg_parts.append(f"— Índices secundarios no soportados: {', '.join(unsupported)}")
        result_msg = " ".join(msg_parts)
        return OperationResult(result_msg, 0, 0, 0)  # CREATE TABLE doesn't involve significant disk I/O in our model

    # ====== helpers CSV ======
    def _defaults_for_field(self, ftype: str) -> Any:
        if ftype == "INT":
            return 0
        if ftype == "FLOAT":
            return 0.0
        if ftype == "CHAR":
            return ""
        if ftype == "BOOL":
            return False
        if ftype == "ARRAY":
            return (0.0, 0.0) 
        return None

    def _cast_value(self, raw: str, ftype: str):
        if raw is None:
            return None
        raw = str(raw).strip()
        if raw == "":
            return self._defaults_for_field(ftype)
        if ftype == "INT":
            return int(raw)
        if ftype == "FLOAT":
            return float(raw)
        if ftype == "CHAR":
            return raw
        if ftype == "BOOL":
            return raw.lower() in ("1", "true", "t", "yes", "y", "si", "sí")
        return raw

    def _guess_delimiter(self, header_line: str) -> str:
        return ";" if header_line.count(";") >= header_line.count(",") else ","

    # ====== LOAD DATA FROM FILE ======
    def _load_data(self, plan: LoadDataPlan):
        info = self.db.get_table_info(plan.table)
        if not info:
            raise ValueError(f"Tabla {plan.table} no existe; crea la tabla primero con CREATE TABLE")

        table_obj = self.db.tables[plan.table]["table"]
        phys_fields = table_obj.all_fields
        key_field = table_obj.key_field

        inserted = duplicates = cast_err = 0
        total_reads = total_writes = 0
        total_time_ms = 0.0

        with open(plan.filepath, "r", encoding="utf-8", newline="") as fh_probe:
            first_line = fh_probe.readline()
            delimiter = self._guess_delimiter(first_line)

        with open(plan.filepath, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f, delimiter=delimiter)
            header = next(reader, None)
            if not header:
                return OperationResult("CSV vacío: insertados=0", 0, 0, 0)

            # Excluir campos internos (active, etc) que no vienen del CSV
            user_fields = [(name, ftype, fsize) for (name, ftype, fsize) in phys_fields
                          if name not in ['active']]

            key_in_csv = key_field in header
            auto_increment_counter = 1

            for row_values in reader:
                rec = Record(phys_fields, key_field)
                ok_row = True

                for field_name, field_type, field_size in user_fields:
                    try:
                        if field_name == key_field and not key_in_csv:
                            rec.set_field_value(field_name, auto_increment_counter)

                        elif field_type == "ARRAY" and plan.column_mappings and field_name in plan.column_mappings:
                            csv_column_names = plan.column_mappings[field_name]
                            array_values = []
                            
                            for csv_col in csv_column_names:
                                try:
                                    csv_idx = header.index(csv_col)
                                    if csv_idx < len(row_values):
                                        val = self._cast_value(row_values[csv_idx], "FLOAT")
                                        array_values.append(val)
                                    else:
                                        array_values.append(0.0)
                                except (ValueError, IndexError):
                                    array_values.append(0.0)
                            
                            while len(array_values) < field_size:
                                array_values.append(0.0)
                            array_values = array_values[:field_size]
                            
                            rec.set_field_value(field_name, tuple(array_values))
                            
                        elif field_type != "ARRAY":
                            try:
                                csv_idx = header.index(field_name)
                                if csv_idx < len(row_values):
                                    raw = row_values[csv_idx]
                                else:
                                    raw = None
                            except ValueError:
                                raw = None


                            val = self._cast_value(raw, field_type)
                            rec.set_field_value(field_name, val)
                        else:
                            rec.set_field_value(field_name, tuple([0.0] * field_size))
                            
                    except Exception as e:
                        ok_row = False
                        break

                if 'active' in [name for (name, _, _) in phys_fields]:
                    rec.set_field_value('active', True)

                if not ok_row:
                    cast_err += 1
                    continue

                try:
                    res = self.db.insert(plan.table, rec)
                    total_reads += res.disk_reads
                    total_writes += res.disk_writes
                    total_time_ms += res.execution_time_ms

                    if hasattr(res, "data") and (res.data is False):
                        duplicates += 1
                    else:
                        inserted += 1

                    if not key_in_csv:
                        auto_increment_counter += 1
                        
                except Exception as e:
                    cast_err += 1
                    if not key_in_csv:
                        auto_increment_counter += 1
                    continue

        self.db.warm_up_indexes(plan.table)

        summary = f"CSV cargado: insertados={inserted}, duplicados={duplicates}, cast_err={cast_err}"
        return OperationResult(summary, total_time_ms, total_reads, total_writes)

    # ====== SELECT ======
    def _get_ftype(self, table: str, col: str) -> Optional[str]:
        tinfo = self.db.tables.get(table)
        if not tinfo:
            return None
        for (n, ftype, _sz) in tinfo["table"].all_fields:
            if n == col:
                return ftype
        return None

    def _project_records(self, records: List[Record], columns: Optional[List[str]]) -> List[Dict[str, Any]]:
        if not records:
            return []
        out = []
        names = [n for (n, _, _) in records[0].value_type_size]
        pick = names if (columns is None) else columns
        for r in records:
            obj = {}
            for c in pick:
                val = getattr(r, c, None)
                if isinstance(val, bytes):
                    try:
                        val = val.decode("utf-8").rstrip("\x00").strip()
                    except UnicodeDecodeError:
                        val = val.decode("utf-8", errors="replace").rstrip("\x00").strip()
                obj[c] = val
            
            if hasattr(r, '_text_score'):
                obj['_text_score'] = r._text_score
            
            out.append(obj)
        return out

    def _select(self, plan: SelectPlan):
        table = plan.table
        where = plan.where

        if where is None:
            res = self.db.scan_all(table)
            projected_data = self._project_records(res.data, plan.columns)
            return OperationResult(projected_data, res.execution_time_ms, res.disk_reads, res.disk_writes, res.rebuild_triggered, res.operation_breakdown)

        if isinstance(where, PredicateEq):
            col = where.column
            val = where.value

            res = self.db.search(table, val, field_name=col)
            data_list = res.data if isinstance(res.data, list) else ([res.data] if res.data else [])
            projected_data = self._project_records(data_list, plan.columns)
            return OperationResult(projected_data, res.execution_time_ms, res.disk_reads, res.disk_writes, res.rebuild_triggered, res.operation_breakdown)

        if isinstance(where, PredicateBetween):
            col = where.column
            lo = where.lo
            hi = where.hi
            res = self.db.range_search(table, lo, hi, field_name=col)
            projected_data = self._project_records(res.data, plan.columns)
            return OperationResult(projected_data, res.execution_time_ms, res.disk_reads, res.disk_writes, res.rebuild_triggered, res.operation_breakdown)

        if isinstance(where, (PredicateInPointRadius, PredicateKNN)):
            col = where.column

            if isinstance(where, PredicateInPointRadius):
                # point, radius
                res = self.db.range_search(plan.table, list(where.point), where.radius, field_name=col, spatial_type="radius")
            else:  # PredicateKNN
                # point, k
                res = self.db.range_search(plan.table, list(where.point), where.k, field_name=col, spatial_type="knn")

            projected_data = self._project_records(res.data, plan.columns)
            return OperationResult(projected_data, res.execution_time_ms, res.disk_reads, res.disk_writes, res.rebuild_triggered, res.operation_breakdown)

        if isinstance(where, PredicateFulltext):
            col = where.column
            query = where.query
            
            table_info = self.db.tables.get(table)
            if not table_info:
                raise ValueError(f"Tabla {table} no existe")
            
            if col not in table_info["secondary_indexes"]:
                raise ValueError(f"El campo '{col}' no tiene un índice secundario. Use CREATE INDEX para crear un índice INVERTED_TEXT primero.")
            
            index_type = table_info["secondary_indexes"][col]["type"]
            if index_type != "INVERTED_TEXT":
                raise ValueError(f"El operador @@ requiere un índice INVERTED_TEXT en el campo '{col}'. Actualmente tiene índice {index_type}.")
            
            limit = plan.limit if plan.limit else None

            res = self.db.search(table, query, field_name=col, limit=limit)

            data_list = res.data if isinstance(res.data, list) else []
            
            projected_data = self._project_records(data_list, plan.columns)
            return OperationResult(projected_data, res.execution_time_ms, res.disk_reads, res.disk_writes, res.rebuild_triggered, res.operation_breakdown)
        
        raise NotImplementedError("Predicado WHERE no soportado")

    # ====== INSERT ======
    def _insert(self, plan: InsertPlan):
        tinfo = self.db.get_table_info(plan.table)
        if not tinfo:
            raise ValueError(f"Tabla {plan.table} no existe")

        table_obj = self.db.tables[plan.table]["table"]
        phys_fields = table_obj.all_fields
        key_field = table_obj.key_field
        names = [n for (n, _, _) in phys_fields]

        rec = Record(phys_fields, key_field)

        if plan.columns is None:
            values = plan.values
            padded = list(values[:len(names)])
            if len(padded) < len(names):
                padded += [None] * (len(names) - len(padded))
            for (name, ftype, _), val in zip(phys_fields, padded):
                v = val
                if v is None:
                    if name == "active" and self.db.tables[plan.table]["primary_type"] == "SEQUENTIAL":
                        v = True
                    else:
                        v = self._defaults_for_field(ftype)
                if ftype == "INT" and v is not None:
                    v = int(v)
                elif ftype == "FLOAT" and v is not None:
                    v = float(v)
                elif ftype == "ARRAY" and v is not None:
                    if isinstance(v, (list, tuple)):
                        v = tuple(float(x) for x in v)
                rec.set_field_value(name, v)
        else:
            for (name, ftype, _) in phys_fields:
                rec.set_field_value(name, self._defaults_for_field(ftype))
            for c, v in zip(plan.columns, plan.values):
                idx = names.index(c)
                _, ftype, _ = phys_fields[idx]
                vv = v
                if ftype == "INT" and vv is not None:
                    vv = int(vv)
                elif ftype == "FLOAT" and vv is not None:
                    vv = float(vv)
                elif ftype == "ARRAY" and vv is not None:
                    if isinstance(vv, (list, tuple)):
                        vv = tuple(float(x) for x in vv)
                rec.set_field_value(c, vv)

        res = self.db.insert(plan.table, rec)

        success_msg = "OK" if bool(res.data) else "Duplicado/No insertado"
        return OperationResult(success_msg, res.execution_time_ms, res.disk_reads, res.disk_writes, res.rebuild_triggered, res.operation_breakdown)

    def _delete(self, plan: DeletePlan):
        tinfo = self.db.get_table_info(plan.table)
        if not tinfo:
            raise ValueError(f"Tabla {plan.table} no existe")

        where = plan.where
        if not isinstance(where, (PredicateEq, PredicateBetween)):
            raise NotImplementedError("DELETE soporta = y BETWEEN por ahora")

        if isinstance(where, PredicateEq):
            col = where.column
            val = where.value
            pk_name = self.db.tables[plan.table]["table"].key_field
            res = self.db.delete(plan.table, val, field_name=(None if col == pk_name else col))

            data = res.data
            if isinstance(data, bool):
                deleted = 1 if data else 0
            else:
                try:
                    deleted = int(data)
                except Exception:
                    deleted = 0
            result_msg = f"OK ({deleted} registros)"
            return OperationResult(result_msg, res.execution_time_ms, res.disk_reads, res.disk_writes, res.rebuild_triggered, res.operation_breakdown)
        else:
            col = where.column
            lo = where.lo
            hi = where.hi
            res = self.db.range_delete(plan.table, lo, hi, field_name=col)

            data = res.data
            try:
                deleted = int(data)
            except Exception:
                deleted = 0
            result_msg = f"OK ({deleted} registros)"
            return OperationResult(result_msg, res.execution_time_ms, res.disk_reads, res.disk_writes, res.rebuild_triggered, res.operation_breakdown)

    # ====== CREATE INDEX ======
    def _create_index(self, plan: CreateIndexPlan):
        try:
            if not self.db._validate_secondary_index(plan.index_type.upper()):
                return OperationResult(f"ERROR: Tipo de índice '{plan.index_type}' no soportado", 0, 0, 0)

            result = self.db.create_index(plan.table, plan.column, plan.index_type.upper(), language=plan.language)
            return OperationResult(
                f"OK: Índice creado en {plan.table}.{plan.column} usando {plan.index_type.upper()}: {result.data}",
                result.execution_time_ms,
                result.disk_reads,
                result.disk_writes
            )
        except Exception as e:
            return OperationResult(f"ERROR: {e}", 0, 0, 0)

    # ====== DROP TABLE ======
    def _drop_table(self, plan: DropTablePlan):
        try:
            if plan.table not in self.db.tables:
                return OperationResult(f"ERROR: Tabla '{plan.table}' no existe", 0, 0, 0)

            removed_files = self.db.drop_table(plan.table)
            files_info = f" (archivos eliminados: {len(removed_files)})" if removed_files else ""

            return OperationResult(f"OK: Tabla '{plan.table}' eliminada{files_info}", 0, 0, 0)
        except Exception as e:
            return OperationResult(f"ERROR: {e}", 0, 0, 0)

    # ====== DROP INDEX ======
    def _drop_index(self, plan: DropIndexPlan):
        try:
            table_name = plan.table
            field_name = plan.field_name

            removed_files = self.db.drop_index(table_name, field_name)
            files_info = f" (archivos eliminados: {len(removed_files)})" if removed_files else ""
            return OperationResult(f"OK: Índice eliminado en campo '{field_name}' de tabla '{table_name}'{files_info}", 0, 0, 0)

        except ValueError as e:
            return OperationResult(f"ERROR: {e}", 0, 0, 0)
        except Exception as e:
            return OperationResult(f"ERROR: {e}", 0, 0, 0)