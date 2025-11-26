import os
import json
from .record import Table, Record, IndexRecord
from .performance_tracker import OperationResult

from ..bplus_tree.bplus_tree_clustered import BPlusTreeClusteredIndex
from ..bplus_tree.bplus_tree_unclustered import BPlusTreeUnclusteredIndex
from ..isam.isam import ISAMPrimaryIndex
from ..extendible_hashing.extendible_hashing import ExtendibleHashing
from ..sequential_file.sequential_file import SequentialFile

class DatabaseManager:

    INDEX_TYPES = {
        "SEQUENTIAL": {"primary": True, "secondary": False},
        "ISAM": {"primary": True, "secondary": False},
        "BTREE": {"primary": True, "secondary": True},
        "HASH": {"primary": False, "secondary": True},
        "RTREE": {"primary": False, "secondary": True},
        "INVERTED_TEXT": {"primary": False, "secondary": True},
        "MULTIMEDIA_SEQ": {"primary": False, "secondary": True},
        "MULTIMEDIA_INV": {"primary": False, "secondary": True}
    }

    def __init__(self, database_name: str = None, base_path: str = None):
        self.tables = {}
        self.database_name = database_name or "default"
        if base_path:
            self.base_dir = os.path.join(base_path, self.database_name)
        else:
            self.base_dir = os.path.join("data", "databases", self.database_name)
        os.makedirs(self.base_dir, exist_ok=True)
        self.metadata_file = os.path.join(self.base_dir, "_metadata.json")
        self._load_metadata()

    def create_table(self, table: Table, primary_index_type: str = "ISAM"):
        if not self._validate_primary_index(primary_index_type):
            raise ValueError(f"{primary_index_type} cannot be used as primary index")

        table_name = table.table_name
        if table_name in self.tables:
            raise ValueError(f"Table {table_name} already exists")

        table_info = {
            "table": table,
            "primary_index": None,
            "secondary_indexes": {},
            "primary_type": primary_index_type
        }

        primary_index = self._create_primary_index(table, primary_index_type)

        if primary_index_type == "SEQUENTIAL":
            extra_fields = {"active": ("BOOL", 1)}
            table_with_active = Table(
                table_name=table.table_name,
                sql_fields=table.sql_fields,
                key_field=table.key_field,
                extra_fields=extra_fields
            )
            table_info["table"] = table_with_active

        table_info["primary_index"] = primary_index
        self.tables[table_name] = table_info
        self._save_metadata()

        return True

    def create_index(self, table_name: str, field_name: str, index_type: str, scan_existing: bool = True, language: str = "spanish", feature_type: str = "SIFT", multimedia_directory: str = None, multimedia_pattern: str = None):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")

        if not self._validate_secondary_index(index_type):
            raise ValueError(f"{index_type} cannot be used as secondary index")

        table_info = self.tables[table_name]
        table = table_info["table"]

        if field_name == table.key_field:
            raise ValueError(f"Cannot create secondary index on primary key field '{field_name}'")

        field_info = self._get_field_info(table, field_name)
        if not field_info:
            raise ValueError(f"Field {field_name} not found in table {table_name}")

        if field_name in table_info["secondary_indexes"]:
            raise ValueError(f"Index on {field_name} already exists")

        secondary_index = self._create_secondary_index(table, field_name, index_type, language=language, feature_type=feature_type, multimedia_directory=multimedia_directory, multimedia_pattern=multimedia_pattern)

        table_info["secondary_indexes"][field_name] = {
            "index": secondary_index,
            "type": index_type,
            "language": language if index_type == "INVERTED_TEXT" else None,
            "feature_type": feature_type if index_type in ("MULTIMEDIA_SEQ", "MULTIMEDIA_INV") else None,
            "multimedia_directory": multimedia_directory if index_type in ("MULTIMEDIA_SEQ", "MULTIMEDIA_INV") else None,
            "multimedia_pattern": multimedia_pattern if index_type in ("MULTIMEDIA_SEQ", "MULTIMEDIA_INV") else None
        }

        total_reads = 0
        total_writes = 0
        total_time = 0
        records_indexed = 0

        if index_type == "INVERTED_TEXT":
            if scan_existing:
                primary_index = table_info["primary_index"]
                if hasattr(primary_index, 'scan_all'):
                    try:
                        scan_result = primary_index.scan_all()
                        existing_records = scan_result.data

                        total_reads = scan_result.disk_reads
                        total_writes = scan_result.disk_writes
                        total_time = scan_result.execution_time_ms

                        build_result = secondary_index.build(existing_records)
                        total_reads += build_result.disk_reads
                        total_writes += build_result.disk_writes
                        total_time += build_result.execution_time_ms

                        records_indexed = len(existing_records)

                    except Exception as e:
                        del table_info["secondary_indexes"][field_name]
                        if hasattr(secondary_index, 'drop_index'):
                            secondary_index.drop_index()
                        raise ValueError(f"Error building fulltext index: {e}")

            self._save_metadata()
            return OperationResult(
                data=f"Fulltext index created on {field_name} with {records_indexed} documents indexed",
                execution_time_ms=total_time,
                disk_reads=total_reads,
                disk_writes=total_writes
            )

        if index_type in ("MULTIMEDIA_SEQ", "MULTIMEDIA_INV"):
            if scan_existing:
                primary_index = table_info["primary_index"]
                if hasattr(primary_index, 'scan_all'):
                    try:
                        scan_result = primary_index.scan_all()
                        existing_records = scan_result.data

                        total_reads = scan_result.disk_reads
                        total_writes = scan_result.disk_writes
                        total_time = scan_result.execution_time_ms

                        build_result = secondary_index.build(existing_records)
                        total_reads += build_result.disk_reads
                        total_writes += build_result.disk_writes
                        total_time += build_result.execution_time_ms

                        records_indexed = len(existing_records)

                    except Exception as e:
                        del table_info["secondary_indexes"][field_name]
                        raise ValueError(f"Error building multimedia index: {e}")

            self._save_metadata()
            return OperationResult(
                data=f"Multimedia index ({index_type}) created on {field_name} with {records_indexed} files indexed",
                execution_time_ms=total_time,
                disk_reads=total_reads,
                disk_writes=total_writes
            )
        
        if scan_existing:
            primary_index = table_info["primary_index"]
            if hasattr(primary_index, 'scan_all'):
                try:
                    scan_result = primary_index.scan_all()
                    existing_records = scan_result.data

                    total_reads += scan_result.disk_reads
                    total_writes += scan_result.disk_writes
                    total_time += scan_result.execution_time_ms

                    field_type, field_size = field_info
                    skipped_duplicates = 0
                    for record in existing_records:
                        secondary_value = getattr(record, field_name)
                        primary_key = record.get_key()

                        index_record = IndexRecord(field_type, field_size)
                        index_record.set_index_data(secondary_value, primary_key)

                        insert_result = secondary_index.insert(index_record)

                        total_reads += insert_result.disk_reads
                        total_writes += insert_result.disk_writes
                        total_time += insert_result.execution_time_ms

                        if insert_result.disk_writes > 0 or index_type != "HASH":
                            records_indexed += 1
                        else:
                            skipped_duplicates += 1

                    if skipped_duplicates > 0:
                        print(f"Skipped {skipped_duplicates} duplicate records during index creation")

                except Exception as e:
                    del table_info["secondary_indexes"][field_name]
                    if hasattr(secondary_index, 'drop_index'):
                        secondary_index.drop_index()
                    raise ValueError(f"Error indexing existing records: {e}")
                
        self._save_metadata()

        return OperationResult(
            data=f"Index created on {field_name} with {records_indexed} records indexed",
            execution_time_ms=total_time,
            disk_reads=total_reads,
            disk_writes=total_writes
        )

    def insert(self, table_name: str, record: Record):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")

        table_info = self.tables[table_name]
        primary_index = table_info["primary_index"]

        primary_result = primary_index.insert(record)

        total_reads = primary_result.disk_reads
        total_writes = primary_result.disk_writes
        total_time = primary_result.execution_time_ms

        breakdown = {
            "primary_metrics": {"reads": primary_result.disk_reads, "writes": primary_result.disk_writes, "time_ms": primary_result.execution_time_ms}
        }

        if not primary_result.data:
            return OperationResult(False, total_time, total_reads, total_writes, primary_result.rebuild_triggered, breakdown)

        for field_name, index_info in table_info["secondary_indexes"].items():
            index_type = index_info["type"]

            if index_type == "INVERTED_TEXT":
                continue

            secondary_index = index_info["index"]

            field_type, field_size = self._get_field_info(table_info["table"], field_name)
            secondary_value = getattr(record, field_name)
            primary_key = record.get_key()

            index_record = IndexRecord(field_type, field_size)
            index_record.set_index_data(secondary_value, primary_key)

            secondary_result = secondary_index.insert(index_record)
            total_reads += secondary_result.disk_reads
            total_writes += secondary_result.disk_writes
            total_time += secondary_result.execution_time_ms

            breakdown[f"secondary_metrics_{field_name}"] = {
                "reads": secondary_result.disk_reads,
                "writes": secondary_result.disk_writes,
                "time_ms": secondary_result.execution_time_ms
            }

        return OperationResult(primary_result.data, total_time, total_reads, total_writes, primary_result.rebuild_triggered, breakdown)

    def search(self, table_name: str, value, field_name: str = None, limit: int = None):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")

        table_info = self.tables[table_name]
        table = table_info["table"]

        if field_name is None or field_name == table.key_field:
            primary_index = table_info["primary_index"]
            result = primary_index.search(value)
            if result.data:
                return OperationResult([result.data], result.execution_time_ms, result.disk_reads, result.disk_writes)
            else:
                return OperationResult([], result.execution_time_ms, result.disk_reads, result.disk_writes)

        elif field_name in table_info["secondary_indexes"]:
            secondary_info = table_info["secondary_indexes"][field_name]
            secondary_index = secondary_info["index"]
            index_type = secondary_info["type"]
            primary_index = table_info["primary_index"]

            if index_type == "INVERTED_TEXT":
                top_k = limit if limit is not None else None
                secondary_result = secondary_index.search(value, top_k=top_k)

                if not secondary_result.data:
                    breakdown = {
                        "primary_metrics": {"reads": 0, "writes": 0, "time_ms": 0},
                        "secondary_metrics": {"reads": secondary_result.disk_reads, "writes": secondary_result.disk_writes, "time_ms": secondary_result.execution_time_ms}
                    }
                    return OperationResult([], secondary_result.execution_time_ms, secondary_result.disk_reads, secondary_result.disk_writes, operation_breakdown=breakdown)

                total_time = secondary_result.execution_time_ms
                total_reads = secondary_result.disk_reads
                total_writes = secondary_result.disk_writes

                primary_lookup_reads = 0
                primary_lookup_writes = 0
                primary_lookup_time = 0

                matching_records = []
                for doc_id, score in secondary_result.data:
                    primary_result = primary_index.search(doc_id)
                    primary_lookup_reads += primary_result.disk_reads
                    primary_lookup_writes += primary_result.disk_writes
                    primary_lookup_time += primary_result.execution_time_ms

                    if primary_result.data:
                        record = primary_result.data
                        if not hasattr(record, '_text_score'):
                            record._text_score = score
                        matching_records.append(record)

                total_reads += primary_lookup_reads
                total_writes += primary_lookup_writes
                total_time += primary_lookup_time

                breakdown = {
                    "primary_metrics": {"reads": primary_lookup_reads, "writes": primary_lookup_writes, "time_ms": primary_lookup_time},
                    "secondary_metrics": {"reads": secondary_result.disk_reads, "writes": secondary_result.disk_writes, "time_ms": secondary_result.execution_time_ms}
                }

                return OperationResult(matching_records, total_time, total_reads, total_writes, operation_breakdown=breakdown)

            if index_type in ("MULTIMEDIA_SEQ", "MULTIMEDIA_INV"):
                top_k = limit if limit is not None else 10
                secondary_result = secondary_index.search(value, top_k=top_k)

                if not secondary_result.data:
                    breakdown = {
                        "primary_metrics": {"reads": 0, "writes": 0, "time_ms": 0},
                        "secondary_metrics": {"reads": secondary_result.disk_reads, "writes": secondary_result.disk_writes, "time_ms": secondary_result.execution_time_ms}
                    }
                    return OperationResult([], secondary_result.execution_time_ms, secondary_result.disk_reads, secondary_result.disk_writes, operation_breakdown=breakdown)

                total_time = secondary_result.execution_time_ms
                total_reads = secondary_result.disk_reads
                total_writes = secondary_result.disk_writes

                primary_lookup_reads = 0
                primary_lookup_writes = 0
                primary_lookup_time = 0

                matching_records = []
                for doc_id, score in secondary_result.data:
                    primary_result = primary_index.search(doc_id)
                    primary_lookup_reads += primary_result.disk_reads
                    primary_lookup_writes += primary_result.disk_writes
                    primary_lookup_time += primary_result.execution_time_ms

                    if primary_result.data:
                        record = primary_result.data
                        if not hasattr(record, '_multimedia_score'):
                            record._multimedia_score = score
                        matching_records.append(record)

                total_reads += primary_lookup_reads
                total_writes += primary_lookup_writes
                total_time += primary_lookup_time

                breakdown = {
                    "primary_metrics": {"reads": primary_lookup_reads, "writes": primary_lookup_writes, "time_ms": primary_lookup_time},
                    "secondary_metrics": {"reads": secondary_result.disk_reads, "writes": secondary_result.disk_writes, "time_ms": secondary_result.execution_time_ms}
                }

                return OperationResult(matching_records, total_time, total_reads, total_writes, operation_breakdown=breakdown)

            secondary_result = secondary_index.search(value)
            if not secondary_result.data:
                breakdown = {
                    "primary_metrics": {"reads": 0, "writes": 0, "time_ms": 0},
                    "secondary_metrics": {"reads": secondary_result.disk_reads, "writes": secondary_result.disk_writes, "time_ms": secondary_result.execution_time_ms}
                }
                return OperationResult([], secondary_result.execution_time_ms, secondary_result.disk_reads, secondary_result.disk_writes, operation_breakdown=breakdown)

            total_reads = secondary_result.disk_reads
            total_writes = secondary_result.disk_writes
            total_time = secondary_result.execution_time_ms

            primary_lookup_reads = 0
            primary_lookup_writes = 0
            primary_lookup_time = 0

            if secondary_result.data:
                matching_records = []
                for primary_key in secondary_result.data:
                    primary_result = primary_index.search(primary_key)
                    primary_lookup_reads += primary_result.disk_reads
                    primary_lookup_writes += primary_result.disk_writes
                    primary_lookup_time += primary_result.execution_time_ms

                    if primary_result.data:
                        matching_records.append(primary_result.data)

                total_reads += primary_lookup_reads
                total_writes += primary_lookup_writes
                total_time += primary_lookup_time
            else:
                matching_records = []

            breakdown = {
                "primary_metrics": {"reads": primary_lookup_reads, "writes": primary_lookup_writes, "time_ms": primary_lookup_time},
                "secondary_metrics": {"reads": secondary_result.disk_reads, "writes": secondary_result.disk_writes, "time_ms": secondary_result.execution_time_ms}
            }

            return OperationResult(matching_records, total_time, total_reads, total_writes, operation_breakdown=breakdown)

        else:
            table = table_info["table"]
            field_info = self._get_field_info(table, field_name)
            if not field_info:
                raise ValueError(f"Field {field_name} not found in table {table_name}")

            primary_index = table_info["primary_index"]

            if hasattr(primary_index, 'scan_all'):
                scan_result = primary_index.scan_all()
                all_records = scan_result.data
            else:
                raise NotImplementedError(f"Full scan not supported for {table_info['primary_type']} index")

            matching_records = []
            for record in all_records:
                record_value = getattr(record, field_name, None)
                if record_value is not None:
                    if hasattr(record_value, 'decode'):
                        record_value = record_value.decode('utf-8').rstrip('\x00').rstrip()
                    else:
                        record_value = str(record_value).rstrip()

                    value_str = value.decode('utf-8').rstrip('\x00').rstrip() if hasattr(value, 'decode') else str(value).rstrip()

                    if record_value == value_str:
                        matching_records.append(record)

            return OperationResult(matching_records, scan_result.execution_time_ms, scan_result.disk_reads, scan_result.disk_writes)

    def range_search(self, table_name: str, start_key, end_key, field_name: str = None, spatial_type: str = None):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")

        table_info = self.tables[table_name]
        table = table_info["table"]

        if field_name is None or field_name == table.key_field:
            primary_index = table_info["primary_index"]
            return primary_index.range_search(start_key, end_key)

        elif field_name in table_info["secondary_indexes"]:
            secondary_info = table_info["secondary_indexes"][field_name]
            secondary_index = secondary_info["index"]
            secondary_type = secondary_info["type"]
            primary_index = table_info["primary_index"]

            if secondary_type == "HASH":
                raise NotImplementedError(f"Range search is not supported for HASH indexes (secondary index on '{field_name}'). Hash indexes are optimized for exact key lookups only.")
            
            if secondary_type == "RTREE":
                if spatial_type is None:
                    raise ValueError("spatial_type is required for R-Tree searches. Use 'radius' or 'knn'")
                secondary_result = secondary_index.range_search(start_key, end_key, spatial_type)
            else:
                secondary_result = secondary_index.range_search(start_key, end_key)
                
            if not secondary_result.data:
                breakdown = {
                    "primary_metrics": {"reads": 0, "writes": 0, "time_ms": 0},
                    "secondary_metrics": {"reads": secondary_result.disk_reads, "writes": secondary_result.disk_writes, "time_ms": secondary_result.execution_time_ms}
                }
                return OperationResult([], secondary_result.execution_time_ms, secondary_result.disk_reads, secondary_result.disk_writes, operation_breakdown=breakdown)

            total_reads = secondary_result.disk_reads
            total_writes = secondary_result.disk_writes
            total_time = secondary_result.execution_time_ms

            primary_lookup_reads = 0
            primary_lookup_writes = 0
            primary_lookup_time = 0

            if secondary_result.data:
                matching_records = []
                for primary_key in secondary_result.data:
                    primary_result = primary_index.search(primary_key)
                    primary_lookup_reads += primary_result.disk_reads
                    primary_lookup_writes += primary_result.disk_writes
                    primary_lookup_time += primary_result.execution_time_ms

                    if primary_result.data:
                        matching_records.append(primary_result.data)

                total_reads += primary_lookup_reads
                total_writes += primary_lookup_writes
                total_time += primary_lookup_time
            else:
                matching_records = []

            breakdown = {
                "primary_metrics": {"reads": primary_lookup_reads, "writes": primary_lookup_writes, "time_ms": primary_lookup_time},
                "secondary_metrics": {"reads": secondary_result.disk_reads, "writes": secondary_result.disk_writes, "time_ms": secondary_result.execution_time_ms}
            }

            return OperationResult(matching_records, total_time, total_reads, total_writes, operation_breakdown=breakdown)

        else:
            table = table_info["table"]
            field_info = self._get_field_info(table, field_name)
            if not field_info:
                raise ValueError(f"Field {field_name} not found in table {table_name}")

            primary_index = table_info["primary_index"]

            if hasattr(primary_index, 'scan_all'):
                scan_result = primary_index.scan_all()
                all_records = scan_result.data
            else:
                raise NotImplementedError(f"Full scan not supported for {table_info['primary_type']} index")

            matching_records = []
            field_type, _ = field_info

            for record in all_records:
                record_value = getattr(record, field_name, None)
                if record_value is not None:
                    if field_type == "FLOAT":
                        try:
                            if hasattr(record_value, 'decode'):
                                record_value = float(record_value.decode('utf-8').rstrip('\x00'))
                            else:
                                record_value = float(record_value)
                            start_val = float(start_key)
                            end_val = float(end_key)
                            if start_val <= record_value <= end_val:
                                matching_records.append(record)
                        except (ValueError, TypeError):
                            continue
                    elif field_type == "INT":
                        try:
                            if hasattr(record_value, 'decode'):
                                record_value = int(record_value.decode('utf-8').rstrip('\x00'))
                            else:
                                record_value = int(record_value)
                            start_val = int(start_key)
                            end_val = int(end_key)
                            if start_val <= record_value <= end_val:
                                matching_records.append(record)
                        except (ValueError, TypeError):
                            continue
                    else:
                        if hasattr(record_value, 'decode'):
                            record_value = record_value.decode('utf-8').rstrip('\x00').rstrip()
                        else:
                            record_value = str(record_value).rstrip()

                        start_str = start_key.decode('utf-8').rstrip('\x00').rstrip() if hasattr(start_key, 'decode') else str(start_key).rstrip()
                        end_str = end_key.decode('utf-8').rstrip('\x00').rstrip() if hasattr(end_key, 'decode') else str(end_key).rstrip()

                        if start_str <= record_value <= end_str:
                            matching_records.append(record)

            matching_records.sort(key=lambda r: getattr(r, field_name))
            return OperationResult(matching_records, scan_result.execution_time_ms, scan_result.disk_reads, scan_result.disk_writes)

    def delete(self, table_name: str, value, field_name: str = None):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")

        table_info = self.tables[table_name]

        if field_name is None:
            primary_index = table_info["primary_index"]

            search_result = self.search(table_name, value)
            if not search_result.data:
                return OperationResult(False, search_result.execution_time_ms, search_result.disk_reads, search_result.disk_writes)

            record = search_result.data[0]
            primary_key = value

            breakdown = {}
            for fname in table_info["secondary_indexes"].keys():
                breakdown[f"secondary_metrics_{fname}"] = {"reads": 0, "writes": 0, "time_ms": 0}
            breakdown["primary_metrics"] = {"reads": search_result.disk_reads, "writes": search_result.disk_writes, "time_ms": search_result.execution_time_ms}

            total_reads = search_result.disk_reads
            total_writes = search_result.disk_writes
            total_time = search_result.execution_time_ms

            for fname, index_info in table_info["secondary_indexes"].items():
                index_type = index_info["type"]

                if index_type == "INVERTED_TEXT":
                    continue

                secondary_index = index_info["index"]
                secondary_value = getattr(record, fname)
                sec_result = secondary_index.delete(secondary_value, primary_key)

                breakdown[f"secondary_metrics_{fname}"]["reads"] += sec_result.disk_reads
                breakdown[f"secondary_metrics_{fname}"]["writes"] += sec_result.disk_writes
                breakdown[f"secondary_metrics_{fname}"]["time_ms"] += sec_result.execution_time_ms

                total_reads += sec_result.disk_reads
                total_writes += sec_result.disk_writes
                total_time += sec_result.execution_time_ms

            delete_result = primary_index.delete(value)

            breakdown["primary_metrics"]["reads"] += delete_result.disk_reads
            breakdown["primary_metrics"]["writes"] += delete_result.disk_writes
            breakdown["primary_metrics"]["time_ms"] += delete_result.execution_time_ms

            total_reads += delete_result.disk_reads
            total_writes += delete_result.disk_writes
            total_time += delete_result.execution_time_ms

            return OperationResult(delete_result.data, total_time, total_reads, total_writes, delete_result.rebuild_triggered, operation_breakdown=breakdown)

        elif field_name in table_info["secondary_indexes"]:
            primary_index = table_info["primary_index"]
            secondary_index = table_info["secondary_indexes"][field_name]["index"]

            del_result = secondary_index.delete(value)
            deleted_pks = del_result.data if isinstance(del_result.data, list) else []

            breakdown = {}
            for fname in table_info["secondary_indexes"].keys():
                breakdown[f"secondary_metrics_{fname}"] = {"reads": 0, "writes": 0, "time_ms": 0}
            breakdown["primary_metrics"] = {"reads": 0, "writes": 0, "time_ms": 0}

            breakdown[f"secondary_metrics_{field_name}"]["reads"] = del_result.disk_reads
            breakdown[f"secondary_metrics_{field_name}"]["writes"] = del_result.disk_writes
            breakdown[f"secondary_metrics_{field_name}"]["time_ms"] = del_result.execution_time_ms

            if not deleted_pks:
                return OperationResult(0, del_result.execution_time_ms, del_result.disk_reads, del_result.disk_writes, operation_breakdown=breakdown)

            deleted_count = 0
            total_reads = del_result.disk_reads
            total_writes = del_result.disk_writes
            total_time = del_result.execution_time_ms

            for pk in deleted_pks:
                search_result = primary_index.search(pk)
                breakdown["primary_metrics"]["reads"] += search_result.disk_reads
                breakdown["primary_metrics"]["writes"] += search_result.disk_writes
                breakdown["primary_metrics"]["time_ms"] += search_result.execution_time_ms
                total_reads += search_result.disk_reads
                total_writes += search_result.disk_writes
                total_time += search_result.execution_time_ms

                if search_result.data:
                    record = search_result.data

                    for fname, index_info in table_info["secondary_indexes"].items():
                        if fname != field_name:
                            sec_type = index_info["type"]

                            if sec_type == "INVERTED_TEXT":
                                continue

                            sec_index = index_info["index"]
                            sec_value = getattr(record, fname)
                            sec_result = sec_index.delete(sec_value, pk)
                            breakdown[f"secondary_metrics_{fname}"]["reads"] += sec_result.disk_reads
                            breakdown[f"secondary_metrics_{fname}"]["writes"] += sec_result.disk_writes
                            breakdown[f"secondary_metrics_{fname}"]["time_ms"] += sec_result.execution_time_ms
                            total_reads += sec_result.disk_reads
                            total_writes += sec_result.disk_writes
                            total_time += sec_result.execution_time_ms

                    prim_del = primary_index.delete(pk)
                    breakdown["primary_metrics"]["reads"] += prim_del.disk_reads
                    breakdown["primary_metrics"]["writes"] += prim_del.disk_writes
                    breakdown["primary_metrics"]["time_ms"] += prim_del.execution_time_ms
                    total_reads += prim_del.disk_reads
                    total_writes += prim_del.disk_writes
                    total_time += prim_del.execution_time_ms

                    if prim_del.data:
                        deleted_count += 1

            return OperationResult(deleted_count, total_time, total_reads, total_writes, operation_breakdown=breakdown)

        else:
            table = table_info["table"]
            field_info = self._get_field_info(table, field_name)
            if not field_info:
                raise ValueError(f"Field {field_name} not found in table {table_name}")

            primary_index = table_info["primary_index"]

            scan_result = primary_index.scan_all()
            all_records = scan_result.data

            matching_records = []
            field_type, _ = field_info

            for record in all_records:
                record_value = getattr(record, field_name, None)
                if record_value is not None:
                    if hasattr(record_value, 'decode'):
                        record_value = record_value.decode('utf-8').rstrip('\x00').rstrip()
                    else:
                        record_value = str(record_value).rstrip()

                    value_str = value.decode('utf-8').rstrip('\x00').rstrip() if hasattr(value, 'decode') else str(value).rstrip()

                    if record_value == value_str:
                        matching_records.append(record)

            if not matching_records:
                return OperationResult(0, scan_result.execution_time_ms, scan_result.disk_reads, scan_result.disk_writes)

            deleted_count = 0
            total_reads = scan_result.disk_reads
            total_writes = scan_result.disk_writes
            total_time = scan_result.execution_time_ms

            secondary_delete_metrics = {}
            for fname in table_info["secondary_indexes"].keys():
                secondary_delete_metrics[fname] = {"reads": 0, "writes": 0, "time_ms": 0}

            primary_delete_reads = 0
            primary_delete_writes = 0
            primary_delete_time = 0

            for record in matching_records:
                pk = record.get_key()

                for fname, index_info in table_info["secondary_indexes"].items():
                    sec_type = index_info["type"]

                    if sec_type == "INVERTED_TEXT":
                        continue

                    sec_index = index_info["index"]
                    sec_value = getattr(record, fname)
                    sec_result = sec_index.delete(sec_value, pk)
                    secondary_delete_metrics[fname]["reads"] += sec_result.disk_reads
                    secondary_delete_metrics[fname]["writes"] += sec_result.disk_writes
                    secondary_delete_metrics[fname]["time_ms"] += sec_result.execution_time_ms
                    total_reads += sec_result.disk_reads
                    total_writes += sec_result.disk_writes
                    total_time += sec_result.execution_time_ms

                prim_delete = primary_index.delete(pk)
                primary_delete_reads += prim_delete.disk_reads
                primary_delete_writes += prim_delete.disk_writes
                primary_delete_time += prim_delete.execution_time_ms
                total_reads += prim_delete.disk_reads
                total_writes += prim_delete.disk_writes
                total_time += prim_delete.execution_time_ms

                if prim_delete.data:
                    deleted_count += 1

            breakdown = {
                "primary_metrics": {
                    "reads": scan_result.disk_reads + primary_delete_reads,
                    "writes": scan_result.disk_writes + primary_delete_writes,
                    "time_ms": scan_result.execution_time_ms + primary_delete_time
                }
            }

            for fname, metrics in secondary_delete_metrics.items():
                breakdown[f"secondary_metrics_{fname}"] = metrics.copy()

            return OperationResult(deleted_count, total_time, total_reads, total_writes, operation_breakdown=breakdown)

    def range_delete(self, table_name: str, start_key, end_key, field_name: str = None):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")

        table_info = self.tables[table_name]
        primary_index = table_info["primary_index"]

        search_result = self.range_search(table_name, start_key, end_key, field_name)

        if not search_result.data:
            return OperationResult(0, search_result.execution_time_ms, search_result.disk_reads, search_result.disk_writes, operation_breakdown=search_result.operation_breakdown)

        deleted_count = 0
        total_reads = search_result.disk_reads
        total_writes = search_result.disk_writes
        total_time = search_result.execution_time_ms

        secondary_delete_metrics = {}
        for fname in table_info["secondary_indexes"].keys():
            secondary_delete_metrics[fname] = {"reads": 0, "writes": 0, "time_ms": 0}

        primary_delete_reads = 0
        primary_delete_writes = 0
        primary_delete_time = 0

        for record in search_result.data:
            primary_key = record.get_key()

            for fname, index_info in table_info["secondary_indexes"].items():
                sec_type = index_info["type"]

                if sec_type == "INVERTED_TEXT":
                    continue

                sec_index = index_info["index"]
                sec_value = getattr(record, fname)
                sec_result = sec_index.delete(sec_value, primary_key)
                secondary_delete_metrics[fname]["reads"] += sec_result.disk_reads
                secondary_delete_metrics[fname]["writes"] += sec_result.disk_writes
                secondary_delete_metrics[fname]["time_ms"] += sec_result.execution_time_ms
                total_reads += sec_result.disk_reads
                total_writes += sec_result.disk_writes
                total_time += sec_result.execution_time_ms

            prim_delete = primary_index.delete(primary_key)
            primary_delete_reads += prim_delete.disk_reads
            primary_delete_writes += prim_delete.disk_writes
            primary_delete_time += prim_delete.execution_time_ms
            total_reads += prim_delete.disk_reads
            total_writes += prim_delete.disk_writes
            total_time += prim_delete.execution_time_ms

            if prim_delete.data:
                deleted_count += 1

        breakdown = {}

        if field_name is None:
            breakdown["primary_metrics"] = {
                "reads": search_result.disk_reads + primary_delete_reads,
                "writes": search_result.disk_writes + primary_delete_writes,
                "time_ms": search_result.execution_time_ms + primary_delete_time
            }

            for fname, metrics in secondary_delete_metrics.items():
                breakdown[f"secondary_metrics_{fname}"] = metrics.copy()

        elif search_result.operation_breakdown and "secondary_metrics" in search_result.operation_breakdown:
            breakdown[f"secondary_metrics_{field_name}"] = search_result.operation_breakdown["secondary_metrics"].copy()
            breakdown["primary_metrics"] = search_result.operation_breakdown["primary_metrics"].copy()

            if field_name in secondary_delete_metrics:
                breakdown[f"secondary_metrics_{field_name}"]["reads"] += secondary_delete_metrics[field_name]["reads"]
                breakdown[f"secondary_metrics_{field_name}"]["writes"] += secondary_delete_metrics[field_name]["writes"]
                breakdown[f"secondary_metrics_{field_name}"]["time_ms"] += secondary_delete_metrics[field_name]["time_ms"]

            for fname, metrics in secondary_delete_metrics.items():
                if fname != field_name:
                    breakdown[f"secondary_metrics_{fname}"] = metrics.copy()

            breakdown["primary_metrics"]["reads"] += primary_delete_reads
            breakdown["primary_metrics"]["writes"] += primary_delete_writes
            breakdown["primary_metrics"]["time_ms"] += primary_delete_time

        else:
            breakdown["primary_metrics"] = {
                "reads": search_result.disk_reads + primary_delete_reads,
                "writes": search_result.disk_writes + primary_delete_writes,
                "time_ms": search_result.execution_time_ms + primary_delete_time
            }

            for fname, metrics in secondary_delete_metrics.items():
                breakdown[f"secondary_metrics_{fname}"] = metrics.copy()

        return OperationResult(deleted_count, total_time, total_reads, total_writes, operation_breakdown=breakdown if breakdown else None)

    def drop_index(self, table_name: str, field_name: str):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")

        table_info = self.tables[table_name]

        if field_name not in table_info["secondary_indexes"]:
            raise ValueError(f"Index on field '{field_name}' not found in table '{table_name}'")

        secondary_index = table_info["secondary_indexes"][field_name]["index"]
        index_type = table_info["secondary_indexes"][field_name]["type"]

        if hasattr(secondary_index, 'close'):
            try:
                secondary_index.close()
            except Exception:
                pass

        import gc
        gc.collect()

        import shutil
        removed_files = []
        index_dir = os.path.join(self.base_dir, table_name, f"secondary_{index_type.lower()}_{field_name}")
        if os.path.exists(index_dir):
            try:
                shutil.rmtree(index_dir)
                removed_files.append(index_dir)
            except Exception:
                pass

        del table_info["secondary_indexes"][field_name]
        self._save_metadata()
        return removed_files

    def drop_table(self, table_name: str):
        if table_name not in self.tables:
            return False

        table_info = self.tables[table_name]
        removed_files = []

        for field_name, index_info in table_info["secondary_indexes"].items():
            secondary_index = index_info["index"]
            if hasattr(secondary_index, 'close'):
                try:
                    secondary_index.close()
                except Exception:
                    pass
            index_info["index"] = None

        primary_index = table_info["primary_index"]
        if hasattr(primary_index, 'close'):
            try:
                primary_index.close()
            except Exception:
                pass
        table_info["primary_index"] = None

        import gc
        gc.collect()

        import shutil
        table_dir = os.path.join(self.base_dir, table_name)
        if os.path.exists(table_dir):
            try:
                shutil.rmtree(table_dir)
                removed_files.append(table_dir)
            except Exception as e:
                pass

        del self.tables[table_name]
        self._save_metadata()
        return removed_files

    def get_table_info(self, table_name: str):
        if table_name not in self.tables:
            return None

        table_info = self.tables[table_name]
        return {
            "table_name": table_name,
            "primary_type": table_info["primary_type"],
            "secondary_indexes": {
                field_name: index_info["type"]
                for field_name, index_info in table_info["secondary_indexes"].items()
            },
            "field_count": len(table_info["table"].all_fields)
        }

    def list_tables(self):
        return list(self.tables.keys())

    def get_database_stats(self):
        stats = {
            "database_name": self.database_name,
            "table_count": len(self.tables),
            "tables": {}
        }

        for table_name, table_info in self.tables.items():
            table_stats = {
                "primary_type": table_info["primary_type"],
                "secondary_count": len(table_info["secondary_indexes"]),
                "secondary_types": list(table_info["secondary_indexes"].values())
            }

            try:
                primary_index = table_info["primary_index"]
                if hasattr(primary_index, 'scan_all'):
                    scan_result = primary_index.scan_all()
                    table_stats["record_count"] = len(scan_result.data) if scan_result.data else 0
                else:
                    table_stats["record_count"] = 0
            except:
                table_stats["record_count"] = 0

            stats["tables"][table_name] = table_stats

        return stats

    def _validate_primary_index(self, index_type: str):
        return self.INDEX_TYPES.get(index_type, {}).get("primary", False)

    def _validate_secondary_index(self, index_type: str):
        return self.INDEX_TYPES.get(index_type, {}).get("secondary", False)

    def _get_field_info(self, table: Table, field_name: str):
        for fname, ftype, fsize in table.all_fields:
            if fname == field_name:
                return (ftype, fsize)
        return None

    def _create_primary_index(self, table: Table, index_type: str):
        if index_type == "ISAM":

            primary_dir = os.path.join(self.base_dir, table.table_name, f"primary_isam_{table.key_field}")
            os.makedirs(primary_dir, exist_ok=True)
            primary_filename = os.path.join(primary_dir, "datos.dat")

            return ISAMPrimaryIndex(table, primary_filename)

        elif index_type == "SEQUENTIAL":

            primary_dir = os.path.join(self.base_dir, table.table_name, f"primary_sequential_{table.key_field}")
            os.makedirs(primary_dir, exist_ok=True)
            main_filename = os.path.join(primary_dir, "main.dat")
            aux_filename = os.path.join(primary_dir, "aux.dat")

            extra_fields = {"active": ("BOOL", 1)}
            table_with_active = Table(
                table_name=table.table_name,
                sql_fields=table.sql_fields,
                key_field=table.key_field,
                extra_fields=extra_fields
            )
            return SequentialFile(main_filename, aux_filename, table_with_active)

        elif index_type == "BTREE":
            primary_dir = os.path.join(self.base_dir, table.table_name, f"primary_btree_{table.key_field}")
            os.makedirs(primary_dir, exist_ok=True)
            primary_filename = os.path.join(primary_dir, "btree_clustered")
            return BPlusTreeClusteredIndex(
                order=50,
                key_column=table.key_field,
                file_path=primary_filename,
                record_class=Record,
                table=table
            )


        raise NotImplementedError(f"Primary index type {index_type} not implemented yet")

    def _create_secondary_index(self, table: Table, field_name: str, index_type: str, language: str = "spanish", feature_type: str = "SIFT", multimedia_directory: str = None, multimedia_pattern: str = None):
        field_type, field_size = self._get_field_info(table, field_name)

        if index_type == "BTREE":
            secondary_dir = os.path.join(self.base_dir, table.table_name, f"secondary_btree_{field_name}")
            os.makedirs(secondary_dir, exist_ok=True)

            filename = os.path.join(secondary_dir, "btree_unclustered")

            return BPlusTreeUnclusteredIndex(
                order=50,
                index_column=field_name,
                file_path=filename
            )
        
        elif index_type == "HASH":

            secondary_dir = os.path.join(self.base_dir, table.table_name, f"secondary_hash_{field_name}")
            os.makedirs(secondary_dir, exist_ok=True)

            data_filename = os.path.join(secondary_dir, "datos")

            return ExtendibleHashing(data_filename, field_name, field_type, field_size, is_primary=False)
        
        elif index_type == "RTREE":
            secondary_dir = os.path.join(self.base_dir, table.table_name, f"secondary_rtree_{field_name}")
            os.makedirs(secondary_dir, exist_ok=True)

            if field_type != "ARRAY":
                raise ValueError(f"R-Tree indexes require ARRAY fields (spatial coordinates), got {field_type}")

            dimension = field_size
            filename = os.path.join(secondary_dir, f"{field_name}_rtree")

            from ..r_tree.r_tree import RTreeSecondaryIndex
            return RTreeSecondaryIndex(field_name, filename, dimension=dimension)
        
        elif index_type == "INVERTED_TEXT":
            if field_type not in ["VARCHAR", "TEXT", "STRING", "CHAR"]:
                raise ValueError(f"INVERTED_TEXT index requires text field, got {field_type}")

            secondary_dir = os.path.join(self.base_dir, table.table_name, f"secondary_inverted_text_{field_name}")
            os.makedirs(secondary_dir, exist_ok=True)

            from ..inverted_index.inverted_index_text import InvertedTextIndex
            return InvertedTextIndex(
                index_dir=secondary_dir,
                field_name=field_name,
                language=language
            )

        elif index_type == "MULTIMEDIA_SEQ":
            if field_type not in ["VARCHAR", "TEXT", "STRING", "CHAR"]:
                raise ValueError(f"MULTIMEDIA_SEQ index requires text field for filename, got {field_type}")

            secondary_dir = os.path.join(self.base_dir, table.table_name, f"secondary_multimedia_seq_{field_name}")
            os.makedirs(secondary_dir, exist_ok=True)

            if multimedia_directory:
                if not os.path.isabs(multimedia_directory):
                    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                    files_dir = os.path.join(project_root, multimedia_directory)
                else:
                    files_dir = multimedia_directory

                if not os.path.exists(files_dir):
                    raise ValueError(f"Multimedia directory not found: {files_dir}")
            else:
                files_dir = os.path.join(self.base_dir, table.table_name, f"{field_name}_files")
                os.makedirs(files_dir, exist_ok=True)

            from ..multimedia_index.multimedia_sequential import MultimediaSequential
            return MultimediaSequential(
                index_dir=secondary_dir,
                files_dir=files_dir,
                field_name=field_name,
                feature_type=feature_type,
                filename_pattern=multimedia_pattern
            )

        elif index_type == "MULTIMEDIA_INV":
            if field_type not in ["VARCHAR", "TEXT", "STRING", "CHAR"]:
                raise ValueError(f"MULTIMEDIA_INV index requires text field for filename, got {field_type}")

            secondary_dir = os.path.join(self.base_dir, table.table_name, f"secondary_multimedia_inv_{field_name}")
            os.makedirs(secondary_dir, exist_ok=True)

            if multimedia_directory:
                if not os.path.isabs(multimedia_directory):
                    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                    files_dir = os.path.join(project_root, multimedia_directory)
                else:
                    files_dir = multimedia_directory

                if not os.path.exists(files_dir):
                    raise ValueError(f"Multimedia directory not found: {files_dir}")
            else:
                files_dir = os.path.join(self.base_dir, table.table_name, f"{field_name}_files")
                os.makedirs(files_dir, exist_ok=True)

            from ..multimedia_index.multimedia_inverted import MultimediaInverted
            return MultimediaInverted(
                index_dir=secondary_dir,
                files_dir=files_dir,
                field_name=field_name,
                feature_type=feature_type,
                filename_pattern=multimedia_pattern
            )

        raise NotImplementedError(f"Secondary index type {index_type} not implemented yet")

    def get_last_operation_metrics(self, table_name: str, index_type: str = "primary", field_name: str = None):
        if table_name not in self.tables:
            return None

        table_info = self.tables[table_name]

        if index_type == "primary":
            index = table_info["primary_index"]
        elif index_type == "secondary" and field_name:
            if field_name not in table_info["secondary_indexes"]:
                return None
            index = table_info["secondary_indexes"][field_name]["index"]
        else:
            return None

        if hasattr(index, 'performance') and hasattr(index.performance, 'last_result'):
            return index.performance.last_result
        return None

    def extract_metrics_from_result(self, result):
        if isinstance(result, OperationResult):
            return {
                "execution_time_ms": result.execution_time_ms,
                "disk_reads": result.disk_reads,
                "disk_writes": result.disk_writes,
                "total_disk_accesses": result.total_disk_accesses,
                "data": result.data
            }
        return {"data": result, "execution_time_ms": 0, "disk_reads": 0, "disk_writes": 0, "total_disk_accesses": 0}

    def print_operation_summary(self, result, operation_name: str = "Operation"):
        if isinstance(result, OperationResult):
            print(f"{operation_name} completed:")
            print(f"  Time: {result.execution_time_ms:.2f} ms")
            print(f"  Disk accesses: {result.total_disk_accesses} (R:{result.disk_reads}, W:{result.disk_writes})")
        else:
            print(f"{operation_name} completed (no metrics available)")

    def scan_all(self, table_name: str):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")

        table_info = self.tables[table_name]
        primary_index = table_info["primary_index"]

        return primary_index.scan_all()

    def warm_up_indexes(self, table_name: str):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")

        table_info = self.tables[table_name]
        primary_index = table_info["primary_index"]

        if hasattr(primary_index, 'warm_up'):
            primary_index.warm_up()

        for index_info in table_info["secondary_indexes"].values():
            secondary_index = index_info["index"]
            if hasattr(secondary_index, 'warm_up'):
                secondary_index.warm_up()

    def _save_metadata(self):
        metadata = {}
        for table_name, table_info in self.tables.items():
            table = table_info["table"]
            metadata[table_name] = {
                "primary_type": table_info["primary_type"],
                "fields": [(name, dtype, size) for name, dtype, size in table.sql_fields],
                "key_field": table.key_field,
                "secondary_indexes": {
                    field: {
                        "type": info["type"],
                        "language": info.get("language"),
                        "feature_type": info.get("feature_type"),
                        "multimedia_directory": info.get("multimedia_directory"),
                        "multimedia_pattern": info.get("multimedia_pattern")
                    }
                    for field, info in table_info["secondary_indexes"].items()
                }
            }

        with open(self.metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)

    def _load_metadata(self):
        if not os.path.exists(self.metadata_file):
            return

        try:
            with open(self.metadata_file, 'r') as f:
                metadata = json.load(f)

            for table_name, table_meta in metadata.items():
                try:
                    table_dir = os.path.join(self.base_dir, table_name)
                    if not os.path.exists(table_dir):
                        continue

                    fields = [tuple(f) for f in table_meta["fields"]]
                    table = Table(
                        table_name=table_name,
                        sql_fields=fields,
                        key_field=table_meta["key_field"]
                    )

                    primary_type = table_meta["primary_type"]

                    if primary_type == "SEQUENTIAL":
                        extra_fields = {"active": ("BOOL", 1)}
                        table = Table(
                            table_name=table_name,
                            sql_fields=fields,
                            key_field=table_meta["key_field"],
                            extra_fields=extra_fields
                        )

                    primary_index = self._create_primary_index(table, primary_type)

                    table_info = {
                        "table": table,
                        "primary_index": primary_index,
                        "secondary_indexes": {},
                        "primary_type": primary_type
                    }

                    for field_name, index_info in table_meta.get("secondary_indexes", {}).items():
                        try:
                            index_type = index_info["type"]
                            language = index_info.get("language", "spanish")
                            feature_type = index_info.get("feature_type", "SIFT")
                            multimedia_directory = index_info.get("multimedia_directory", None)
                            multimedia_pattern = index_info.get("multimedia_pattern", None)

                            secondary_index = self._create_secondary_index(table, field_name, index_type, language=language, feature_type=feature_type, multimedia_directory=multimedia_directory, multimedia_pattern=multimedia_pattern)
                            table_info["secondary_indexes"][field_name] = {
                                "index": secondary_index,
                                "type": index_type,
                                "language": language if index_type == "INVERTED_TEXT" else None,
                                "feature_type": feature_type if index_type in ("MULTIMEDIA_SEQ", "MULTIMEDIA_INV") else None,
                                "multimedia_directory": multimedia_directory if index_type in ("MULTIMEDIA_SEQ", "MULTIMEDIA_INV") else None,
                                "multimedia_pattern": multimedia_pattern if index_type in ("MULTIMEDIA_SEQ", "MULTIMEDIA_INV") else None
                            }
                            if hasattr(secondary_index, 'warm_up'):
                                secondary_index.warm_up()
                        except Exception:
                            pass

                    self.tables[table_name] = table_info

                    if hasattr(primary_index, 'warm_up'):
                        primary_index.warm_up()
                except Exception:
                    continue
        except Exception:
            pass

