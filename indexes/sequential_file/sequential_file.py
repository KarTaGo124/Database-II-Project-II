import os
import math
from typing import List, Optional, Any
from ..core.record import Record, Table
from ..core.performance_tracker import PerformanceTracker

class SequentialFile:
    def __init__(self, main_file: str, aux_file: str, table: Table, k_rec: Optional[int] = None):
        self.main_file = main_file
        self.aux_file = aux_file
        self.table = table
        self.list_of_types = table.all_fields
        self.key_field = table.key_field
        self.record_size = table.record_size
        self.k = k_rec if k_rec is not None else 10
        self.deleted_count = 0
        self.total_records = 0
        self.performance = PerformanceTracker()

        if not any(field[0] == 'active' for field in self.list_of_types):
            raise ValueError("La tabla debe tener un campo 'active' de tipo BOOL en extra_fields")

        if not os.path.exists(self.main_file):
            open(self.main_file, 'wb').close()
        if not os.path.exists(self.aux_file):
            open(self.aux_file, 'wb').close()

    def update_k_dynamically(self) -> int:
        if self.total_records > 0:
            new_k = max(1, int(math.log2(self.total_records)))
            self.k = new_k
            return new_k
        return self.k

    def get_file_size(self, filename: str) -> int:
        if not os.path.exists(filename):
            return 0
        file_size = os.path.getsize(filename)
        return file_size // self.record_size


    def rebuild(self):
        self.performance.start_operation()

        scan_result = self.scan_all()
        records = scan_result.data
        records.sort(key=lambda r: r.get_key())

        if os.path.exists(self.aux_file):
            os.remove(self.aux_file)

        with open(self.main_file, 'wb') as f:
            for record in records:
                self.performance.track_write()
                f.write(record.pack())

        open(self.aux_file, 'wb').close()

        self.deleted_count = 0
        self.total_records = len(records)
        self.update_k_dynamically()

        return self.performance.end_operation(True)

    def insert(self, record: Record):
        self.performance.start_operation()

        existing_result = self.search(record.get_key())
        if existing_result.data is not None:
            raise ValueError(f"Record con clave {record.get_key()} ya existe")

        record.active = True
        with open(self.aux_file, 'ab') as f:
            f.write(record.pack())
            self.performance.track_write()

        self.total_records += 1

        aux_size = self.get_file_size(self.aux_file)
        rebuild_triggered = aux_size > self.k
        if rebuild_triggered:
            self.rebuild()

        return self.performance.end_operation(True, rebuild_triggered)



    def delete(self, key: Any):
        self.performance.start_operation()

        main_size = self.get_file_size(self.main_file)
        if main_size > 0:
            with open(self.main_file, 'r+b') as f:
                left, right = 0, main_size - 1

                while left <= right:
                    mid = (left + right) // 2
                    f.seek(mid * self.record_size)
                    data = f.read(self.record_size)
                    self.performance.track_read()

                    if not data:
                        break

                    rec = Record.unpack(data, self.list_of_types, self.key_field)
                    rec_key = rec.get_key()

                    if rec_key == key:
                        if rec.active:
                            rec.active = False
                            f.seek(mid * self.record_size)
                            f.write(rec.pack())
                            self.performance.track_write()
                            self.deleted_count += 1

                            rebuild_triggered = self.total_records > 0 and self.deleted_count > (self.total_records * 0.1)
                            f.close()

                            if rebuild_triggered:
                                self.rebuild()

                            return self.performance.end_operation(True, rebuild_triggered)
                        else:
                            return self.performance.end_operation(False)
                    elif rec_key < key:
                        left = mid + 1
                    else:
                        right = mid - 1

        if os.path.exists(self.aux_file):
            with open(self.aux_file, 'r+b') as f:
                i = 0
                while data := f.read(self.record_size):
                    self.performance.track_read()
                    rec = Record.unpack(data, self.list_of_types, self.key_field)
                    if rec.get_key() == key:
                        if rec.active:
                            rec.active = False
                            f.seek(i * self.record_size)
                            f.write(rec.pack())
                            self.performance.track_write()
                            self.deleted_count += 1

                            rebuild_triggered = self.total_records > 0 and self.deleted_count > (self.total_records * 0.1)
                            f.close()

                            if rebuild_triggered:
                                self.rebuild()

                            return self.performance.end_operation(True, rebuild_triggered)
                        else:
                            return self.performance.end_operation(False)
                    i += 1

        return self.performance.end_operation(False)

    def search(self, key):
        self.performance.start_operation()

        main_size = self.get_file_size(self.main_file)
        if main_size > 0:
            with open(self.main_file, 'rb') as f:
                left, right = 0, main_size - 1

                while left <= right:
                    mid = (left + right) // 2
                    f.seek(mid * self.record_size)
                    data = f.read(self.record_size)
                    self.performance.track_read()

                    if not data:
                        break

                    rec = Record.unpack(data, self.list_of_types, self.key_field)
                    rec_key = rec.get_key()

                    if rec_key == key:
                        if rec.active:
                            return self.performance.end_operation(rec)
                        else:
                            return self.performance.end_operation(None)
                    elif rec_key < key:
                        left = mid + 1
                    else:
                        right = mid - 1

        if os.path.exists(self.aux_file):
            with open(self.aux_file, 'rb') as f:
                while True:
                    data = f.read(self.record_size)
                    if not data:
                        break

                    self.performance.track_read()
                    rec = Record.unpack(data, self.list_of_types, self.key_field)

                    if rec.get_key() == key:
                        if rec.active:
                            return self.performance.end_operation(rec)
                        else:
                            return self.performance.end_operation(None)

        return self.performance.end_operation(None)


    def range_search(self, begin_key, end_key):
        self.performance.start_operation()

        results = []
        main_size = self.get_file_size(self.main_file)

        if main_size > 0:
            start_pos = 0
            with open(self.main_file, 'rb') as f:
                left, right = 0, main_size - 1
                while left <= right:
                    mid = (left + right) // 2
                    f.seek(mid * self.record_size)
                    data = f.read(self.record_size)
                    if data:
                        self.performance.track_read()
                        rec = Record.unpack(data, self.list_of_types, self.key_field)
                        if rec.get_key() >= begin_key:
                            start_pos = mid
                            right = mid - 1
                        else:
                            left = mid + 1

            with open(self.main_file, 'rb') as f:
                f.seek(start_pos * self.record_size)
                for i in range(start_pos, main_size):
                    data = f.read(self.record_size)
                    if not data:
                        break
                    self.performance.track_read()
                    rec = Record.unpack(data, self.list_of_types, self.key_field)
                    if rec.active and begin_key <= rec.get_key() <= end_key:
                        results.append(rec)
                    elif rec.get_key() > end_key:
                        break

        if os.path.exists(self.aux_file):
            with open(self.aux_file, 'rb') as f:
                while data := f.read(self.record_size):
                    self.performance.track_read()
                    rec = Record.unpack(data, self.list_of_types, self.key_field)
                    if rec.active and begin_key <= rec.get_key() <= end_key:
                        results.append(rec)

        results.sort(key=lambda r: r.get_key())
        return self.performance.end_operation(results)

    def scan_all(self):
        self.performance.start_operation()

        records = []

        with open(self.main_file, 'rb') as f:
            while data := f.read(self.record_size):
                self.performance.track_read()
                rec = Record.unpack(data, self.list_of_types, self.key_field)
                if rec.active:
                    records.append(rec)

        if os.path.exists(self.aux_file):
            with open(self.aux_file, 'rb') as f:
                while data := f.read(self.record_size):
                    self.performance.track_read()
                    rec = Record.unpack(data, self.list_of_types, self.key_field)
                    if rec.active:
                        records.append(rec)

        return self.performance.end_operation(records)

    def drop_table(self):
        removed_files = []
        if os.path.exists(self.main_file):
            os.remove(self.main_file)
            removed_files.append(self.main_file)
        if os.path.exists(self.aux_file):
            os.remove(self.aux_file)
            removed_files.append(self.aux_file)
        return removed_files

