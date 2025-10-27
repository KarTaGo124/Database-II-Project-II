import os, struct
from typing import Any, Optional
from ..core.record import Record, Table
from ..core.performance_tracker import PerformanceTracker

BLOCK_FACTOR = 30
ROOT_INDEX_BLOCK_FACTOR = 50
LEAF_INDEX_BLOCK_FACTOR = 50
CONSOLIDATION_THRESHOLD = BLOCK_FACTOR // 3


class Page:
    HEADER_FORMAT = 'ii'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    def __init__(self, records=None, next_page=-1, block_factor=BLOCK_FACTOR, record_size=None):
        self.records = records if records else []
        self.next_page = next_page
        self.block_factor = block_factor
        self.record_size = record_size
        self.SIZE_OF_PAGE = self.HEADER_SIZE + self.block_factor * self.record_size if record_size else None

    def pack(self):
        header_data = struct.pack(self.HEADER_FORMAT, len(self.records), self.next_page)
        record_data = b''.join(r.pack() for r in self.records)
        record_data += b'\x00' * (self.record_size * (self.block_factor - len(self.records)))
        return header_data + record_data

    @staticmethod
    def unpack(data: bytes, block_factor: int = BLOCK_FACTOR, record_size: Optional[int] = None, table: Optional[Table] = None):
        size, next_page = struct.unpack(Page.HEADER_FORMAT, data[:Page.HEADER_SIZE])
        offset = Page.HEADER_SIZE
        records = []
        for _ in range(size):
            record_data = data[offset: offset + record_size]
            records.append(Record.unpack(record_data, table.all_fields, table.key_field))
            offset += record_size
        return Page(records, next_page, block_factor, record_size)
    
    def insert_sorted(self, record: Record):
        left, right = 0, len(self.records)
        while left < right:
            mid = (left + right) // 2
            if self.records[mid].get_key() == record.get_key():
                raise ValueError(f"Primary key {record.get_key()} already exists in page")
            elif self.records[mid].get_key() < record.get_key():
                left = mid + 1
            else:
                right = mid
        self.records.insert(left, record)

    def is_full(self):
        return len(self.records) >= self.block_factor
    
    def remove_record(self, key_value: Any):
        left, right = 0, len(self.records) - 1

        while left <= right:
            mid = (left + right) // 2
            if self.records[mid].get_key() == key_value:
                del self.records[mid]
                return True
            elif self.records[mid].get_key() < key_value:
                left = mid + 1
            else:
                right = mid - 1

        return False
    
    def is_empty(self):
        return len(self.records) == 0
    
    def can_merge_with(self, other_page):
        return len(self.records) + len(other_page.records) <= self.block_factor
    
    def merge_with(self, other_page):
        all_records = self.records + other_page.records
        all_records.sort(key=lambda r: r.get_key())
        self.records = all_records


class RootIndexEntry:
    FORMAT = "ii"
    SIZE = struct.calcsize(FORMAT)
    
    def __init__(self, key: int, leaf_page_number: int):
        self.key = key
        self.leaf_page_number = leaf_page_number
    
    def pack(self) -> bytes:
        return struct.pack(self.FORMAT, self.key, self.leaf_page_number)
    
    @staticmethod
    def unpack(data: bytes):
        key, leaf_page_number = struct.unpack(RootIndexEntry.FORMAT, data)
        return RootIndexEntry(key, leaf_page_number)
    
    def __str__(self):
        return f"RootKey: {self.key} -> LeafPage: {self.leaf_page_number}"


class RootIndex:
    HEADER_FORMAT = 'ii'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, entries=None, next_page=-1, root_index_block_factor=ROOT_INDEX_BLOCK_FACTOR):
        self.entries = entries if entries else []
        self.next_page = next_page
        self.root_index_block_factor = root_index_block_factor
        self.SIZE_OF_ROOT_INDEX = self.HEADER_SIZE + self.root_index_block_factor * RootIndexEntry.SIZE

    def pack(self):
        header_data = struct.pack(self.HEADER_FORMAT, len(self.entries), self.next_page)
        entries_data = b''.join(entry.pack() for entry in self.entries)
        entries_data += b'\x00' * (RootIndexEntry.SIZE * (self.root_index_block_factor - len(self.entries)))
        return header_data + entries_data

    @staticmethod
    def unpack(data: bytes, root_index_block_factor=ROOT_INDEX_BLOCK_FACTOR):
        size, next_page = struct.unpack(RootIndex.HEADER_FORMAT, data[:RootIndex.HEADER_SIZE])
        offset = RootIndex.HEADER_SIZE
        entries = []
        for _ in range(size):
            entry_data = data[offset: offset + RootIndexEntry.SIZE]
            entries.append(RootIndexEntry.unpack(entry_data))
            offset += RootIndexEntry.SIZE
        return RootIndex(entries, next_page, root_index_block_factor)

    def insert_sorted(self, entry):
        left, right = 0, len(self.entries)
        while left < right:
            mid = (left + right) // 2
            if self.entries[mid].key < entry.key:
                left = mid + 1
            else:
                right = mid
        self.entries.insert(left, entry)

    def is_full(self):
        return len(self.entries) >= self.root_index_block_factor

    def find_leaf_page_for_key(self, key):
        if not self.entries:
            return 0
        
        left = 0
        right = len(self.entries) - 1
        result_page = 0 
        
        while left <= right:
            mid = (left + right) // 2
            mid_key = self.entries[mid].key
            
            if key < mid_key:
                right = mid - 1
            elif key >= mid_key:
                result_page = self.entries[mid].leaf_page_number
                left = mid + 1
            
        return result_page


class LeafIndexEntry:
    FORMAT = "ii"
    SIZE = struct.calcsize(FORMAT)
    
    def __init__(self, key: int, data_page_number: int):
        self.key = key
        self.data_page_number = data_page_number
    
    def pack(self) -> bytes:
        return struct.pack(self.FORMAT, self.key, self.data_page_number)
    
    @staticmethod
    def unpack(data: bytes):
        key, data_page_number = struct.unpack(LeafIndexEntry.FORMAT, data)
        return LeafIndexEntry(key, data_page_number)
    
    def __str__(self):
        return f"LeafKey: {self.key} -> DataPage: {self.data_page_number}"


class LeafIndex:
    HEADER_FORMAT = 'ii'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, entries=None, next_page=-1, leaf_index_block_factor=LEAF_INDEX_BLOCK_FACTOR):
        self.entries = entries if entries else []
        self.next_page = next_page
        self.leaf_index_block_factor = leaf_index_block_factor
        self.SIZE_OF_LEAF_INDEX = self.HEADER_SIZE + self.leaf_index_block_factor * LeafIndexEntry.SIZE

    def pack(self):
        header_data = struct.pack(self.HEADER_FORMAT, len(self.entries), self.next_page)
        entries_data = b''.join(entry.pack() for entry in self.entries)
        entries_data += b'\x00' * (LeafIndexEntry.SIZE * (self.leaf_index_block_factor - len(self.entries)))
        return header_data + entries_data

    @staticmethod
    def unpack(data: bytes, leaf_index_block_factor=LEAF_INDEX_BLOCK_FACTOR):
        size, next_page = struct.unpack(LeafIndex.HEADER_FORMAT, data[:LeafIndex.HEADER_SIZE])
        offset = LeafIndex.HEADER_SIZE
        entries = []
        for _ in range(size):
            entry_data = data[offset: offset + LeafIndexEntry.SIZE]
            entries.append(LeafIndexEntry.unpack(entry_data))
            offset += LeafIndexEntry.SIZE
        return LeafIndex(entries, next_page, leaf_index_block_factor)

    def insert_sorted(self, entry):
        left, right = 0, len(self.entries)
        while left < right:
            mid = (left + right) // 2
            if self.entries[mid].key < entry.key:
                left = mid + 1
            else:
                right = mid
        self.entries.insert(left, entry)

    def is_full(self):
        return len(self.entries) >= self.leaf_index_block_factor

    def find_data_page_for_key(self, key):
        if not self.entries:
            return 0

        left = 0
        right = len(self.entries) - 1
        result_page = 0

        while left <= right:
            mid = (left + right) // 2
            mid_key = self.entries[mid].key

            if key < mid_key:
                right = mid - 1
            elif key >= mid_key:
                result_page = self.entries[mid].data_page_number
                left = mid + 1

        return result_page


class FreeListStack:
    def __init__(self, free_list_file="free_list.dat"):
        self.free_list_file = free_list_file

    def push_free_page(self, page_num):
        try:
            if not os.path.exists(self.free_list_file):
                count = 0
            else:
                with open(self.free_list_file, "rb") as file:
                    count_data = file.read(4)
                    count = struct.unpack('i', count_data)[0] if count_data else 0

            with open(self.free_list_file, "r+b" if count > 0 else "wb") as file:
                file.seek(0)
                file.write(struct.pack('i', count + 1))
                file.seek(0, 2)
                file.write(struct.pack('i', page_num))
            return True
        except:
            return False
    
    def pop_free_page(self):
        if not os.path.exists(self.free_list_file):
            return None

        try:
            with open(self.free_list_file, "r+b") as file:
                count_data = file.read(4)
                if len(count_data) < 4:
                    return None

                count = struct.unpack('i', count_data)[0]
                if count <= 0:
                    return None

                file.seek(4 + (count - 1) * 4)
                page_num = struct.unpack('i', file.read(4))[0]

                file.seek(0)
                file.write(struct.pack('i', count - 1))

                return page_num
        except:
            return None
    
    def get_free_count(self):
        if not os.path.exists(self.free_list_file):
            return 0

        try:
            with open(self.free_list_file, "rb") as file:
                count_data = file.read(4)
                return struct.unpack('i', count_data)[0] if count_data else 0
        except:
            return 0
    
    def clear(self):
        if os.path.exists(self.free_list_file):
            os.remove(self.free_list_file)
    
    def is_empty(self):
        return self.get_free_count() == 0


class ISAMPrimaryIndex:
    HEADER_FORMAT = 'i'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    DATA_START_OFFSET = HEADER_SIZE

    def __init__(self, table: Table, filename: str = "datos.dat", root_index_file: Optional[str] = None, leaf_index_file: Optional[str] = None, free_list_file: Optional[str] = None,
                 block_factor: Optional[int] = None, root_index_block_factor: Optional[int] = None, leaf_index_block_factor: Optional[int] = None, consolidation_threshold: Optional[int] = None):
        self.table = table
        self.filename = filename
        # Create record template once for consistent access to RECORD_SIZE
        self.record_template = table.record

        base_dir = os.path.dirname(filename)

        if root_index_file is None:
            root_index_file = os.path.join(base_dir, "root_index.dat")
        if leaf_index_file is None:
            leaf_index_file = os.path.join(base_dir, "leaf_index.dat")
        if free_list_file is None:
            free_list_file = os.path.join(base_dir, "free_list.dat")

        self.root_index_file = root_index_file
        self.leaf_index_file = leaf_index_file
        self.free_list_stack = FreeListStack(free_list_file)

        self.block_factor = block_factor if block_factor is not None else BLOCK_FACTOR
        self.root_index_block_factor = root_index_block_factor if root_index_block_factor is not None else ROOT_INDEX_BLOCK_FACTOR
        self.leaf_index_block_factor = leaf_index_block_factor if leaf_index_block_factor is not None else LEAF_INDEX_BLOCK_FACTOR
        self.consolidation_threshold = consolidation_threshold if consolidation_threshold is not None else CONSOLIDATION_THRESHOLD

        self.next_page_number = 0
        self.next_root_index_page_number = 0
        self.next_leaf_index_page_number = 0
        self.performance = PerformanceTracker()

    def _create_initial_files(self, record: Record):
        with open(self.filename, "wb") as file:
            self.performance.track_write()
            file.write(struct.pack(self.HEADER_FORMAT, 0))
            page = Page([record], block_factor=self.block_factor, record_size=self.record_template.RECORD_SIZE)
            self.performance.track_write()
            file.write(page.pack())

        with open(self.leaf_index_file, "wb") as file:
            initial_entry = LeafIndexEntry(record.get_key(), 0)
            leaf_index = LeafIndex([initial_entry], leaf_index_block_factor=self.leaf_index_block_factor)
            self.performance.track_write()
            file.write(leaf_index.pack())

        with open(self.root_index_file, "wb") as file:
            root_index = RootIndex([], root_index_block_factor=self.root_index_block_factor)
            self.performance.track_write()
            file.write(root_index.pack())

        self.free_list_stack.clear()

        self.next_page_number = 1
        self.next_leaf_index_page_number = 1
        self.next_root_index_page_number = 1

    # Manejo de la free list
    
    def _push_free_page(self, page_num):
        return self.free_list_stack.push_free_page(page_num)

    def _pop_free_page(self):
        return self.free_list_stack.pop_free_page()

    def _get_free_count(self):
        return self.free_list_stack.get_free_count()

    # Escritura y lectura de páginas e índices
    
    def _read_page(self, file, page_num):
        page_size = Page.HEADER_SIZE + self.block_factor * self.record_template.RECORD_SIZE
        offset = self.DATA_START_OFFSET + (page_num * page_size)
        file.seek(offset)
        self.performance.track_read()
        return Page.unpack(file.read(page_size), self.block_factor, self.record_template.RECORD_SIZE, self.table)

    def _write_page(self, file, page_num, page):
        page_size = Page.HEADER_SIZE + self.block_factor * self.record_template.RECORD_SIZE
        offset = self.DATA_START_OFFSET + (page_num * page_size)
        file.seek(offset)
        self.performance.track_write()
        file.write(page.pack())

    def _read_root_index(self, file, page_num):
        root_size = RootIndex.HEADER_SIZE + self.root_index_block_factor * RootIndexEntry.SIZE
        file.seek(page_num * root_size)
        self.performance.track_read()
        return RootIndex.unpack(file.read(root_size), self.root_index_block_factor)

    def _write_root_index(self, file, page_num, root_index):
        root_size = RootIndex.HEADER_SIZE + self.root_index_block_factor * RootIndexEntry.SIZE
        file.seek(page_num * root_size)
        self.performance.track_write()
        file.write(root_index.pack())

    def _read_leaf_index(self, file, page_num):
        leaf_size = LeafIndex.HEADER_SIZE + self.leaf_index_block_factor * LeafIndexEntry.SIZE
        file.seek(page_num * leaf_size)
        self.performance.track_read()
        return LeafIndex.unpack(file.read(leaf_size), self.leaf_index_block_factor)

    def _write_leaf_index(self, file, page_num, leaf_index):
        leaf_size = LeafIndex.HEADER_SIZE + self.leaf_index_block_factor * LeafIndexEntry.SIZE
        file.seek(page_num * leaf_size)
        self.performance.track_write()
        file.write(leaf_index.pack())


    # Operaciones intermedias
    
    def _find_target_leaf_page(self, key_value):
        if not os.path.exists(self.root_index_file):
            return 0

        with open(self.root_index_file, "rb") as file:
            root_index = self._read_root_index(file, 0)
            return root_index.find_leaf_page_for_key(key_value)

    def _find_leaf_page_range_for_keys(self, begin_key, end_key):
        if not os.path.exists(self.root_index_file) or not os.path.exists(self.leaf_index_file):
            return 0, 0

        with open(self.root_index_file, "rb") as root_file:
            root_index = self._read_root_index(root_file, 0)

            if not root_index.entries:
                return 0, 0

            start_leaf = 0
            end_leaf = 0

            with open(self.leaf_index_file, "rb") as leaf_file:
                file_size = os.path.getsize(self.leaf_index_file)
                leaf_size = LeafIndex.HEADER_SIZE + self.leaf_index_block_factor * LeafIndexEntry.SIZE
                num_leaf_pages = file_size // leaf_size

                for i in range(num_leaf_pages):
                    leaf_index = self._read_leaf_index(leaf_file, i)
                    if not leaf_index.entries:
                        continue

                    min_key = leaf_index.entries[0].key
                    max_key = leaf_index.entries[-1].key

                    if max_key >= begin_key:
                        start_leaf = i
                        break

                for i in range(num_leaf_pages - 1, -1, -1):
                    leaf_index = self._read_leaf_index(leaf_file, i)
                    if not leaf_index.entries:
                        continue

                    min_key = leaf_index.entries[0].key
                    max_key = leaf_index.entries[-1].key

                    if min_key <= end_key:
                        end_leaf = i
                        break

            return start_leaf, end_leaf

    def _find_target_data_page(self, key_value, leaf_page_num):
        if not os.path.exists(self.leaf_index_file):
            return 0

        with open(self.leaf_index_file, "rb") as file:
            leaf_index = self._read_leaf_index(file, leaf_page_num)
            return leaf_index.find_data_page_for_key(key_value)

    def _handle_page_overflow(self, file, page_num, page, new_record, current_leaf_page_num):
        with open(self.leaf_index_file, "rb") as leaf_index_file:
            leaf_index_obj = self._read_leaf_index(leaf_index_file, current_leaf_page_num)
            
            if not leaf_index_obj.is_full():
                # Estrategia 1: Split página de datos
                self._split_page_strategy(file, page_num, page, new_record, current_leaf_page_num)
            else:
                with open(self.root_index_file, "rb") as root_file:
                    root_index_obj = self._read_root_index(root_file, 0)

                    if not root_index_obj.is_full():
                        # Estrategia 2: Split leaf index
                        self._split_leaf_index_strategy(file, page_num, page, new_record, current_leaf_page_num)
                    else:
                        # Estrategia 3: Overflow chain
                        self._overflow_page_strategy(file, page_num, page, new_record)

    def _split_page_strategy(self, file, page_num, page, new_record, leaf_page_num):
        all_records = page.records + [new_record]
        all_records.sort(key=lambda r: r.get_key())

        mid_point = len(all_records) // 2
        left_records = all_records[:mid_point]
        right_records = all_records[mid_point:]

        left_page = Page(left_records, block_factor=self.block_factor, record_size=self.record_template.RECORD_SIZE)
        self._write_page(file, page_num, left_page)

        file.seek(0, 2)
        page_size = Page.HEADER_SIZE + self.block_factor * self.record_template.RECORD_SIZE
        new_page_num = (file.tell() - self.DATA_START_OFFSET) // page_size
        right_page = Page(right_records, block_factor=self.block_factor, record_size=self.record_template.RECORD_SIZE)
        self.performance.track_write()
        file.write(right_page.pack())

        separator_key = right_records[0].get_key()

        self._update_leaf_index_after_split(separator_key, new_page_num, page_num, left_records[0].get_key(), leaf_page_num)
        self.next_page_number = new_page_num + 1

    def _split_leaf_index_strategy(self, file, page_num, page, new_record, leaf_page_num):

        all_records = page.records + [new_record]
        all_records.sort(key=lambda r: r.get_key())

        mid_point = len(all_records) // 2
        left_records = all_records[:mid_point]
        right_records = all_records[mid_point:]

        left_page = Page(left_records, block_factor=self.block_factor, record_size=self.record_template.RECORD_SIZE)
        self._write_page(file, page_num, left_page)

        file.seek(0, 2)
        page_size = Page.HEADER_SIZE + self.block_factor * self.record_template.RECORD_SIZE
        new_data_page_num = (file.tell() - self.DATA_START_OFFSET) // page_size
        right_page = Page(right_records, block_factor=self.block_factor, record_size=self.record_template.RECORD_SIZE)
        self.performance.track_write()
        file.write(right_page.pack())
        self.next_page_number = new_data_page_num + 1

        separator_key = right_records[0].get_key()
        
        with open(self.leaf_index_file, "r+b") as leaf_index_file:
            current_leaf_index = self._read_leaf_index(leaf_index_file, leaf_page_num)
            
            new_entry = LeafIndexEntry(separator_key, new_data_page_num)
            current_leaf_index.insert_sorted(new_entry)
            
            if len(current_leaf_index.entries) > self.leaf_index_block_factor:
                self._split_leaf_index_page(leaf_index_file, leaf_page_num, current_leaf_index)
            else:
                self._write_leaf_index(leaf_index_file, leaf_page_num, current_leaf_index)

    def _split_leaf_index_page(self, leaf_index_file, leaf_page_num, overloaded_leaf_index):
        mid_point = len(overloaded_leaf_index.entries) // 2
        left_entries = overloaded_leaf_index.entries[:mid_point]
        right_entries = overloaded_leaf_index.entries[mid_point:]

        # Calcular nueva página antes de escribir
        leaf_index_file.seek(0, 2)
        leaf_size = LeafIndex.HEADER_SIZE + self.leaf_index_block_factor * LeafIndexEntry.SIZE
        new_leaf_page_num = leaf_index_file.tell() // leaf_size

        # El lado izquierdo apunta al nuevo lado derecho
        left_leaf_index = LeafIndex(left_entries, new_leaf_page_num, self.leaf_index_block_factor)
        self._write_leaf_index(leaf_index_file, leaf_page_num, left_leaf_index)

        # El lado derecho mantiene el next_page original
        right_leaf_index = LeafIndex(right_entries, overloaded_leaf_index.next_page, self.leaf_index_block_factor)
        self._write_leaf_index(leaf_index_file, new_leaf_page_num, right_leaf_index)

        self.next_leaf_index_page_number = new_leaf_page_num + 1
        
        separator_key = right_entries[0].key
        self._update_root_index_with_new_page(separator_key, new_leaf_page_num)

    def _overflow_page_strategy(self, file, page_num, original_page, new_record):
        page_num_found, page_found, need_new_page = self._find_available_or_last_page_in_chain(file, page_num)

        if not need_new_page:
            page_found.insert_sorted(new_record)
            self._write_page(file, page_num_found, page_found)
        else:
            free_page_num = self.free_list_stack.pop_free_page()
            if free_page_num is not None:
                new_overflow_page_num = free_page_num
            else:
                file.seek(0, 2)
                page_size = Page.HEADER_SIZE + self.block_factor * self.record_template.RECORD_SIZE
                new_overflow_page_num = (file.tell() - self.DATA_START_OFFSET) // page_size
                self.next_page_number = new_overflow_page_num + 1

            new_overflow_page = Page([new_record], block_factor=self.block_factor, record_size=self.record_template.RECORD_SIZE)
            self._write_page(file, new_overflow_page_num, new_overflow_page)

            page_found.next_page = new_overflow_page_num
            self._write_page(file, page_num_found, page_found)

    def _find_available_or_last_page_in_chain(self, file, start_page_num):
        current_page_num = start_page_num
        
        while current_page_num != -1:
            page = self._read_page(file, current_page_num)
            
            if not page.is_full():
                return current_page_num, page, False
            
            if page.next_page == -1:
                return current_page_num, page, True 
            
            current_page_num = page.next_page
        
        return start_page_num, None, True


    def _search_in_page_chain(self, file, start_page_num, key_value):
        current_page_num = start_page_num
        visited = set()

        while current_page_num != -1 and current_page_num not in visited:
            visited.add(current_page_num)
            try:
                page = self._read_page(file, current_page_num)

                for record in page.records:
                    if record.get_key() == key_value:
                        return record

                current_page_num = page.next_page if page.next_page != -1 else -1
            except:
                break

        return None


    def _update_leaf_index_after_split(self, right_key, right_page_num, left_page_num, left_key, leaf_page_num):
        with open(self.leaf_index_file, "r+b") as file:
            leaf_index = self._read_leaf_index(file, leaf_page_num)

            # 1. Encontrar y actualizar la entrada que apunta a la página izquierda
            for entry in leaf_index.entries:
                if entry.data_page_number == left_page_num:
                    entry.key = left_key
                    break

            # 2. Agregar nueva entrada para la página derecha
            new_entry = LeafIndexEntry(right_key, right_page_num)
            leaf_index.insert_sorted(new_entry)

            if len(leaf_index.entries) > self.leaf_index_block_factor:
                self._split_leaf_index_page(file, leaf_page_num, leaf_index)
            else:
                self._write_leaf_index(file, leaf_page_num, leaf_index)

    def _update_leaf_index_with_new_page(self, key, page_num, leaf_page_num):
        with open(self.leaf_index_file, "r+b") as file:
            leaf_index = self._read_leaf_index(file, leaf_page_num)
            new_entry = LeafIndexEntry(key, page_num)
            leaf_index.insert_sorted(new_entry)

            if len(leaf_index.entries) > self.leaf_index_block_factor:
                self._split_leaf_index_page(file, leaf_page_num, leaf_index)
            else:
                self._write_leaf_index(file, leaf_page_num, leaf_index)

    def _update_root_index_with_new_page(self, key, leaf_page_num):
        with open(self.root_index_file, "r+b") as file:
            root_index = self._read_root_index(file, 0)
            new_entry = RootIndexEntry(key, leaf_page_num)
            root_index.insert_sorted(new_entry)
            
            if root_index.is_full():
                print("WARNING: Root index is full.")
            
            self._write_root_index(file, 0, root_index)

    def _delete_from_overflow_chain(self, file, start_page_num, key_value):
        current_page_num = start_page_num

        while current_page_num != -1:
            page = self._read_page(file, current_page_num)

            if page.remove_record(key_value):
                self._write_page(file, current_page_num, page)

                rebuild_triggered = False
                if len(page.records) == 0 and self._is_overflow_page(current_page_num):
                    self._remove_page_from_chain(file, start_page_num, current_page_num)
                    self.free_list_stack.push_free_page(current_page_num)
                elif len(page.records) <= self.consolidation_threshold:
                    self._try_consolidate_page(file, current_page_num)

                if self._should_rebuild():
                    self.rebuild()
                    rebuild_triggered = True

                return True, rebuild_triggered

            current_page_num = page.next_page

        return False, False

    def _try_consolidate_page(self, file, page_num):
        page = self._read_page(file, page_num)
        
        if page.next_page != -1:
            next_page_num = page.next_page
            next_page = self._read_page(file, next_page_num)
            
            if page.can_merge_with(next_page):
                page.merge_with(next_page)
                page.next_page = next_page.next_page
                
                self._write_page(file, page_num, page)
                
                if self._is_overflow_page(next_page_num):
                    self.free_list_stack.push_free_page(next_page_num)
                else:
                    empty_page = Page(block_factor=self.block_factor, record_size=self.record_template.RECORD_SIZE)
                    self._write_page(file, next_page_num, empty_page)

    def _remove_page_from_chain(self, file, start_page_num, page_to_remove):
        if start_page_num == page_to_remove:
            return
        
        current_page_num = start_page_num
        while current_page_num != -1:
            page = self._read_page(file, current_page_num)
            
            if page.next_page == page_to_remove:
                page_to_remove_obj = self._read_page(file, page_to_remove)
                page.next_page = page_to_remove_obj.next_page
                self._write_page(file, current_page_num, page)
                return
            
            current_page_num = page.next_page



    def _count_overflow_chain_length(self, file, start_page_num):
        if self._is_overflow_page(start_page_num):
            return 0

        page = self._read_page(file, start_page_num)
        if page.next_page == -1:
            return 0

        length = 1
        current = page.next_page
        while current != -1 and length < 10:
            try:
                next_page = self._read_page(file, current)
                current = next_page.next_page
                length += 1
            except:
                break

        return length
    def warm_up(self):
        
        if not os.path.exists(self.filename):
            return
        
        try:
            with open(self.root_index_file, "rb") as root_file:
                _ = self._read_root_index(root_file, 0)
            
            with open(self.leaf_index_file, "rb") as leaf_file:
                _ = self._read_leaf_index(leaf_file, 0)
            
            with open(self.filename, "rb") as data_file:
                _ = self._read_page(data_file, 0)
            
            dummy_key = -999999
            _ = self._find_target_leaf_page(dummy_key)
            _ = self._find_target_data_page(dummy_key, 0)
            
        except:
            pass
        
        self.performance = PerformanceTracker()

    def _should_rebuild(self):
        if not os.path.exists(self.filename):
            return False

        free_count = self._get_free_count()
        if free_count == 0:
            return False

        with open(self.filename, "rb") as file:
            file_size = os.path.getsize(self.filename)
            if file_size < self.DATA_START_OFFSET:
                return False

            page_size = Page.HEADER_SIZE + self.block_factor * self.record_template.RECORD_SIZE
            total_pages = (file_size - self.DATA_START_OFFSET) // page_size

            if total_pages == 0:
                return False

            free_ratio = free_count / total_pages

            if free_ratio > 0.40:
                return True

            chain_count = 0
            chain_total = 0

            for i in range(total_pages):
                if not self._is_overflow_page(i):
                    chain_length = self._count_overflow_chain_length(file, i)
                    if chain_length > 0:
                        chain_count += 1
                        chain_total += chain_length

            if chain_count > 0:
                avg_chain = chain_total / chain_count
                return avg_chain > 4.0

        return False

    # Operaciones principales
    
    def insert(self, record: Record):
        self.performance.start_operation()

        existing_record_result = self.search(record.get_key())
        if existing_record_result.data is not None:
            raise ValueError(f"Primary key {record.get_key()} already exists")

        if not os.path.exists(self.filename):
            self._create_initial_files(record)
            return self.performance.end_operation(True, False)

        target_leaf_page_num = self._find_target_leaf_page(record.get_key())
        target_data_page_num = self._find_target_data_page(record.get_key(), target_leaf_page_num)

        with open(self.filename, "r+b") as file:
            page = self._read_page(file, target_data_page_num)

            if not page.is_full():
                page.insert_sorted(record)
                self._write_page(file, target_data_page_num, page)
            else:
                self._handle_page_overflow(file, target_data_page_num, page, record, target_leaf_page_num)

        rebuild_triggered = False
        if self._should_rebuild():
            self.rebuild()
            rebuild_triggered = True

        return self.performance.end_operation(True, rebuild_triggered)

    def search(self, key_value):
        self.performance.start_operation()

        if not os.path.exists(self.filename):
            return self.performance.end_operation(None, False)

        with open(self.root_index_file, "rb") as root_file, \
             open(self.leaf_index_file, "rb") as leaf_file, \
             open(self.filename, "rb") as data_file:

            root_index = self._read_root_index(root_file, 0)
            target_leaf_page_num = root_index.find_leaf_page_for_key(key_value)

            leaf_index = self._read_leaf_index(leaf_file, target_leaf_page_num)
            target_data_page_num = leaf_index.find_data_page_for_key(key_value)

            result = self._search_in_page_chain(data_file, target_data_page_num, key_value)
            return self.performance.end_operation(result)


    def delete(self, key_value):
        self.performance.start_operation()

        if not os.path.exists(self.filename):
            return self.performance.end_operation(False)

        target_leaf_page_num = self._find_target_leaf_page(key_value)
        target_data_page_num = self._find_target_data_page(key_value, target_leaf_page_num)

        with open(self.filename, "r+b") as file:
            page = self._read_page(file, target_data_page_num)

            if page.remove_record(key_value):
                self._write_page(file, target_data_page_num, page)

                rebuild_triggered = False
                if len(page.records) <= self.consolidation_threshold:
                    self._try_consolidate_page(file, target_data_page_num)

                    if self._should_rebuild():
                        self.rebuild()
                        rebuild_triggered = True

                return self.performance.end_operation(True, rebuild_triggered)

            result, rebuild_triggered = self._delete_from_overflow_chain(file, target_data_page_num, key_value)
            return self.performance.end_operation(result, rebuild_triggered)

    def range_search(self, begin_key, end_key):
        self.performance.start_operation()

        results = []

        if not os.path.exists(self.filename) or begin_key > end_key:
            return self.performance.end_operation(results)

        start_leaf, end_leaf = self._find_leaf_page_range_for_keys(begin_key, end_key)

        with open(self.filename, "rb") as data_file:
            with open(self.leaf_index_file, "rb") as leaf_file:
                visited_pages = set()

                for leaf_page_num in range(start_leaf, end_leaf + 1):
                    leaf_index = self._read_leaf_index(leaf_file, leaf_page_num)

                    for entry in leaf_index.entries:
                        if entry.key > end_key:
                            break

                        current_page_num = entry.data_page_number

                        while current_page_num is not None and current_page_num not in visited_pages:
                            visited_pages.add(current_page_num)
                            page = self._read_page(data_file, current_page_num)

                            for record in page.records:
                                if record.get_key() > end_key:
                                    break
                                if begin_key <= record.get_key() <= end_key:
                                    results.append(record)

                            current_page_num = page.next_page if page.next_page != -1 else None

        sorted_results = sorted(results, key=lambda r: r.get_key())
        return self.performance.end_operation(sorted_results)

    def rebuild(self):
        scan_result = self.scan_all()
        all_records = scan_result.data
        all_records.sort(key=lambda r: r.get_key())

        old_files = [self.filename, self.root_index_file, self.leaf_index_file]
        backup_files = [f + ".backup" for f in old_files]

        for old, backup in zip(old_files, backup_files):
            if os.path.exists(old):
                os.rename(old, backup)

        new_root_factor = int(self.root_index_block_factor * 1.5)
        new_leaf_factor = int(self.leaf_index_block_factor * 1.5)
        new_block_factor = int(self.block_factor * 1.3)

        self.root_index_block_factor = new_root_factor
        self.leaf_index_block_factor = new_leaf_factor
        self.block_factor = new_block_factor
        self.consolidation_threshold = new_block_factor // 3

        self.free_list_stack.clear()
        self.next_page_number = 0
        self.next_root_index_page_number = 0
        self.next_leaf_index_page_number = 0

        for record in all_records:
            self.insert(record)

        for backup in backup_files:
            if os.path.exists(backup):
                os.remove(backup)

        return True


    # Funciones extras

    def _is_overflow_page(self, page_num):
        if not os.path.exists(self.leaf_index_file):
            return page_num > 0

        try:
            with open(self.leaf_index_file, "rb") as file:
                file_size = os.path.getsize(self.leaf_index_file)
                leaf_size = LeafIndex.HEADER_SIZE + self.leaf_index_block_factor * LeafIndexEntry.SIZE
                num_leaf_pages = file_size // leaf_size

                for i in range(num_leaf_pages):
                    leaf_index = self._read_leaf_index(file, i)
                    for entry in leaf_index.entries:
                        if entry.data_page_number == page_num:
                            return False
                return True
        except:
            return page_num > 0

    def scan_all(self):
        self.performance.start_operation()

        results = []

        if not os.path.exists(self.filename):
            return self.performance.end_operation(results)

        with open(self.filename, "rb") as file:
            file_size = os.path.getsize(self.filename)
            if file_size < self.DATA_START_OFFSET:
                return self.performance.end_operation(results)

            page_size = Page.HEADER_SIZE + self.block_factor * self.record_template.RECORD_SIZE
            num_pages = (file_size - self.DATA_START_OFFSET) // page_size
            visited = set()

            for i in range(num_pages):
                if i in visited:
                    continue

                current_page_num = i
                while current_page_num is not None and current_page_num not in visited:
                    visited.add(current_page_num)
                    page = self._read_page(file, current_page_num)
                    results.extend(page.records)
                    current_page_num = page.next_page if page.next_page != -1 else None

        return self.performance.end_operation(results)

    def drop_table(self):
        files_to_remove = [
            self.filename,
            self.root_index_file,
            self.leaf_index_file,
            self.free_list_stack.free_list_file
        ]

        removed_files = []
        for file_path in files_to_remove:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    removed_files.append(file_path)
                except OSError:
                    pass

        base_dir = os.path.dirname(self.filename)
        if os.path.exists(base_dir) and not os.listdir(base_dir):
            try:
                os.rmdir(base_dir)
                removed_files.append(base_dir)
            except OSError:
                pass

        return removed_files


