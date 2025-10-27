import struct
import os
import hashlib
from ..core.record import IndexRecord
from ..core.performance_tracker import PerformanceTracker

BLOCK_FACTOR = 20
MAX_OVERFLOW = 2
MIN_N = BLOCK_FACTOR // 2


class Bucket:
    HEADER_FORMAT = "iiii"  # local_depth, allocated_slots, actual_size, next_bucket
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, local_depth, num_slots, num_records, next_overflow_bucket, index_record_template, performance):
        self.local_depth = local_depth
        self.num_slots = num_slots
        self.num_records = num_records
        self.next_overflow_bucket = next_overflow_bucket
        self.index_record_template = index_record_template
        self.index_record_size = index_record_template.RECORD_SIZE
        self.performance = performance
        self.records = []
        self.bucket_data = None

    @classmethod
    def read_bucket(cls, bucket_pos, bucketfile, index_record_template, performance):
        if bucket_pos == -1:
            return None
        bucketfile.seek(bucket_pos)
        bucket_size = cls.HEADER_SIZE + (BLOCK_FACTOR * index_record_template.RECORD_SIZE)
        bucket_data = bucketfile.read(bucket_size)
        performance.track_read()

        local_depth, num_slots, num_records, next_overflow = struct.unpack(cls.HEADER_FORMAT,
                                                                           bucket_data[:cls.HEADER_SIZE])

        bucket = cls(local_depth, num_slots, num_records, next_overflow, index_record_template, performance)

        bucket.bucket_data = bucket_data[cls.HEADER_SIZE:]

        tombstone = b'\x00' * bucket.index_record_size
        for i in range(num_slots):
            offset = i * bucket.index_record_size
            record_data = bucket.bucket_data[offset:offset + bucket.index_record_size]
            if record_data != tombstone:
                record = IndexRecord.unpack(record_data, index_record_template.value_type_size, "index_value")
                bucket.records.append(record)

        return bucket

    def is_full(self):
        return self.num_records >= BLOCK_FACTOR

    def has_space(self):
        return self.num_records < BLOCK_FACTOR

    def search(self, secondary_value, extendible_hash, debug=False):
        matching_pks = []
        normalized_search = extendible_hash._normalize_value(secondary_value)

        for i, record in enumerate(self.records):
            normalized_record = extendible_hash._normalize_value(record.index_value)

            if normalized_record == normalized_search:
                matching_pks.append(record.primary_key)

        if debug:
            print(f"    [BUCKET DEBUG] Found {len(matching_pks)} matches")

        return matching_pks

    def write_bucket(self, bucket_pos, bucketfile):
        self.num_slots = len(self.records)

        bucketfile.seek(bucket_pos)
        bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, self.local_depth,
                                     self.num_slots, self.num_records,
                                     self.next_overflow_bucket))

        tombstone = b'\x00' * self.index_record_size
        for i in range(BLOCK_FACTOR):
            if i < len(self.records):
                bucketfile.write(self.records[i].pack())
            else:
                bucketfile.write(tombstone)

        self.performance.track_write()

    def insert(self, index_record: IndexRecord, bucket_pos, bucketfile, extendible_hash=None):
        if self.is_full():
            return False

        for existing_record in self.records:
            if extendible_hash:
                existing_normalized = extendible_hash._normalize_value(existing_record.index_value)
                new_normalized = extendible_hash._normalize_value(index_record.index_value)
                if existing_normalized == new_normalized and existing_record.primary_key == index_record.primary_key:
                    return False
            else:
                if existing_record.index_value == index_record.index_value and existing_record.primary_key == index_record.primary_key:
                    return False

        insert_position = None
        tombstone = b'\x00' * self.index_record_size

        if self.num_slots > self.num_records:
            for i in range(self.num_slots):
                offset = i * self.index_record_size
                if self.bucket_data[offset:offset + self.index_record_size] == tombstone:
                    insert_position = bucket_pos + Bucket.HEADER_SIZE + offset
                    break

        if insert_position is None:
            if self.num_records < BLOCK_FACTOR:
                insert_position = bucket_pos + Bucket.HEADER_SIZE + (self.num_slots * self.index_record_size)
                self.num_slots += 1
            else:
                return False

        bucketfile.seek(insert_position)
        bucketfile.write(index_record.pack())
        self.performance.track_write()

        self.records.append(index_record)
        self.num_records += 1

        bucketfile.seek(bucket_pos)
        bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, self.local_depth, self.num_slots,
                                     self.num_records, self.next_overflow_bucket))
        self.performance.track_write()
        return True

    def delete(self, key, bucket_pos, bucketfile, pk=None, extendible_hash=None):
        tombstone = b'\x00' * self.index_record_size
        deleted_pks = []

        if extendible_hash:
            normalized_key = extendible_hash._normalize_value(key)
        else:
            normalized_key = key

        records_to_remove = []
        for i, record in enumerate(self.records):
            if extendible_hash:
                stored_key = extendible_hash._normalize_value(record.index_value)
            else:
                stored_key = record.index_value

            should_delete = False

            if stored_key == normalized_key:
                if pk is None or record.primary_key == pk:
                    should_delete = True

            if should_delete:
                slot_index = self.find_record_slot(record)
                if slot_index != -1:
                    slot_pos = bucket_pos + Bucket.HEADER_SIZE + (slot_index * self.index_record_size)
                    bucketfile.seek(slot_pos)
                    bucketfile.write(tombstone)
                    self.performance.track_write()

                    records_to_remove.append(i)
                    deleted_pks.append(record.primary_key)

                    if pk is not None:
                        break

        for i in reversed(records_to_remove):
            self.records.pop(i)

        self.num_records -= len(records_to_remove)
        if records_to_remove:
            bucketfile.seek(bucket_pos)
            bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, self.local_depth,
                                         self.num_slots, self.num_records,
                                         self.next_overflow_bucket))
            self.performance.track_write()

        return deleted_pks

    def find_record_slot(self, target_record):
        tombstone = b'\x00' * self.index_record_size
        target_packed = target_record.pack()

        for i in range(self.num_slots):
            offset = i * self.index_record_size
            record_data = self.bucket_data[offset:offset + self.index_record_size]
            if record_data != tombstone and record_data == target_packed:
                return i
        return -1


class ExtendibleHashing:
    HEADER_FORMAT = "ii"  # global_depth, free pointer
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    DIR_FORMAT = "i"  # bucket pointer
    DIR_SIZE = struct.calcsize(DIR_FORMAT)

    def __init__(self, data_filename, index_field_name, index_field_type, index_field_size, is_primary=False):
        if is_primary:
            raise ValueError("ExtendibleHashing can only be used as secondary index")

        self.index_field_name = index_field_name
        self.dirname = f"{data_filename}.dir"
        self.bucketname = f"{data_filename}.bkt"
        self.index_record_template = IndexRecord(index_field_type, index_field_size)
        self.index_record_size = self.index_record_template.RECORD_SIZE
        self.performance = PerformanceTracker()

        if not os.path.exists(self.dirname) or not os.path.exists(self.bucketname):
            self._initialize_files()

        self.global_depth, self.first_free_bucket_pos = self._read_header()

    def warm_up(self):

        temp_tracker = PerformanceTracker()
        old_tracker = self.performance
        self.performance = temp_tracker

        try:
            with open(self.dirname, 'rb') as dirfile:
                dirfile.read(self.HEADER_SIZE)

                dir_size = 2 ** self.global_depth
                dirfile.read(dir_size * self.DIR_SIZE)

            with open(self.bucketname, 'rb') as bucketfile:
                bucket_size = Bucket.HEADER_SIZE + (BLOCK_FACTOR * self.index_record_size)
                for _ in range(min(10, 2 ** self.global_depth)):
                    bucketfile.read(bucket_size)
        except:
            pass
        finally:
            self.performance = old_tracker

    def _hash_key(self, key):
        normalized = self._normalize_value(key)
        if isinstance(normalized, str):
            normalized = normalized.encode('utf-8')
        elif not isinstance(normalized, bytes):
            normalized = str(normalized).encode('utf-8')
        return int(hashlib.md5(normalized).hexdigest(), 16)

    def _normalize_value(self, value):
        if value is None:
            return ""
        if isinstance(value, bytes):
            decoded = value.decode('utf-8', errors='ignore')
            return decoded.strip('\x00').strip()
        else:
            return str(value).strip()

    def _read_header(self):
        with open(self.dirname, 'rb') as dirfile:
            data = dirfile.read(self.HEADER_SIZE)
            self.performance.track_read()
            return struct.unpack(self.HEADER_FORMAT, data)

    def _write_header(self):
        with open(self.dirname, 'r+b') as dirfile:
            dirfile.seek(0)
            dirfile.write(struct.pack(self.HEADER_FORMAT, self.global_depth, self.first_free_bucket_pos))
            self.performance.track_write()

    def search(self, secondary_value, debug=False):
        self.performance.start_operation()

        if debug:
            normalized = self._normalize_value(secondary_value)
            hash_val = self._hash_key(secondary_value)
            dir_index = hash_val % (2 ** self.global_depth)

        with open(self.dirname, 'rb') as dirfile, open(self.bucketname, 'rb') as bucketfile:
            bucket, bucket_pos = self._get_bucket_from_key(secondary_value, dirfile, bucketfile)

            matching_pk = []

            current_pos = bucket_pos
            current_bucket = bucket
            bucket_num = 0
            while current_bucket is not None:
                bucket_matches = current_bucket.search(secondary_value, self, debug=debug)
                matching_pk.extend(bucket_matches)

                if debug:
                    print(f"[HASH SEARCH DEBUG] Bucket {bucket_num} found {len(bucket_matches)} matches")

                if current_bucket.next_overflow_bucket != -1:
                    current_pos = current_bucket.next_overflow_bucket
                    current_bucket = Bucket.read_bucket(current_pos, bucketfile, self.index_record_template,
                                                        self.performance)
                    bucket_num += 1
                else:
                    break

            return self.performance.end_operation(matching_pk)

    def insert(self, index_record: IndexRecord, debug=False):
        self.performance.start_operation()

        secondary_value = index_record.index_value
        if secondary_value is None:
            return self.performance.end_operation(True)

        if isinstance(index_record.index_value, str):
            index_record.index_value = index_record.index_value.encode('utf-8')[
                                       :self.index_record_template.value_type_size[0][2]]

        with open(self.dirname, 'r+b') as dirfile, open(self.bucketname, 'r+b') as bucketfile:
            result = self._insert_index_record(index_record, dirfile, bucketfile, debug=debug,
                                               original_value=secondary_value)
            return self.performance.end_operation(True)

    def delete(self, secondary_value, primary_key=None):
        self.performance.start_operation()

        with open(self.dirname, 'r+b') as dirfile, open(self.bucketname, 'r+b') as bucketfile:
            bucket, bucket_pos = self._get_bucket_from_key(secondary_value, dirfile, bucketfile)
            deleted_pks = []
            head = bucket
            while bucket is not None:
                deleted_pks += bucket.delete(secondary_value, bucket_pos, bucketfile, primary_key, self)
                bucket_pos = bucket.next_overflow_bucket
                bucket = Bucket.read_bucket(bucket_pos, bucketfile, self.index_record_template, self.performance)

            if head.num_records <= MIN_N:
                if head.next_overflow_bucket != -1:
                    self._overflow_to_main_bucket(head, bucket_pos, dirfile, bucketfile)
                elif head.num_records == 0:
                    self._handle_empty_bucket(head, bucket_pos, dirfile, bucketfile)

            if primary_key is None:
                return self.performance.end_operation(deleted_pks)
            else:
                return self.performance.end_operation(len(deleted_pks) > 0)

    def _get_bucket_from_key(self, key, dirfile, bucketfile):
        hash_val = self._hash_key(key)
        dir_index = hash_val % (2 ** self.global_depth)
        dirfile.seek(self.HEADER_SIZE + dir_index * self.DIR_SIZE)
        bucket_pos = struct.unpack(self.DIR_FORMAT, dirfile.read(self.DIR_SIZE))[0]
        self.performance.track_read()

        bucket = Bucket.read_bucket(bucket_pos, bucketfile, self.index_record_template, self.performance)
        return bucket, bucket_pos

    def _insert_index_record(self, index_record, dirfile, bucketfile, debug=False, original_value=None):
        secondary_value = original_value if original_value is not None else index_record.index_value

        head_bucket, head_bucket_pos = self._get_bucket_from_key(secondary_value, dirfile, bucketfile)

        current_bucket = head_bucket
        current_bucket_pos = head_bucket_pos
        overflow_count = 0

        while True:
            if current_bucket.has_space():
                success = current_bucket.insert(index_record, current_bucket_pos, bucketfile, extendible_hash=self)
                if success:
                    return True

            if current_bucket.next_overflow_bucket != -1:
                overflow_count += 1
                current_bucket_pos = current_bucket.next_overflow_bucket
                current_bucket = Bucket.read_bucket(current_bucket_pos, bucketfile, self.index_record_template,
                                                    self.performance)
            else:
                break

        if head_bucket.local_depth < self.global_depth:
            return self._split_bucket(head_bucket, head_bucket_pos, index_record, dirfile, bucketfile)
        else:
            if overflow_count < MAX_OVERFLOW:
                overflow_bucket_pos = self._append_new_bucket(head_bucket.local_depth, bucketfile)
                self._add_overflow(current_bucket_pos, overflow_bucket_pos, bucketfile)
                overflow_bucket = Bucket.read_bucket(overflow_bucket_pos, bucketfile, self.index_record_template,
                                                     self.performance)
                overflow_bucket.insert(index_record, overflow_bucket_pos, bucketfile, extendible_hash=self)
                return True
            else:
                self._double_directory(dirfile)
                return self._split_bucket(head_bucket, head_bucket_pos, index_record, dirfile, bucketfile)

    def _handle_empty_bucket(self, empty_bucket, bucket_pos, dirfile, bucketfile):
        self._redirect_directory_entries(empty_bucket, bucket_pos, dirfile, bucketfile)
        self.free_bucket(bucket_pos, bucketfile)

    def _redirect_directory_entries(self, empty_bucket, bucket_pos, dirfile, bucketfile):
        dir_size = 2 ** self.global_depth
        header_offset = self.HEADER_SIZE

        empty_index = None
        for i in range(dir_size):
            dirfile.seek(header_offset + i * self.DIR_SIZE)
            pos = struct.unpack(self.DIR_FORMAT, dirfile.read(self.DIR_SIZE))[0]
            if pos == bucket_pos:
                empty_index = i
                break

        if empty_index is None:
            return

        mask = 1 << (empty_bucket.local_depth - 1)
        sibling_index = empty_index ^ mask

        dirfile.seek(header_offset + sibling_index * self.DIR_SIZE)
        sibling_pos = struct.unpack(self.DIR_FORMAT, dirfile.read(self.DIR_SIZE))[0]
        self.performance.track_read()

        for i in range(dir_size):
            dirfile.seek(header_offset + i * self.DIR_SIZE)
            pos = struct.unpack(self.DIR_FORMAT, dirfile.read(self.DIR_SIZE))[0]

            if pos == bucket_pos:
                dirfile.seek(header_offset + i * self.DIR_SIZE)
                dirfile.write(struct.pack(self.DIR_FORMAT, sibling_pos))

        if sibling_pos != bucket_pos:
            bucketfile.seek(sibling_pos)
            ld, num_slots, num_records, next_overflow = struct.unpack(
                Bucket.HEADER_FORMAT,
                bucketfile.read(Bucket.HEADER_SIZE)
            )
            self.performance.track_read()

            if ld == empty_bucket.local_depth:
                new_local_depth = ld - 1
                bucketfile.seek(sibling_pos)
                bucketfile.write(struct.pack(
                    Bucket.HEADER_FORMAT,
                    new_local_depth,
                    num_slots,
                    num_records,
                    next_overflow
                ))
                self.performance.track_write()

    def free_bucket(self, bucket_pos, bucketfile):
        bucketfile.seek(bucket_pos)
        next_free = self.first_free_bucket_pos
        bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, 0, 0, 0, next_free))
        self.performance.track_write()

        self.first_free_bucket_pos = bucket_pos
        self._write_header()

    def _add_overflow(self, bucket_pos, overflow_bucket_pos, bucketfile):
        bucket = Bucket.read_bucket(bucket_pos, bucketfile, self.index_record_template, self.performance)
        bucket.next_overflow_bucket = overflow_bucket_pos
        bucketfile.seek(bucket_pos)
        bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, bucket.local_depth,
                                     bucket.num_slots, bucket.num_records,
                                     bucket.next_overflow_bucket))
        self.performance.track_write()

    def _append_new_bucket(self, local_depth, bucketfile):
        if self.first_free_bucket_pos == -1:
            bucketfile.seek(0, 2)
            new_pos = bucketfile.tell()
        else:
            new_pos = self.first_free_bucket_pos
            bucketfile.seek(new_pos)
            _, _, _, self.first_free_bucket_pos = struct.unpack(Bucket.HEADER_FORMAT,
                                                                bucketfile.read(Bucket.HEADER_SIZE))
            self.performance.track_read()
            self._write_header()

        bucketfile.seek(new_pos)
        bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, local_depth, 0, 0, -1))
        self.performance.track_write()

        tombstone = b'\x00' * self.index_record_size
        bucketfile.write(tombstone * BLOCK_FACTOR)
        self.performance.track_write()

        return new_pos

    def _split_bucket(self, head_bucket, head_bucket_pos, new_index_record, dirfile, bucketfile):
        if head_bucket.local_depth == self.global_depth:
            self._double_directory(dirfile)

        all_records_packed = self._get_all_records_from_bucket(head_bucket, head_bucket_pos, bucketfile)
        all_records_packed.append(new_index_record)

        next_pos = head_bucket.next_overflow_bucket
        while next_pos != -1:
            bucketfile.seek(next_pos)
            _, _, _, next_in_chain = struct.unpack(Bucket.HEADER_FORMAT, bucketfile.read(Bucket.HEADER_SIZE))
            self.performance.track_read()
            self.free_bucket(next_pos, bucketfile)
            next_pos = next_in_chain

        new_local_depth = head_bucket.local_depth + 1
        bucketfile.seek(head_bucket_pos)
        bucketfile.write(struct.pack(Bucket.HEADER_FORMAT, new_local_depth, 0, 0, -1))
        self.performance.track_write()
        tombstone = b'\x00' * self.index_record_size
        bucketfile.seek(head_bucket_pos + Bucket.HEADER_SIZE)
        bucketfile.write(tombstone * BLOCK_FACTOR)
        self.performance.track_write()

        new_bucket_pos = self._append_new_bucket(new_local_depth, bucketfile)
        self._update_directory_pointers(head_bucket_pos, new_bucket_pos, new_local_depth, dirfile)

        bucket_groups = {}
        for index_record in all_records_packed:
            secondary_value = index_record.index_value
            hash_val = self._hash_key(secondary_value)
            dir_index = hash_val % (2 ** self.global_depth)

            dirfile.seek(self.HEADER_SIZE + dir_index * self.DIR_SIZE)
            target_bucket_pos = struct.unpack(self.DIR_FORMAT, dirfile.read(self.DIR_SIZE))[0]
            self.performance.track_read()

            if target_bucket_pos not in bucket_groups:
                bucket_groups[target_bucket_pos] = []
            bucket_groups[target_bucket_pos].append(index_record)

        for bucket_pos, records in bucket_groups.items():
            bucket = Bucket.read_bucket(bucket_pos, bucketfile, self.index_record_template, self.performance)

            for record in records:
                is_duplicate = False
                for existing_record in bucket.records:
                    existing_normalized = self._normalize_value(existing_record.index_value)
                    new_normalized = self._normalize_value(record.index_value)
                    if existing_normalized == new_normalized and existing_record.primary_key == record.primary_key:
                        is_duplicate = True
                        break

                if not is_duplicate:
                    if len(bucket.records) < BLOCK_FACTOR:
                        bucket.records.append(record)
                        bucket.num_records += 1
                    else:
                        if bucket.next_overflow_bucket == -1:
                            overflow_pos = self._append_new_bucket(bucket.local_depth, bucketfile)
                            bucket.next_overflow_bucket = overflow_pos
                            bucket.write_bucket(bucket_pos, bucketfile)

                            bucket = Bucket.read_bucket(overflow_pos, bucketfile, self.index_record_template,
                                                        self.performance)
                            bucket_pos = overflow_pos

                        if len(bucket.records) < BLOCK_FACTOR:
                            bucket.records.append(record)
                            bucket.num_records += 1

            bucket.write_bucket(bucket_pos, bucketfile)

    def _get_all_records_from_bucket(self, bucket, bucket_pos, bucketfile):
        all_records = []
        current_bucket = bucket
        current_pos = bucket_pos

        while current_bucket is not None:
            all_records.extend(current_bucket.records)

            if current_bucket.next_overflow_bucket != -1:
                current_pos = current_bucket.next_overflow_bucket
                current_bucket = Bucket.read_bucket(current_pos, bucketfile, self.index_record_template,
                                                    self.performance)
            else:
                break

        return all_records

    def _double_directory(self, dirfile):
        dir_size = 2 ** self.global_depth
        header = self.HEADER_SIZE

        dirfile.seek(header)
        buf = dirfile.read(dir_size * self.DIR_SIZE)
        self.performance.track_read()

        current_dir = [entry[0] for entry in struct.iter_unpack(self.DIR_FORMAT, buf)]

        new_dir = current_dir + current_dir
        dirfile.seek(header)
        dirfile.write(struct.pack(self.DIR_FORMAT * len(new_dir), *new_dir))

        self.global_depth += 1
        dirfile.seek(0)
        dirfile.write(struct.pack(self.HEADER_FORMAT, self.global_depth, self.first_free_bucket_pos))
        self.performance.track_write()

    def _update_directory_pointers(self, old_bucket_pos, new_bucket_pos, new_local_depth, dirfile):
        dir_size = 2 ** self.global_depth
        entry_size = self.DIR_SIZE
        header = self.HEADER_SIZE

        for i in range(dir_size):
            dirfile.seek(header + i * entry_size)
            current_ptr = struct.unpack(self.DIR_FORMAT, dirfile.read(entry_size))[0]

            if current_ptr == old_bucket_pos:
                bit = (i >> (new_local_depth - 1)) & 1
                if bit == 1:
                    dirfile.seek(header + i * entry_size)
                    dirfile.write(struct.pack(self.DIR_FORMAT, new_bucket_pos))

    def _initialize_files(self, initial_depth=3):
        self.global_depth = initial_depth
        self.first_free_bucket_pos = -1

        with open(self.dirname, 'w+b') as dirfile:
            dirfile.write(struct.pack(self.HEADER_FORMAT, self.global_depth, self.first_free_bucket_pos))
            self.performance.track_write()

            with open(self.bucketname, 'w+b') as bucketfile:
                bucket0_pos = self._append_new_bucket_init(bucketfile, 1)
                bucket1_pos = self._append_new_bucket_init(bucketfile, 1)

            dirfile.seek(self.HEADER_SIZE)
            for i in range(2 ** self.global_depth):
                bucket_pos = bucket0_pos if (i % 2 == 0) else bucket1_pos
                dirfile.write(struct.pack(self.DIR_FORMAT, bucket_pos))

    def _append_new_bucket_init(self, bucketfile, local_depth):
        bucketfile.seek(0, 2)
        new_pos = bucketfile.tell()
        header_data = struct.pack(Bucket.HEADER_FORMAT, local_depth, 0, 0, -1)
        bucketfile.write(header_data)
        self.performance.track_write()
        tombstone = b'\x00' * self.index_record_size
        records_data = tombstone * BLOCK_FACTOR
        bucketfile.write(records_data)
        self.performance.track_write()
        return new_pos

    def drop_index(self):
        removed_files = []
        for file_path in [self.dirname, self.bucketname]:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    removed_files.append(file_path)
                except OSError:
                    pass
        return removed_files

    def _overflow_to_main_bucket(self, curr, curr_pos, dirfile, bucketfile):
        if curr.next_overflow_bucket != -1:
            next_pos = curr.next_overflow_bucket
            next_bucket = Bucket.read_bucket(next_pos, bucketfile, self.index_record_template, self.performance)

            while next_bucket.num_records > 0:
                for rec in next_bucket.records:
                    next_bucket.delete(rec.index_value, next_pos, bucketfile, extendible_hash=self)
                    self.insert(rec)

                if next_bucket.next_overflow_bucket != -1:
                    next_pos = next_bucket.next_overflow_bucket
                    next_bucket = Bucket.read_bucket(next_pos, bucketfile, self.index_record_template, self.performance)
                else:
                    break

            next_pos = curr.next_overflow_bucket
            next_bucket = Bucket.read_bucket(next_pos, bucketfile, self.index_record_template, self.performance)
            while next_bucket is not None:
                temp_pos = next_pos
                next_pos = next_bucket.next_overflow_bucket
                if next_bucket.num_records == 0:
                    self.free_bucket(temp_pos, bucketfile)
                next_bucket = Bucket.read_bucket(next_pos, bucketfile, self.index_record_template, self.performance)

            if curr.num_records == 0:
                self._handle_empty_bucket(curr, curr_pos, dirfile, bucketfile)