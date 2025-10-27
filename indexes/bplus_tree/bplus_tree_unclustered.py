from typing import Any, List, Optional
import bisect
import struct
import os
import unicodedata
from ..core.record import IndexRecord
from ..core.performance_tracker import PerformanceTracker, OperationResult


class Node:
    def __init__(self, is_leaf: bool = False):
        self.is_leaf = is_leaf
        self.keys = []
        self.parent_node_id = None
        self.node_id = None

    def is_full(self, max_keys: int) -> bool:
        return len(self.keys) >= max_keys

    def is_underflow(self, min_keys: int) -> bool:
        return len(self.keys) < min_keys


class LeafNode(Node):
    def __init__(self):
        super().__init__(is_leaf=True)
        self.index_records = []
        self.prev_leaf_id = None
        self.next_leaf_id = None

    def pack(self, key_packer, index_record_size: int, null_id: int) -> bytes:
        parent_id = self.parent_node_id if self.parent_node_id is not None else null_id
        prev_id = self.prev_leaf_id if self.prev_leaf_id is not None else null_id
        next_id = self.next_leaf_id if self.next_leaf_id is not None else null_id
        
        data = bytearray()
        
        data.extend(struct.pack('?', True))
        data.extend(struct.pack('i', len(self.keys)))
        data.extend(struct.pack('i', self.node_id))
        data.extend(struct.pack('i', parent_id))

        data.extend(struct.pack('i', prev_id))
        data.extend(struct.pack('i', next_id))
        
        for i in range(len(self.keys)):
            data.extend(key_packer(self.keys[i]))
            data.extend(self.index_records[i].pack())
        
        return bytes(data)

    @staticmethod
    def unpack(data: bytes, offset: int, num_keys: int, node_id: int, parent_id: Optional[int],
               key_unpacker, key_storage_size: int, index_record_size: int, index_record_class,
               value_type_size: List, key_column: str, null_id: int, normalize_key: bool) -> 'LeafNode':
        leaf = LeafNode()
        leaf.node_id = node_id
        leaf.parent_node_id = parent_id

        prev_id, next_id = struct.unpack('ii', data[offset:offset+8])
        
        leaf.prev_leaf_id = None if prev_id == null_id else prev_id
        leaf.next_leaf_id = None if next_id == null_id else next_id
        
        offset += 8

        leaf.keys = []
        leaf.index_records = []

        for i in range(num_keys):
            key_bytes = data[offset:offset+key_storage_size]
            
            key = key_unpacker(key_bytes)
            
            if normalize_key:
                key = key.decode('utf-8').rstrip('\x00')
            
            leaf.keys.append(key)
            
            offset += key_storage_size

            index_record_bytes = data[offset:offset+index_record_size]
            
            index_record = index_record_class.unpack(index_record_bytes, value_type_size, key_column)
            
            for field_name, field_type, _ in value_type_size:
                if field_type == "CHAR":
                    value = getattr(index_record, field_name)
                    if isinstance(value, bytes):
                        setattr(index_record, field_name, value.decode('utf-8').rstrip('\x00'))
            
            leaf.index_records.append(index_record)
            
            offset += index_record_size

        return leaf


class InternalNode(Node):
    def __init__(self):
        super().__init__(is_leaf=False)
        self.child_node_ids = []

    def pack(self, key_packer, null_id: int) -> bytes:
        parent_id = self.parent_node_id if self.parent_node_id is not None else null_id
        
        data = bytearray()
        
        data.extend(struct.pack('?', False))
        data.extend(struct.pack('i', len(self.keys)))
        data.extend(struct.pack('i', self.node_id))
        data.extend(struct.pack('i', parent_id))
        
        for key in self.keys:
            data.extend(key_packer(key))
        
        for child_id in self.child_node_ids:
            data.extend(struct.pack('i', child_id))
        
        return bytes(data)

    @staticmethod
    def unpack(data: bytes, offset: int, num_keys: int, node_id: int, parent_id: Optional[int],
               key_unpacker, key_storage_size: int, normalize_key: bool) -> 'InternalNode':
        internal = InternalNode()
        internal.node_id = node_id
        internal.parent_node_id = parent_id

        internal.keys = []
        internal.child_node_ids = []

        for i in range(num_keys):
            key_bytes = data[offset:offset+key_storage_size]
            
            key = key_unpacker(key_bytes)
            
            if normalize_key:
                key = key.decode('utf-8').rstrip('\x00')
            
            internal.keys.append(key)
            
            offset += key_storage_size

        child_count = num_keys + 1
        
        children = struct.unpack(f'{child_count}i', data[offset:offset+(child_count*4)])
        
        internal.child_node_ids = list(children)

        return internal

class BPlusTreeUnclusteredIndex:
    METADATA_NODE_ID = 0
    FIRST_DATA_NODE_ID = 1
    NULL_NODE_ID = -1

    def __init__(self, order: int, index_column: str, file_path: str):
        self.index_column = index_column
        self.order = order
        self.max_keys = order - 1
        self.min_keys = (order + 1) // 2 - 1
        self.file_path = file_path + ".dat"
        self.performance = PerformanceTracker()

        self.root_node_id = self.FIRST_DATA_NODE_ID
        self.next_available_node_id = self.FIRST_DATA_NODE_ID + 1
        self._metadata_dirty = False

        self.index_record_class = None
        self.value_type_size = None
        self.index_record_size = None
        self.key_type = None
        self.key_size = None
        self.key_storage_size = None
        self.NODE_SIZE = None
        self.internal_node_size = None
        self.leaf_node_size = None

        if not os.path.exists(self.file_path):
            with open(self.file_path, 'wb') as f:
                f.write(b'\x00' * 8192)
        else:
            self._load_tree_metadata()

    def _initialize_index_record_info(self, index_record: IndexRecord):
        if self.index_record_class is None:
            self.index_record_class = IndexRecord
            self.value_type_size = index_record.value_type_size
            self.index_record_size = index_record.RECORD_SIZE
            
            for field_name, field_type, field_size in self.value_type_size:
                if field_name == "index_value":
                    self.key_type = field_type
                    self.key_size = field_size
                    break
            
            self._calculate_node_sizes()
            self._persist_metadata()

    def _calculate_node_sizes(self):
        header_size = 13
        
        if self.key_type == "INT":
            self.key_storage_size = 4
        elif self.key_type == "FLOAT":
            self.key_storage_size = 4
        elif self.key_type == "CHAR":
            self.key_storage_size = self.key_size
        else:
            raise ValueError(f"Unsupported key type: {self.key_type}")
        
        self.internal_node_size = (
            header_size + 
            (self.max_keys * self.key_storage_size) + 
            ((self.max_keys + 1) * 4)
        )
        
        self.leaf_node_size = (
            header_size + 
            8 + 
            (self.max_keys * (self.key_storage_size + self.index_record_size))
        )
        
        self.NODE_SIZE = max(self.internal_node_size, self.leaf_node_size)
        self.NODE_SIZE = ((self.NODE_SIZE + 511) // 512) * 512

    def _pack_key(self, key: Any) -> bytes:
        if self.key_type == "INT":
            return struct.pack('i', int(key))
        elif self.key_type == "FLOAT":
            return struct.pack('f', float(key))
        elif self.key_type == "CHAR":
            if isinstance(key, bytes):
                key_bytes = key
            elif isinstance(key, str):
                key_bytes = key.encode('utf-8')
            else:
                key_bytes = str(key).encode('utf-8')
            return key_bytes[:self.key_size].ljust(self.key_size, b'\x00')
        else:
            raise ValueError(f"Unsupported key type: {self.key_type}")

    def _unpack_key(self, data: bytes) -> Any:
        if self.key_type == "INT":
            return struct.unpack('i', data)[0]
        elif self.key_type == "FLOAT":
            return struct.unpack('f', data)[0]
        elif self.key_type == "CHAR":
            return data
        else:
            raise ValueError(f"Unsupported key type: {self.key_type}")

    def _normalize_key(self, key: Any) -> Any:
        if self.key_type == "CHAR":
            if isinstance(key, bytes):
                key = key.decode('utf-8')
            if isinstance(key, str):
                key = key.rstrip('\x00')
        return key

    def _initialize_new_tree(self):
        with open(self.file_path, 'wb') as f:
            f.write(b'\x00' * 8192)

        root = LeafNode()
        root.node_id = self.FIRST_DATA_NODE_ID
        root.parent_node_id = None
        root.prev_leaf_id = None
        root.next_leaf_id = None

        self._metadata_dirty = True

    def _load_tree_metadata(self):
        try:
            with open(self.file_path, 'rb') as f:
                f.seek(0)
                metadata_bytes = f.read(8192)

                if len(metadata_bytes) < 24 or metadata_bytes == b'\x00' * 8192:
                    self.root_node_id = self.FIRST_DATA_NODE_ID
                    self.next_available_node_id = self.FIRST_DATA_NODE_ID + 1
                    return

                magic = struct.unpack('4s', metadata_bytes[0:4])[0]
                if magic != b'BPT+':
                    self.root_node_id = self.FIRST_DATA_NODE_ID
                    self.next_available_node_id = self.FIRST_DATA_NODE_ID + 1
                    return

                version, root_id, next_id, order, record_size = struct.unpack('iiiii', metadata_bytes[4:24])

                self.root_node_id = root_id
                self.next_available_node_id = next_id
                self.index_record_size = record_size

                offset = 24
                if offset + 12 > len(metadata_bytes):
                    return

                num_fields, key_type_len = struct.unpack('ii', metadata_bytes[offset:offset+8])
                offset += 8

                if key_type_len > 0:
                    if offset + key_type_len + 4 > len(metadata_bytes):
                        offset = 28
                        num_fields = struct.unpack('i', metadata_bytes[24:28])[0]
                    else:
                        self.key_type = metadata_bytes[offset:offset+key_type_len].decode('utf-8')
                        offset += key_type_len
                        self.key_size, = struct.unpack('i', metadata_bytes[offset:offset+4])
                        offset += 4
                else:
                    offset = 28
                    num_fields = struct.unpack('i', metadata_bytes[24:28])[0]

                self.value_type_size = []
                for i in range(num_fields):
                    if offset + 4 > len(metadata_bytes):
                        return

                    field_name_len, = struct.unpack('i', metadata_bytes[offset:offset+4])
                    offset += 4

                    if offset + field_name_len > len(metadata_bytes):
                        return

                    field_name = metadata_bytes[offset:offset+field_name_len].decode('utf-8')
                    offset += field_name_len

                    if offset + 4 > len(metadata_bytes):
                        return

                    field_type_len, = struct.unpack('i', metadata_bytes[offset:offset+4])
                    offset += 4

                    if offset + field_type_len > len(metadata_bytes):
                        return

                    field_type = metadata_bytes[offset:offset+field_type_len].decode('utf-8')
                    offset += field_type_len

                    if offset + 4 > len(metadata_bytes):
                        return

                    field_size, = struct.unpack('i', metadata_bytes[offset:offset+4])
                    offset += 4
                    
                    self.value_type_size.append((field_name, field_type, field_size))
                
                self.index_record_class = IndexRecord

                if self.key_type is None:
                    for field_name, field_type, field_size in self.value_type_size:
                        if field_name == "index_value":
                            self.key_type = field_type
                            self.key_size = field_size
                            break

                self._calculate_node_sizes()

        except Exception as e:
            print(f"Error loading metadata: {e}")
            self.root_node_id = self.FIRST_DATA_NODE_ID
            self.next_available_node_id = self.FIRST_DATA_NODE_ID + 1

    def _persist_metadata(self):
        if self.value_type_size is None or self.index_record_size is None:
            return

        self.performance.track_write()

        try:
            metadata_parts = []

            metadata_parts.append(struct.pack('4siiii',
                b'BPT+', 1, self.root_node_id, self.next_available_node_id, self.order
            ))

            key_type_bytes = self.key_type.encode('utf-8') if self.key_type else b''
            metadata_parts.append(struct.pack('iii',
                self.index_record_size, len(self.value_type_size), len(key_type_bytes)
            ))
            if key_type_bytes:
                metadata_parts.append(key_type_bytes)
            metadata_parts.append(struct.pack('i', self.key_size if self.key_size else 0))

            for field_name, field_type, field_size in self.value_type_size:
                name_bytes = field_name.encode('utf-8')
                type_bytes = field_type.encode('utf-8')

                metadata_parts.append(struct.pack('i', len(name_bytes)))
                metadata_parts.append(name_bytes)
                metadata_parts.append(struct.pack('i', len(type_bytes)))
                metadata_parts.append(type_bytes)
                metadata_parts.append(struct.pack('i', field_size))

            metadata_bytes = b''.join(metadata_parts)

            if len(metadata_bytes) > self.NODE_SIZE:
                raise ValueError(f"Metadata too large: {len(metadata_bytes)} > {self.NODE_SIZE}")

            padded_data = metadata_bytes + b'\x00' * (self.NODE_SIZE - len(metadata_bytes))

            with open(self.file_path, 'r+b') as f:
                f.seek(0)
                f.write(padded_data)
                f.flush()

            self._metadata_dirty = False

        except Exception as e:
            print(f"Error persisting metadata: {e}")
            raise

    def _get_node_offset(self, node_id: int) -> int:
        return node_id * self.NODE_SIZE

    def _read_node(self, node_id: int) -> Optional[Node]:
        if node_id is None or node_id == self.METADATA_NODE_ID:
            return None
        
        if self.NODE_SIZE is None:
            return None

        self.performance.track_read()

        try:
            offset = self._get_node_offset(node_id)

            with open(self.file_path, 'rb') as f:
                f.seek(offset)
                node_bytes = f.read(self.NODE_SIZE)


                if len(node_bytes) < 13 or (node_bytes[0] == 0 and node_bytes[1] == 0):
                    return None

                try:
                    node_type = struct.unpack('?', node_bytes[0:1])[0]
                    num_keys = struct.unpack('i', node_bytes[1:5])[0]
                    node_id_read = struct.unpack('i', node_bytes[5:9])[0]
                    parent_id = struct.unpack('i', node_bytes[9:13])[0]
                except struct.error as e:
                    return None
                
                if parent_id == self.NULL_NODE_ID:
                    parent_id = None

                data_offset = 13
                normalize_key = self.key_type == "CHAR"

                if node_type:
                    return LeafNode.unpack(
                        node_bytes, data_offset, num_keys, node_id_read, parent_id,
                        self._unpack_key, self.key_storage_size, self.index_record_size,
                        self.index_record_class, self.value_type_size, "index_value",
                        self.NULL_NODE_ID, normalize_key
                    )
                else:
                    return InternalNode.unpack(
                        node_bytes, data_offset, num_keys, node_id_read, parent_id,
                        self._unpack_key, self.key_storage_size, normalize_key
                    )

        except Exception as e:
            print(f"Error reading node {node_id}: {e}")
            return None

    def _write_node(self, node_id: int, node: Node):
        if node_id == self.METADATA_NODE_ID:
            raise ValueError("Cannot write data to metadata node (node 0)")

        if self.NODE_SIZE is None:
            return

        self.performance.track_write()

        try:

            if isinstance(node, LeafNode):
                node_bytes = node.pack(self._pack_key, self.index_record_size, self.NULL_NODE_ID)
            else:
                node_bytes = node.pack(self._pack_key, self.NULL_NODE_ID)

            padded_data = node_bytes.ljust(self.NODE_SIZE, b'\x00')

            offset = self._get_node_offset(node_id)

            if not os.path.exists(self.file_path):
                with open(self.file_path, 'wb') as f:
                    f.write(b'\x00' * self.NODE_SIZE)

            current_size = os.path.getsize(self.file_path)
            required_size = (node_id + 1) * self.NODE_SIZE

            if current_size < required_size:
                with open(self.file_path, 'ab') as f:
                    f.write(b'\x00' * (required_size - current_size))

            with open(self.file_path, 'r+b') as f:
                f.seek(offset)
                f.write(padded_data)
                f.flush()

        except Exception as e:
            print(f"Error writing node {node_id}: {e}")
            raise

    def _mark_node_as_deleted(self, node_id: int):
        if node_id == self.METADATA_NODE_ID:
            raise ValueError("Cannot delete metadata node")

        self.performance.track_write()

        try:
            offset = self._get_node_offset(node_id)

            if os.path.exists(self.file_path):
                with open(self.file_path, 'r+b') as f:
                    f.seek(offset)
                    f.write(b'\x00' * self.NODE_SIZE)
                    f.flush()
        except Exception as e:
            print(f"Error deleting node {node_id}: {e}")

    def _allocate_node_id(self) -> int:
        node_id = self.next_available_node_id
        self.next_available_node_id += 1
        self._metadata_dirty = True
        return node_id

    def _flush_metadata_if_needed(self):
        if self._metadata_dirty:
            self._persist_metadata()

    def search(self, key: Any) -> OperationResult:
        self.performance.start_operation()

        if self.NODE_SIZE is None:
            return self.performance.end_operation([])

        key = self._normalize_key(key)

        leaf = self._find_leaf_for_key(key)
        if leaf is None:
            return self.performance.end_operation([])

        primary_keys = []
        pos = bisect.bisect_left(leaf.keys, key)

        while leaf is not None:
            while pos < len(leaf.keys) and leaf.keys[pos] == key:
                primary_keys.append(leaf.index_records[pos].primary_key)
                pos += 1

            if pos >= len(leaf.keys):
                if leaf.next_leaf_id is not None:
                    leaf = self._read_node(leaf.next_leaf_id)
                    if leaf is None:
                        break
                    pos = 0
                else:
                    break 
            elif leaf.keys[pos] > key:
                break
            else:
                break

        return self.performance.end_operation(primary_keys)
    
    def insert(self, index_record: IndexRecord) -> OperationResult:
        self.performance.start_operation()

        try:
            if self.index_record_class is None:
                self._initialize_index_record_info(index_record)
                root = LeafNode()
                root.node_id = self.FIRST_DATA_NODE_ID
                self._write_node(self.FIRST_DATA_NODE_ID, root)
                self._persist_metadata()

            key = self._normalize_key(index_record.index_value)
            success = self._insert_into_tree(self.root_node_id, key, index_record)
            
            self._flush_metadata_if_needed()
            
            return self.performance.end_operation(success)
        except Exception as e:
            return self.performance.end_operation(False)

    def delete(self, secondary_key: Any, primary_key: Any = None) -> OperationResult:
        self.performance.start_operation()
        
        secondary_key = self._normalize_key(secondary_key)

        if primary_key is not None:
            result = self._delete_by_keys(secondary_key, primary_key)
            return self.performance.end_operation(result)
        else:
            result = self._delete_all_by_secondary_key(secondary_key)
            return self.performance.end_operation(result)

    def _delete_by_keys(self, secondary_key: Any, primary_key: Any) -> bool:
        leaf = self._find_leaf_for_key(secondary_key)
        if leaf is None:
            return False

        pos = bisect.bisect_left(leaf.keys, secondary_key)

        while leaf is not None:
            while pos < len(leaf.keys) and leaf.keys[pos] == secondary_key:
                if leaf.index_records[pos].primary_key == primary_key:
                    leaf.keys.pop(pos)
                    leaf.index_records.pop(pos)
                    self._write_node(leaf.node_id, leaf)

                    if leaf.node_id != self.root_node_id and leaf.is_underflow(self.min_keys):
                        self._handle_leaf_underflow(leaf)

                    self._reduce_tree_height_if_needed()
                    self._flush_metadata_if_needed()

                    return True
                pos += 1

            if pos >= len(leaf.keys):
                if leaf.next_leaf_id is not None:
                    leaf = self._read_node(leaf.next_leaf_id)
                    if leaf is None:
                        break
                    pos = 0
                else:
                    break
            elif leaf.keys[pos] > secondary_key:
                break
            else:
                break

        return False

    def _delete_all_by_secondary_key(self, secondary_key: Any) -> List[Any]:
        leaf = self._find_leaf_for_key(secondary_key)
        if leaf is None:
            return []

        deleted_pks = []
        pos = bisect.bisect_left(leaf.keys, secondary_key)

        while leaf is not None:
            indices_to_delete = []

            i = pos
            while i < len(leaf.keys) and leaf.keys[i] == secondary_key:
                indices_to_delete.append(i)
                deleted_pks.append(leaf.index_records[i].primary_key)
                i += 1

            for idx in reversed(indices_to_delete):
                leaf.keys.pop(idx)
                leaf.index_records.pop(idx)
            if indices_to_delete:
                self._write_node(leaf.node_id, leaf)

                if leaf.node_id != self.root_node_id and leaf.is_underflow(self.min_keys):
                    self._handle_leaf_underflow(leaf)

            if i >= len(leaf.keys):
                if leaf.next_leaf_id is not None:
                    next_leaf = self._read_node(leaf.next_leaf_id)
                    if next_leaf is None or len(next_leaf.keys) == 0 or next_leaf.keys[0] > secondary_key:
                        break
                    leaf = next_leaf
                    pos = 0
                else:
                    break
            elif len(leaf.keys) > 0 and leaf.keys[0] > secondary_key:
                break
            else:
                break

        if deleted_pks:
            self._reduce_tree_height_if_needed()
            self._flush_metadata_if_needed()

        return deleted_pks

    def range_search(self, start_key: Any, end_key: Any) -> OperationResult:
        self.performance.start_operation()

        if self.NODE_SIZE is None:
            return self.performance.end_operation([])

        start_key_normalized = self._normalize_key(start_key)
        end_key_normalized = self._normalize_key(end_key)

        results = []
        leaf = self._find_start_leaf_for_range(start_key_normalized)
        leaf_idx = 0
        if leaf is None:
            return self.performance.end_operation([])

        while leaf is not None:
            found_in_leaf = 0
            for i in range(len(leaf.keys)):
                stored_key = leaf.keys[i]
                stored_key_normalized = self._normalize_key(stored_key)
                if stored_key_normalized < start_key_normalized:
                    continue
                if stored_key_normalized > end_key_normalized:
                    break
                results.append(leaf.index_records[i].primary_key)
                found_in_leaf += 1
            if leaf.next_leaf_id is not None:
                next_leaf = self._read_node(leaf.next_leaf_id)
                if next_leaf is None or not next_leaf.keys:
                    break
                min_next_key = self._normalize_key(next_leaf.keys[0])
                if min_next_key <= end_key_normalized:
                    leaf = next_leaf
                    leaf_idx += 1
                else:
                    break
            else:
                break
        return self.performance.end_operation(results)

    def _find_start_leaf_for_range(self, start_key: Any) -> Optional[LeafNode]:
       
        leaf = self._find_leaf_for_key(start_key)
        if leaf is None:
            return None

        
        while leaf.prev_leaf_id is not None:
            prev_leaf = self._read_node(leaf.prev_leaf_id)
            if prev_leaf is None or not prev_leaf.keys:
                break
            
            last_key_prev = self._normalize_key(prev_leaf.keys[-1])
            if last_key_prev >= start_key:
                leaf = prev_leaf
            else:
                break

        return leaf
    def _find_leaf_for_key(self, key: Any) -> Optional[LeafNode]:
        if self.NODE_SIZE is None:
            return None
            
        current_id = self.root_node_id
        
        while True:
            current = self._read_node(current_id)
            
            if current is None:
                return None
            
            if isinstance(current, LeafNode):
                return current
            
            pos = bisect.bisect_right(current.keys, key)
            current_id = current.child_node_ids[pos]

    def _insert_into_tree(self, node_id: int, key: Any, index_record: IndexRecord) -> bool:
        node = self._read_node(node_id)

        if isinstance(node, LeafNode):
            return self._insert_into_leaf(node, key, index_record)
        else:
            return self._insert_into_internal(node, key, index_record)

    def _insert_into_leaf(self, leaf: LeafNode, key: Any, index_record: IndexRecord) -> bool:
        pos = 0
        while pos < len(leaf.keys):
            if leaf.keys[pos] > key:
                break
            elif leaf.keys[pos] == key:
                if leaf.index_records[pos].primary_key == index_record.primary_key:
                    return False  
                elif leaf.index_records[pos].primary_key > index_record.primary_key:
                    break
            pos += 1

        leaf.keys.insert(pos, key)
        leaf.index_records.insert(pos, index_record)
        self._write_node(leaf.node_id, leaf)

        if leaf.is_full(self.max_keys):
            self._split_leaf_node(leaf)
        
        return True

    def _insert_into_internal(self, internal: InternalNode, key: Any, index_record: IndexRecord) -> bool:
        pos = bisect.bisect_right(internal.keys, key)
        child_id = internal.child_node_ids[pos]
        return self._insert_into_tree(child_id, key, index_record)

    def _split_leaf_node(self, leaf: LeafNode):
        new_leaf = LeafNode()
        new_leaf.node_id = self._allocate_node_id()
        new_leaf.parent_node_id = leaf.parent_node_id

        mid = len(leaf.keys) // 2
        
        if mid < self.min_keys:
            mid = self.min_keys
        elif len(leaf.keys) - mid < self.min_keys:
            mid = len(leaf.keys) - self.min_keys
        
        split_key = leaf.keys[mid]
        
        while mid < len(leaf.keys) - 1 and leaf.keys[mid] == split_key:
            if len(leaf.keys) - (mid + 1) >= self.min_keys:
                mid += 1
            else:
                break

        new_leaf.keys = leaf.keys[mid:]
        new_leaf.index_records = leaf.index_records[mid:]

        new_leaf.next_leaf_id = leaf.next_leaf_id
        new_leaf.prev_leaf_id = leaf.node_id
        leaf.next_leaf_id = new_leaf.node_id

        if new_leaf.next_leaf_id is not None:
            next_leaf = self._read_node(new_leaf.next_leaf_id)
            if next_leaf: 
                next_leaf.prev_leaf_id = new_leaf.node_id
                self._write_node(next_leaf.node_id, next_leaf)

        leaf.keys = leaf.keys[:mid]
        leaf.index_records = leaf.index_records[:mid]

        self._write_node(leaf.node_id, leaf)
        self._write_node(new_leaf.node_id, new_leaf)

        promote_key = new_leaf.keys[0]
        self._promote_key_to_parent(leaf, promote_key, new_leaf.node_id)
        

    def _split_internal_node(self, internal: InternalNode):
        new_internal = InternalNode()
        new_internal.node_id = self._allocate_node_id()
        new_internal.parent_node_id = internal.parent_node_id

        mid = len(internal.keys) // 2
        promote_key = internal.keys[mid]

        new_internal.keys = internal.keys[mid + 1:]
        new_internal.child_node_ids = internal.child_node_ids[mid + 1:]

        internal.keys = internal.keys[:mid]
        internal.child_node_ids = internal.child_node_ids[:mid + 1]

        for child_id in new_internal.child_node_ids:
            child = self._read_node(child_id)
            child.parent_node_id = new_internal.node_id
            self._write_node(child_id, child)

        for child_id in internal.child_node_ids:
            child = self._read_node(child_id)
            child.parent_node_id = internal.node_id
            self._write_node(child_id, child)

        self._write_node(internal.node_id, internal)
        self._write_node(new_internal.node_id, new_internal)


        self._promote_key_to_parent(internal, promote_key, new_internal.node_id)

    def _find_rightmost_leaf_in_subtree(self, node_id: int) -> Optional[LeafNode]:
        node = self._read_node(node_id)

        while isinstance(node, InternalNode):
            node = self._read_node(node.child_node_ids[-1])

        return node

    def _find_leftmost_leaf_in_subtree(self, node_id: int) -> Optional[LeafNode]:
        node = self._read_node(node_id)

        while isinstance(node, InternalNode):
            node = self._read_node(node.child_node_ids[0])

        return node

    def _rebuild_entire_leaf_chain(self):
        if self.NODE_SIZE is None:
            return

        all_leaves = []
        self._collect_leaves_in_order(self.root_node_id, all_leaves)
        
        for i in range(len(all_leaves) - 1):
            current_leaf = all_leaves[i]
            next_leaf = all_leaves[i + 1]
            
            if current_leaf.keys and next_leaf.keys:
                last_key_current = self._normalize_key(current_leaf.keys[-1])
                first_key_next = self._normalize_key(next_leaf.keys[0])
                
                if last_key_current > first_key_next:
                    all_leaves.sort(key=lambda leaf: self._normalize_key(leaf.keys[0]) if leaf.keys else "")
                    break

        for i in range(len(all_leaves)):
            leaf = all_leaves[i]
            leaf.prev_leaf_id = all_leaves[i - 1].node_id if i > 0 else None
            leaf.next_leaf_id = all_leaves[i + 1].node_id if i < len(all_leaves) - 1 else None
            self._write_node(leaf.node_id, leaf)
        
    def _collect_leaves_in_order(self, node_id: int, leaves_list: list):

        node = self._read_node(node_id)

        if isinstance(node, InternalNode):
            for child_id in node.child_node_ids:
                self._collect_leaves_in_order(child_id, leaves_list)
        else:
            leaves_list.append(node)

    def _promote_key_to_parent(self, left_child: Node, key: Any, right_child_id: int):
        if left_child.parent_node_id is None:
            new_root = InternalNode()
            new_root.node_id = self._allocate_node_id()
            new_root.parent_node_id = None
            new_root.keys = [key]
            new_root.child_node_ids = [left_child.node_id, right_child_id]

            left_child.parent_node_id = new_root.node_id
            right_child = self._read_node(right_child_id)
            right_child.parent_node_id = new_root.node_id

            self._write_node(left_child.node_id, left_child)
            self._write_node(right_child_id, right_child)
            self._write_node(new_root.node_id, new_root)

            self.root_node_id = new_root.node_id
            self._metadata_dirty = True
        else:
            parent = self._read_node(left_child.parent_node_id)

            if not isinstance(parent, InternalNode):
                raise ValueError(f"Parent must be internal node, got {type(parent)}")

            pos = bisect.bisect_left(parent.keys, key)
            parent.keys.insert(pos, key)
            parent.child_node_ids.insert(pos + 1, right_child_id)

            right_child = self._read_node(right_child_id)
            right_child.parent_node_id = parent.node_id
            self._write_node(right_child_id, right_child)

            self._write_node(left_child.node_id, left_child)
            self._write_node(parent.node_id, parent)

            if parent.is_full(self.max_keys):
                self._split_internal_node(parent)

    def _reduce_tree_height_if_needed(self):
        root = self._read_node(self.root_node_id)

        if isinstance(root, InternalNode) and len(root.keys) == 0:
            if len(root.child_node_ids) > 0:
                old_root_id = root.node_id
                self.root_node_id = root.child_node_ids[0]

                new_root = self._read_node(self.root_node_id)
                new_root.parent_node_id = None
                self._write_node(self.root_node_id, new_root)

                self._metadata_dirty = True
                self._mark_node_as_deleted(old_root_id)

    def _handle_leaf_underflow(self, leaf: LeafNode):
        if leaf.parent_node_id is None:
            return

        parent = self._read_node(leaf.parent_node_id)
        leaf_index = parent.child_node_ids.index(leaf.node_id)

        if leaf_index > 0:
            left_sibling_id = parent.child_node_ids[leaf_index - 1]
            left_sibling = self._read_node(left_sibling_id)
            if isinstance(left_sibling, LeafNode) and len(left_sibling.keys) > self.min_keys:
                self._borrow_from_left_leaf(leaf, left_sibling, parent, leaf_index)
                return

        if leaf_index < len(parent.child_node_ids) - 1:
            right_sibling_id = parent.child_node_ids[leaf_index + 1]
            right_sibling = self._read_node(right_sibling_id)
            if isinstance(right_sibling, LeafNode) and len(right_sibling.keys) > self.min_keys:
                self._borrow_from_right_leaf(leaf, right_sibling, parent, leaf_index)
                return

        if leaf_index > 0:
            left_sibling_id = parent.child_node_ids[leaf_index - 1]
            left_sibling = self._read_node(left_sibling_id)
            if isinstance(left_sibling, LeafNode):
                self._merge_leaf_with_left(leaf, left_sibling, parent, leaf_index)
        else:
            right_sibling_id = parent.child_node_ids[leaf_index + 1]
            right_sibling = self._read_node(right_sibling_id)
            if isinstance(right_sibling, LeafNode):
                self._merge_leaf_with_right(leaf, right_sibling, parent, leaf_index)

    def _borrow_from_left_leaf(self, leaf: LeafNode, left_sibling: LeafNode,
                                parent: InternalNode, leaf_index: int):
        borrowed_key = left_sibling.keys.pop()
        borrowed_record = left_sibling.index_records.pop()

        leaf.keys.insert(0, borrowed_key)
        leaf.index_records.insert(0, borrowed_record)

        parent.keys[leaf_index - 1] = leaf.keys[0]

        self._write_node(left_sibling.node_id, left_sibling)
        self._write_node(leaf.node_id, leaf)
        self._write_node(parent.node_id, parent)

    def _borrow_from_right_leaf(self, leaf: LeafNode, right_sibling: LeafNode,
                                 parent: InternalNode, leaf_index: int):
        borrowed_key = right_sibling.keys.pop(0)
        borrowed_record = right_sibling.index_records.pop(0)

        leaf.keys.append(borrowed_key)
        leaf.index_records.append(borrowed_record)

        parent.keys[leaf_index] = right_sibling.keys[0]

        self._write_node(right_sibling.node_id, right_sibling)
        self._write_node(leaf.node_id, leaf)
        self._write_node(parent.node_id, parent)

    def _merge_leaf_with_left(self, leaf: LeafNode, left_sibling: LeafNode,
                               parent: InternalNode, leaf_index: int):
        left_sibling.keys.extend(leaf.keys)
        left_sibling.index_records.extend(leaf.index_records)

        left_sibling.next_leaf_id = leaf.next_leaf_id
        if leaf.next_leaf_id is not None:
            next_leaf = self._read_node(leaf.next_leaf_id)
            if next_leaf:
                next_leaf.prev_leaf_id = left_sibling.node_id
                self._write_node(next_leaf.node_id, next_leaf)

        parent.child_node_ids.pop(leaf_index)
        parent.keys.pop(leaf_index - 1)

        self._write_node(left_sibling.node_id, left_sibling)
        self._write_node(parent.node_id, parent)
        self._mark_node_as_deleted(leaf.node_id)

        if parent.node_id != self.root_node_id and parent.is_underflow(self.min_keys):
            self._handle_internal_underflow(parent)

    def _merge_leaf_with_right(self, leaf: LeafNode, right_sibling: LeafNode,
                                parent: InternalNode, leaf_index: int):
        leaf.keys.extend(right_sibling.keys)
        leaf.index_records.extend(right_sibling.index_records)

        leaf.next_leaf_id = right_sibling.next_leaf_id
        if right_sibling.next_leaf_id is not None:
            next_leaf = self._read_node(right_sibling.next_leaf_id)
            if next_leaf:
                next_leaf.prev_leaf_id = leaf.node_id
                self._write_node(next_leaf.node_id, next_leaf)

        parent.child_node_ids.pop(leaf_index + 1)
        parent.keys.pop(leaf_index)

        self._write_node(leaf.node_id, leaf)
        self._write_node(parent.node_id, parent)
        self._mark_node_as_deleted(right_sibling.node_id)

        if parent.node_id != self.root_node_id and parent.is_underflow(self.min_keys):
            self._handle_internal_underflow(parent)

    def _handle_internal_underflow(self, internal: InternalNode):
        if internal.parent_node_id is None:
            return

        parent = self._read_node(internal.parent_node_id)
        internal_index = parent.child_node_ids.index(internal.node_id)

        if internal_index > 0:
            left_sibling_id = parent.child_node_ids[internal_index - 1]
            left_sibling = self._read_node(left_sibling_id)
            if isinstance(left_sibling, InternalNode) and len(left_sibling.keys) > self.min_keys:
                self._borrow_from_left_internal(internal, left_sibling, parent, internal_index)
                return

        if internal_index < len(parent.child_node_ids) - 1:
            right_sibling_id = parent.child_node_ids[internal_index + 1]
            right_sibling = self._read_node(right_sibling_id)
            if isinstance(right_sibling, InternalNode) and len(right_sibling.keys) > self.min_keys:
                self._borrow_from_right_internal(internal, right_sibling, parent, internal_index)
                return

        if internal_index > 0:
            left_sibling_id = parent.child_node_ids[internal_index - 1]
            left_sibling = self._read_node(left_sibling_id)
            if isinstance(left_sibling, InternalNode):
                self._merge_internal_with_left(internal, left_sibling, parent, internal_index)
        else:
            right_sibling_id = parent.child_node_ids[internal_index + 1]
            right_sibling = self._read_node(right_sibling_id)
            if isinstance(right_sibling, InternalNode):
                self._merge_internal_with_right(internal, right_sibling, parent, internal_index)

    def _borrow_from_left_internal(self, internal: InternalNode, left_sibling: InternalNode,
                                    parent: InternalNode, internal_index: int):
        separator_key = parent.keys[internal_index - 1]
        internal.keys.insert(0, separator_key)

        borrowed_child_id = left_sibling.child_node_ids.pop()
        internal.child_node_ids.insert(0, borrowed_child_id)

        borrowed_child = self._read_node(borrowed_child_id)
        borrowed_child.parent_node_id = internal.node_id
        self._write_node(borrowed_child_id, borrowed_child)

        parent.keys[internal_index - 1] = left_sibling.keys.pop()

        self._write_node(left_sibling.node_id, left_sibling)
        self._write_node(internal.node_id, internal)
        self._write_node(parent.node_id, parent)

    def _borrow_from_right_internal(self, internal: InternalNode, right_sibling: InternalNode,
                                     parent: InternalNode, internal_index: int):
        separator_key = parent.keys[internal_index]
        internal.keys.append(separator_key)

        borrowed_child_id = right_sibling.child_node_ids.pop(0)
        internal.child_node_ids.append(borrowed_child_id)

        borrowed_child = self._read_node(borrowed_child_id)
        borrowed_child.parent_node_id = internal.node_id
        self._write_node(borrowed_child_id, borrowed_child)

        parent.keys[internal_index] = right_sibling.keys.pop(0)

        self._write_node(right_sibling.node_id, right_sibling)
        self._write_node(internal.node_id, internal)
        self._write_node(parent.node_id, parent)

    def _merge_internal_with_left(self, internal: InternalNode, left_sibling: InternalNode,
                                   parent: InternalNode, internal_index: int):
        separator_key = parent.keys[internal_index - 1]

        left_sibling.keys.append(separator_key)
        left_sibling.keys.extend(internal.keys)
        left_sibling.child_node_ids.extend(internal.child_node_ids)

        for child_id in internal.child_node_ids:
            child = self._read_node(child_id)
            child.parent_node_id = left_sibling.node_id
            self._write_node(child_id, child)

        parent.child_node_ids.pop(internal_index)
        parent.keys.pop(internal_index - 1)

        self._write_node(left_sibling.node_id, left_sibling)
        self._write_node(parent.node_id, parent)
        self._mark_node_as_deleted(internal.node_id)

        if parent.node_id != self.root_node_id and parent.is_underflow(self.min_keys):
            self._handle_internal_underflow(parent)

    def _merge_internal_with_right(self, internal: InternalNode, right_sibling: InternalNode,
                                    parent: InternalNode, internal_index: int):
        separator_key = parent.keys[internal_index]

        internal.keys.append(separator_key)
        internal.keys.extend(right_sibling.keys)
        internal.child_node_ids.extend(right_sibling.child_node_ids)

        for child_id in right_sibling.child_node_ids:
            child = self._read_node(child_id)
            child.parent_node_id = internal.node_id
            self._write_node(child_id, child)

        parent.child_node_ids.pop(internal_index + 1)
        parent.keys.pop(internal_index)

        self._write_node(internal.node_id, internal)
        self._write_node(parent.node_id, parent)
        self._mark_node_as_deleted(right_sibling.node_id)

        if parent.node_id != self.root_node_id and parent.is_underflow(self.min_keys):
            self._handle_internal_underflow(parent)

    def drop_index(self):
        if os.path.exists(self.file_path):
            os.remove(self.file_path)
            return [self.file_path]
        return []

    def clear(self):
        if os.path.exists(self.file_path):
            os.remove(self.file_path)

        self.root_node_id = self.FIRST_DATA_NODE_ID
        self.next_available_node_id = self.FIRST_DATA_NODE_ID + 1
        self._metadata_dirty = False
        self.index_record_class = None
        self.value_type_size = None
        self.index_record_size = None
        self.key_type = None
        self.key_size = None
        self.key_storage_size = None

        self._initialize_new_tree()

    def get_total_nodes(self) -> int:
        if not os.path.exists(self.file_path):
            return 0
        return os.path.getsize(self.file_path) // self.NODE_SIZE if hasattr(self, 'NODE_SIZE') else 0

    def get_file_info(self) -> dict:
        if not os.path.exists(self.file_path):
            return {"exists": False}

        file_size = os.path.getsize(self.file_path)
        
        if not hasattr(self, 'NODE_SIZE') or self.NODE_SIZE is None:
            return {
                "exists": True,
                "file_path": self.file_path,
                "file_size_bytes": file_size,
                "file_size_kb": file_size / 1024,
                "status": "Not initialized"
            }

        total_nodes = file_size // self.NODE_SIZE

        return {
            "exists": True,
            "file_path": self.file_path,
            "file_size_bytes": file_size,
            "file_size_kb": file_size / 1024,
            "node_size_bytes": self.NODE_SIZE,
            "internal_node_size": self.internal_node_size,
            "leaf_node_size": self.leaf_node_size,
            "index_record_size": self.index_record_size,
            "total_nodes": total_nodes,
            "allocated_nodes": self.next_available_node_id,
            "utilization_ratio": f"{(self.next_available_node_id / total_nodes * 100):.1f}%" if total_nodes > 0 else "0%"
        }

    def get_tree_info(self) -> dict:
        if not hasattr(self, 'NODE_SIZE') or self.NODE_SIZE is None:
            return {
                "order": self.order,
                "max_keys_per_node": self.max_keys,
                "min_keys_per_node": self.min_keys,
                "root_node_id": self.root_node_id,
                "next_available_node_id": self.next_available_node_id,
                "index_column": self.index_column,
                "status": "Not initialized"
            }

        return {
            "order": self.order,
            "max_keys_per_node": self.max_keys,
            "min_keys_per_node": self.min_keys,
            "root_node_id": self.root_node_id,
            "next_available_node_id": self.next_available_node_id,
            "index_column": self.index_column,
            "key_type": self.key_type,
            "key_storage_size": self.key_storage_size
        }

    def warm_up(self):
        if self.NODE_SIZE is None or not os.path.exists(self.file_path):
            return

        try:
            
            self._rebuild_entire_leaf_chain()

            _ = self._read_node(self.root_node_id)

            if self.key_type == "INT":
                dummy_key = -999999
            elif self.key_type == "FLOAT":
                dummy_key = -999999.0
            else:
                dummy_key = "\x00"

            try:
                leaf = self._find_leaf_for_key(dummy_key)
                if leaf:
                    import bisect
                    _ = bisect.bisect_left(leaf.keys, dummy_key)
            except:
                pass

            from ..core.performance_tracker import PerformanceTracker
            self.performance = PerformanceTracker()
        except:
            pass
