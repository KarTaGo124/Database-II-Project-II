from typing import Any, List, Optional, Dict
import bisect
import struct
import os
from ..core.record import Record
from ..core.performance_tracker import PerformanceTracker, OperationResult


"""Hello"""
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
        self.records = []
        self.prev_leaf_id = None
        self.next_leaf_id = None

    def pack(self, key_packer, record_size: int, null_id: int) -> bytes:
        parent_id = self.parent_node_id if self.parent_node_id is not None else null_id
        
        data = bytearray()
        data.extend(struct.pack('?', True))
        data.extend(struct.pack('i', len(self.keys)))
        data.extend(struct.pack('i', self.node_id))
        data.extend(struct.pack('i', parent_id))
        
        prev_id = self.prev_leaf_id if self.prev_leaf_id is not None else null_id
        next_id = self.next_leaf_id if self.next_leaf_id is not None else null_id
        data.extend(struct.pack('i', prev_id))
        data.extend(struct.pack('i', next_id))
        
        for i in range(len(self.keys)):
            data.extend(key_packer(self.keys[i]))
            data.extend(self.records[i].pack())
        
        return bytes(data)

    @staticmethod
    def unpack(data: bytes, offset: int, num_keys: int, node_id: int, parent_id: Optional[int],
               key_unpacker, key_storage_size: int, record_size: int, record_class, 
               value_type_size: List, key_column: str, null_id: int, normalize_key: bool) -> 'LeafNode':
        leaf = LeafNode()
        leaf.node_id = node_id
        leaf.parent_node_id = parent_id

        prev_id, next_id = struct.unpack('ii', data[offset:offset+8])
        leaf.prev_leaf_id = None if prev_id == null_id else prev_id
        leaf.next_leaf_id = None if next_id == null_id else next_id
        offset += 8

        leaf.keys = []
        leaf.records = []

        for i in range(num_keys):
            key_bytes = data[offset:offset+key_storage_size]
            key = key_unpacker(key_bytes)
            
            if normalize_key:
                key = key.decode('utf-8').rstrip('\x00')
            
            leaf.keys.append(key)
            offset += key_storage_size

            record_bytes = data[offset:offset+record_size]
            record = record_class.unpack(record_bytes, value_type_size, key_column)
            
            for field_name, field_type, _ in value_type_size:
                if field_type == "CHAR":
                    value = getattr(record, field_name)
                    if isinstance(value, bytes):
                        setattr(record, field_name, value.decode('utf-8').rstrip('\x00'))
            
            leaf.records.append(record)
            offset += record_size

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
        child_format = f'{child_count}i'
        children = struct.unpack(child_format, data[offset:offset+(child_count*4)])
        internal.child_node_ids = list(children)

        return internal


class BPlusTreeClusteredIndex:
    METADATA_NODE_ID = 0
    FIRST_DATA_NODE_ID = 1
    NULL_NODE_ID = -1

    def __init__(self, order: int, key_column: str, file_path: str, record_class, table=None):
        self.key_column = key_column
        self.record_class = record_class
        self.order = order
        self.max_keys = order - 1
        self.min_keys = (order + 1) // 2 - 1
        self.file_path = file_path
        self.data_file = file_path + ".dat"
        self.table = table
        self.performance = PerformanceTracker()

        if table is not None:
            dummy_record = table.record
            self.record_size = dummy_record.RECORD_SIZE
            self.record_format = dummy_record.FORMAT
            self.value_type_size = dummy_record.value_type_size
        else:
            if os.path.exists(self.data_file):
                self._load_record_info_from_metadata()
            else:
                raise ValueError("Either 'table' parameter must be provided or metadata must exist")
        
        self.key_type, self.key_size = self._get_key_type_info()
        self._calculate_node_sizes()

        self.root_node_id = self.FIRST_DATA_NODE_ID
        self.next_available_node_id = self.FIRST_DATA_NODE_ID + 1
        self._metadata_dirty = False

        if not os.path.exists(self.data_file):
            self._initialize_new_tree()
        else:
            self._load_tree_metadata()

    def _load_record_info_from_metadata(self):
        try:
            with open(self.data_file, 'rb') as f:
                f.seek(0)
                metadata_bytes = f.read(self.NODE_SIZE)
                
                magic = struct.unpack('4s', metadata_bytes[0:4])[0]
                if magic != b'BPT+':
                    raise ValueError("Invalid metadata format")
                
                version, root_id, next_id, order, key_col_len = struct.unpack('iiiii', metadata_bytes[4:24])
                offset = 24
                
                key_col_bytes = metadata_bytes[offset:offset+key_col_len]
                self.key_column = key_col_bytes.decode('utf-8')
                offset += key_col_len
                
                record_size, num_fields = struct.unpack('ii', metadata_bytes[offset:offset+8])
                offset += 8
                
                self.record_size = record_size
                
                self.value_type_size = []
                for i in range(num_fields):
                    field_name_len = struct.unpack('i', metadata_bytes[offset:offset+4])[0]
                    offset += 4
                    field_name = metadata_bytes[offset:offset+field_name_len].decode('utf-8')
                    offset += field_name_len
                    
                    field_type_len = struct.unpack('i', metadata_bytes[offset:offset+4])[0]
                    offset += 4
                    field_type = metadata_bytes[offset:offset+field_type_len].decode('utf-8')
                    offset += field_type_len
                    
                    field_size = struct.unpack('i', metadata_bytes[offset:offset+4])[0]
                    offset += 4
                    
                    self.value_type_size.append((field_name, field_type, field_size))
                
                dummy = self.record_class(self.value_type_size, self.key_column)
                self.record_format = dummy.FORMAT
                
        except Exception as e:
            raise ValueError(f"Cannot load record info from metadata: {e}")

    def _get_key_type_info(self) -> tuple:
        for field_name, field_type, field_size in self.value_type_size:
            if field_name == self.key_column:
                return (field_type, field_size)
        raise ValueError(f"Key column '{self.key_column}' not found in record")

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
            (self.max_keys * (self.key_storage_size + self.record_size))
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

    def _initialize_new_tree(self):
        with open(self.data_file, 'wb') as f:
            f.write(b'\x00' * self.NODE_SIZE)

        self._persist_metadata()

        root = LeafNode()
        root.node_id = self.FIRST_DATA_NODE_ID
        root.parent_node_id = None
        root.prev_leaf_id = None
        root.next_leaf_id = None

        self._write_node(self.FIRST_DATA_NODE_ID, root)

    def _load_tree_metadata(self):
        try:
            with open(self.data_file, 'rb') as f:
                f.seek(0)
                metadata_bytes = f.read(self.NODE_SIZE)

                if metadata_bytes == b'\x00' * self.NODE_SIZE:
                    self.root_node_id = self.FIRST_DATA_NODE_ID
                    self.next_available_node_id = self.FIRST_DATA_NODE_ID + 1
                    return

                magic = struct.unpack('4s', metadata_bytes[0:4])[0]
                if magic != b'BPT+':
                    self.root_node_id = self.FIRST_DATA_NODE_ID
                    self.next_available_node_id = self.FIRST_DATA_NODE_ID + 1
                    return

                version, root_id, next_id, order = struct.unpack('iiii', metadata_bytes[4:20])
                
                self.root_node_id = root_id
                self.next_available_node_id = next_id
                
                if not hasattr(self, 'value_type_size') or not self.value_type_size:
                    offset = 20
                    
                    key_col_len = struct.unpack('i', metadata_bytes[offset:offset+4])[0]
                    offset += 4
                    key_col_bytes = metadata_bytes[offset:offset+key_col_len]
                    offset += key_col_len
                    
                    record_size, num_fields = struct.unpack('ii', metadata_bytes[offset:offset+8])
                    offset += 8
                    
                    self.record_size = record_size
                    self.value_type_size = []
                    
                    for i in range(num_fields):
                        field_name_len = struct.unpack('i', metadata_bytes[offset:offset+4])[0]
                        offset += 4
                        field_name = metadata_bytes[offset:offset+field_name_len].decode('utf-8')
                        offset += field_name_len
                        
                        field_type_len = struct.unpack('i', metadata_bytes[offset:offset+4])[0]
                        offset += 4
                        field_type = metadata_bytes[offset:offset+field_type_len].decode('utf-8')
                        offset += field_type_len
                        
                        field_size = struct.unpack('i', metadata_bytes[offset:offset+4])[0]
                        offset += 4
                        
                        self.value_type_size.append((field_name, field_type, field_size))
                    
                    dummy = self.record_class(self.value_type_size, self.key_column)
                    self.record_format = dummy.FORMAT

        except Exception as e:
            print(f"Error loading metadata: {e}")
            self.root_node_id = self.FIRST_DATA_NODE_ID
            self.next_available_node_id = self.FIRST_DATA_NODE_ID + 1

    def _persist_metadata(self):
        self.performance.track_write()

        try:
            metadata_parts = []
            
            metadata_parts.append(struct.pack('4siiii', 
                b'BPT+',
                1,
                self.root_node_id,
                self.next_available_node_id,
                self.order
            ))
            
            key_col_bytes = self.key_column.encode('utf-8')
            metadata_parts.append(struct.pack('i', len(key_col_bytes)))
            metadata_parts.append(key_col_bytes)
            
            metadata_parts.append(struct.pack('ii', self.record_size, len(self.value_type_size)))
            
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

            with open(self.data_file, 'r+b') as f:
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

        self.performance.track_read()

        try:
            offset = self._get_node_offset(node_id)

            with open(self.data_file, 'rb') as f:
                f.seek(offset)
                node_bytes = f.read(self.NODE_SIZE)

                if len(node_bytes) < 13:
                    return None
                
                if node_bytes[0] == 0 and node_bytes[1] == 0:
                    return None

                node_type = node_bytes[0] != 0
                num_keys = struct.unpack('i', node_bytes[1:5])[0]
                node_id_read = struct.unpack('i', node_bytes[5:9])[0]
                parent_id = struct.unpack('i', node_bytes[9:13])[0]
                
                if parent_id == self.NULL_NODE_ID:
                    parent_id = None

                data_offset = 13
                normalize_key = self.key_type == "CHAR"

                if node_type:
                    return LeafNode.unpack(
                        node_bytes, data_offset, num_keys, node_id_read, parent_id,
                        self._unpack_key, self.key_storage_size, self.record_size,
                        self.record_class, self.value_type_size, self.key_column,
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

        self.performance.track_write()

        try:
            if isinstance(node, LeafNode):
                node_bytes = node.pack(self._pack_key, self.record_size, self.NULL_NODE_ID)
            else:
                node_bytes = node.pack(self._pack_key, self.NULL_NODE_ID)

            padded_data = node_bytes + b'\x00' * (self.NODE_SIZE - len(node_bytes))

            offset = self._get_node_offset(node_id)

            if not os.path.exists(self.data_file):
                with open(self.data_file, 'wb') as f:
                    f.write(b'\x00' * self.NODE_SIZE)

            current_size = os.path.getsize(self.data_file)
            required_size = (node_id + 1) * self.NODE_SIZE

            if current_size < required_size:
                with open(self.data_file, 'ab') as f:
                    f.write(b'\x00' * (required_size - current_size))

            with open(self.data_file, 'r+b') as f:
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

            if os.path.exists(self.data_file):
                with open(self.data_file, 'r+b') as f:
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

    def get_key_value(self, record: Record) -> Any:
        key = record.get_field_value(self.key_column)
        return self._normalize_key(key)
    
    def _normalize_key(self, key: Any) -> Any:
        if self.key_type == "CHAR":
            if isinstance(key, bytes):
                return key.decode('utf-8').rstrip('\x00')
            elif isinstance(key, str):
                return key.rstrip('\x00')
        return key

    def search(self, key: Any) -> OperationResult:
        self.performance.start_operation()
        
        key = self._normalize_key(key)

        leaf = self._find_leaf_for_key(key)
        pos = bisect.bisect_left(leaf.keys, key)

        if pos < len(leaf.keys) and leaf.keys[pos] == key:
            record = leaf.records[pos]
            return self.performance.end_operation(record)

        return self.performance.end_operation(None)

    def insert(self, record: Record) -> OperationResult:
        self.performance.start_operation()

        try:
            key = self.get_key_value(record)
            success = self._insert_into_tree(self.root_node_id, key, record)
            
            self._flush_metadata_if_needed()
            
            return self.performance.end_operation(success)
        except ValueError as e:
            return self.performance.end_operation(False)

    def delete(self, key: Any) -> OperationResult:
        self.performance.start_operation()
        
        key = self._normalize_key(key)

        leaf = self._find_leaf_for_key(key)
        pos = bisect.bisect_left(leaf.keys, key)

        if pos >= len(leaf.keys) or leaf.keys[pos] != key:
            return self.performance.end_operation(False)

        leaf.keys.pop(pos)
        leaf.records.pop(pos)
        self._write_node(leaf.node_id, leaf)

        if leaf.node_id != self.root_node_id and leaf.is_underflow(self.min_keys):
            self._handle_leaf_underflow(leaf)

        self._reduce_tree_height_if_needed()
        self._flush_metadata_if_needed()

        return self.performance.end_operation(True)

    def range_search(self, start_key: Any, end_key: Any) -> OperationResult:
        self.performance.start_operation()
        
        start_key = self._normalize_key(start_key)
        end_key = self._normalize_key(end_key)

        results = []
        leaf = self._find_leaf_for_key(start_key)

        pos = bisect.bisect_left(leaf.keys, start_key)

        while leaf is not None:
            for i in range(pos, len(leaf.keys)):
                if leaf.keys[i] > end_key:
                    return self.performance.end_operation(results)
                if leaf.keys[i] >= start_key:
                    results.append(leaf.records[i])

            if leaf.next_leaf_id is not None:
                leaf = self._read_node(leaf.next_leaf_id)
                pos = 0
            else:
                break

        return self.performance.end_operation(results)

    def scan_all(self) -> OperationResult:
        self.performance.start_operation()
        results = []

        current = self._read_node(self.root_node_id)
        while isinstance(current, InternalNode):
            if len(current.child_node_ids) > 0:
                current = self._read_node(current.child_node_ids[0])
            else:
                break

        while current is not None and isinstance(current, LeafNode):
            results.extend(current.records)

            if current.next_leaf_id is not None:
                current = self._read_node(current.next_leaf_id)
            else:
                current = None

        return self.performance.end_operation(results)

    def _find_leaf_for_key(self, key: Any) -> LeafNode:
        current_id = self.root_node_id
        
        while True:
            current = self._read_node(current_id)
            
            if isinstance(current, LeafNode):
                return current
            
            pos = bisect.bisect_right(current.keys, key)
            current_id = current.child_node_ids[pos]

    def _insert_into_tree(self, node_id: int, key: Any, record: Record) -> bool:
        node = self._read_node(node_id)

        if isinstance(node, LeafNode):
            return self._insert_into_leaf(node, key, record)
        else:
            return self._insert_into_internal(node, key, record)

    def _insert_into_leaf(self, leaf: LeafNode, key: Any, record: Record) -> bool:
        pos = bisect.bisect_left(leaf.keys, key)
        if pos < len(leaf.keys) and leaf.keys[pos] == key:
            raise ValueError(f"Duplicate key: {key}")

        leaf.keys.insert(pos, key)
        leaf.records.insert(pos, record)
        self._write_node(leaf.node_id, leaf)

        if leaf.is_full(self.max_keys):
            self._split_leaf_node(leaf)

        return True

    def _insert_into_internal(self, internal: InternalNode, key: Any, record: Record) -> bool:
        pos = bisect.bisect_right(internal.keys, key)
        child_id = internal.child_node_ids[pos]
        return self._insert_into_tree(child_id, key, record)

    def _split_leaf_node(self, leaf: LeafNode):
        new_leaf = LeafNode()
        new_leaf.node_id = self._allocate_node_id()
        new_leaf.parent_node_id = leaf.parent_node_id

        mid = len(leaf.keys) // 2
        new_leaf.keys = leaf.keys[mid:]
        new_leaf.records = leaf.records[mid:]

        new_leaf.next_leaf_id = leaf.next_leaf_id
        new_leaf.prev_leaf_id = leaf.node_id
        leaf.next_leaf_id = new_leaf.node_id

        if new_leaf.next_leaf_id is not None:
            next_leaf = self._read_node(new_leaf.next_leaf_id)
            next_leaf.prev_leaf_id = new_leaf.node_id
            self._write_node(next_leaf.node_id, next_leaf)

        leaf.keys = leaf.keys[:mid]
        leaf.records = leaf.records[:mid]

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

        self._write_node(internal.node_id, internal)
        self._write_node(new_internal.node_id, new_internal)

        self._promote_key_to_parent(internal, promote_key, new_internal.node_id)

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
        borrowed_record = left_sibling.records.pop()

        leaf.keys.insert(0, borrowed_key)
        leaf.records.insert(0, borrowed_record)

        parent.keys[leaf_index - 1] = leaf.keys[0]

        self._write_node(left_sibling.node_id, left_sibling)
        self._write_node(leaf.node_id, leaf)
        self._write_node(parent.node_id, parent)

    def _borrow_from_right_leaf(self, leaf: LeafNode, right_sibling: LeafNode,
                                 parent: InternalNode, leaf_index: int):
        borrowed_key = right_sibling.keys.pop(0)
        borrowed_record = right_sibling.records.pop(0)

        leaf.keys.append(borrowed_key)
        leaf.records.append(borrowed_record)

        parent.keys[leaf_index] = right_sibling.keys[0]

        self._write_node(right_sibling.node_id, right_sibling)
        self._write_node(leaf.node_id, leaf)
        self._write_node(parent.node_id, parent)

    def _merge_leaf_with_left(self, leaf: LeafNode, left_sibling: LeafNode,
                               parent: InternalNode, leaf_index: int):
        left_sibling.keys.extend(leaf.keys)
        left_sibling.records.extend(leaf.records)

        left_sibling.next_leaf_id = leaf.next_leaf_id
        if leaf.next_leaf_id is not None:
            next_leaf = self._read_node(leaf.next_leaf_id)
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
        leaf.records.extend(right_sibling.records)

        leaf.next_leaf_id = right_sibling.next_leaf_id
        if right_sibling.next_leaf_id is not None:
            next_leaf = self._read_node(right_sibling.next_leaf_id)
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

    def warm_up(self):
        _ = self._read_node(self.root_node_id)
        
        if self.key_type == "INT":
            dummy_key = -999999
        elif self.key_type == "FLOAT":
            dummy_key = -999999.0
        else:
            dummy_key = "\x00"
        
        try:
            leaf = self._find_leaf_for_key(dummy_key)
            _ = bisect.bisect_left(leaf.keys, dummy_key)
            
            if not self._metadata_dirty:
                pass
        except:
            pass
        
        self.performance = PerformanceTracker()

    def drop_table(self):
        removed_files = []
        if os.path.exists(self.data_file):
            os.remove(self.data_file)
            removed_files.append(self.data_file)

        self.root_node_id = self.FIRST_DATA_NODE_ID
        self.next_available_node_id = self.FIRST_DATA_NODE_ID + 1
        self._metadata_dirty = False

        self._initialize_new_tree()
        return removed_files

    def get_total_nodes(self) -> int:
        if not os.path.exists(self.data_file):
            return 0

        file_size = os.path.getsize(self.data_file)
        return file_size // self.NODE_SIZE

    def get_file_info(self) -> dict:
        if not os.path.exists(self.data_file):
            return {"exists": False}

        file_size = os.path.getsize(self.data_file)
        total_nodes = file_size // self.NODE_SIZE

        return {
            "exists": True,
            "file_path": self.data_file,
            "file_size_bytes": file_size,
            "file_size_kb": file_size / 1024,
            "node_size_bytes": self.NODE_SIZE,
            "internal_node_size": self.internal_node_size,
            "leaf_node_size": self.leaf_node_size,
            "record_size": self.record_size,
            "total_nodes": total_nodes,
            "allocated_nodes": self.next_available_node_id,
            "utilization_ratio": f"{(self.next_available_node_id / total_nodes * 100):.1f}%" if total_nodes > 0 else "0%"
        }

    def get_tree_info(self) -> dict:
        return {
            "order": self.order,
            "max_keys_per_node": self.max_keys,
            "min_keys_per_node": self.min_keys,
            "root_node_id": self.root_node_id,
            "next_available_node_id": self.next_available_node_id,
            "key_column": self.key_column,
            "key_type": self.key_type,
            "key_storage_size": self.key_storage_size
        }