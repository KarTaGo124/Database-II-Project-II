import struct
from typing import List, Tuple, Dict

class Table:
    def __init__(self, table_name: str, sql_fields: List[Tuple[str, str, int]], key_field: str, extra_fields: Dict[str, Tuple[str, int]] = None):
        self.table_name = table_name
        self.sql_fields = sql_fields
        self.key_field = key_field

        all_fields = sql_fields.copy()
        if extra_fields:
            for field_name, (field_type, field_size) in extra_fields.items():
                all_fields.append((field_name, field_type, field_size))

        self.all_fields = all_fields
        self.record = Record(all_fields, key_field)
        self.record_size = self.record.RECORD_SIZE

class Record:
    def __init__(self, list_of_types: List[Tuple[str, str, int]], key_field: str):
        self.FORMAT = self._make_format(list_of_types)
        self.RECORD_SIZE = struct.calcsize(self.FORMAT)
        self.value_type_size = [(element[0], element[1], element[2]) for element in list_of_types]
        self.key_field = key_field

        for field_name, _, _ in self.value_type_size:
            setattr(self, field_name, None)

    def _make_format(self, list_of_types):
        format_str = ""
        for _, field_type, field_size in list_of_types:
            if field_type == "INT":
                format_str += "i"
            elif field_type == "FLOAT":
                format_str += "f"
            elif field_type == "CHAR":
                format_str += f"{field_size}s"
            elif field_type == "ARRAY":
                format_str += f"{field_size}f"
            elif field_type == "BOOL":
                format_str += "?"
        return format_str

    def set_values(self, **kwargs):
        """Método flexible para asignar valores a cualquier campo"""
        for field_name, value in kwargs.items():
            if hasattr(self, field_name):
                setattr(self, field_name, value)
            else:
                raise AttributeError(f"Campo {field_name} no existe en el registro")
    def pack(self) -> bytes:
        processed_values = []
        for field_name, field_type, field_size in self.value_type_size:
            value = getattr(self, field_name)
            if field_type == "ARRAY":
                if len(value) != field_size:
                    raise ValueError(f"Array debe tener {field_size} dimensiones")
                processed_values.extend(value)
            else:
                processed_values.append(self._process_value(value, field_type, field_size))

        return struct.pack(self.FORMAT, *processed_values)

    def _process_value(self, value, field_type: str, field_size: int):
        if field_type == "CHAR":
            if isinstance(value, bytes):
                return value[:field_size].ljust(field_size, b'\x00')
            else:
                return str(value).ljust(field_size).encode('utf-8')[:field_size]
        elif field_type == "INT":
            return int(value)
        elif field_type == "FLOAT":
            return float(value)
        elif field_type == "BOOL":
            return bool(value)
        return value

    def get_key(self, key_field: str = None):
        if key_field is None:
            key_field = self.key_field
        return getattr(self, key_field)

    def get_spatial_key(self, spatial_field: str):
        return getattr(self, spatial_field)

    def get_field_value(self, field_name: str):
        return getattr(self, field_name)

    def set_field_value(self, field_name: str, value):
        if hasattr(self, field_name):
            setattr(self, field_name, value)
        else:
            raise AttributeError(f"Campo {field_name} no existe")

    @classmethod
    def unpack(cls, data: bytes, list_of_types: List[Tuple[str, str, int]], key_field: str):
        record = cls(list_of_types, key_field)
        unpacked_data = struct.unpack(record.FORMAT, data)

        data_index = 0
        for field_name, field_type, field_size in record.value_type_size:
            if field_type == "ARRAY":
                array_values = unpacked_data[data_index:data_index + field_size]
                setattr(record, field_name, list(array_values))
                data_index += field_size
            else:
                setattr(record, field_name, unpacked_data[data_index])
                data_index += 1

        return record
    
    def __str__(self):
        fields = []
        for field_name, field_type, field_size in self.value_type_size:
            value = getattr(self, field_name)
            if field_type == "CHAR" and value:
                if isinstance(value, bytes):
                    value = value.decode('utf-8').rstrip('\x00').strip()
            fields.append(f"{field_name}: {value}")

        return f"Record({', '.join(fields)})"

    def __repr__(self):
        """Representación técnica del record"""
        return self.__str__()

    def print_detailed(self):
        print(f"Record Details")
        print(f"Key Field: {self.key_field}")
        print(f"Size: {self.RECORD_SIZE} bytes")
        print("Fields:")
        for field_name, field_type, field_size in self.value_type_size:
            value = getattr(self, field_name)
            if field_type == "CHAR" and isinstance(value, bytes):
                value = value.decode('utf-8').rstrip('\x00')
            print(f"  {field_name} ({field_type}[{field_size}]): {value}")


class IndexRecord(Record):
    def __init__(self, index_field_type: str, index_field_size: int):
        list_of_types = [
            ("index_value", index_field_type, index_field_size),
            ("primary_key", "INT", 4)
        ]
        super().__init__(list_of_types, "index_value")

    def set_index_data(self, index_value, primary_key):
        self.index_value = index_value
        self.primary_key = primary_key

    @classmethod
    def unpack(cls, data: bytes, list_of_types: List[Tuple[str, str, int]], key_field: str):
        index_field_type = list_of_types[0][1]
        index_field_size = list_of_types[0][2]
        record = cls(index_field_type, index_field_size)
        unpacked_data = struct.unpack(record.FORMAT, data)

        data_index = 0
        for field_name, field_type, field_size in record.value_type_size:
            if field_type == "ARRAY":
                array_values = unpacked_data[data_index:data_index + field_size]
                setattr(record, field_name, list(array_values))
                data_index += field_size
            else:
                setattr(record, field_name, unpacked_data[data_index])
                data_index += 1

        return record



