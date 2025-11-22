from dataclasses import dataclass
from typing import List, Optional, Tuple, Any, Dict

# Tipos/Columnas

@dataclass
class ColumnType:
    kind: str                 # "INT" | "FLOAT" | "DATE" | "VARCHAR" | "ARRAY_FLOAT"
    length: Optional[int] = None

@dataclass
class ColumnDef:
    name: str
    type: ColumnType
    is_key: bool = False
    index: Optional[str] = None  # "ISAM" | "BTREE" | "RTREE" | "SEQUENTIAL" | "HASH"

# Planes

@dataclass
class CreateTablePlan:
    table: str
    columns: List[ColumnDef]

@dataclass
class LoadDataPlan:
    table: str
    filepath: str
    column_mappings: Optional[Dict[str, List[str]]] = None

@dataclass
class PredicateEq:
    column: str
    value: Any

@dataclass
class PredicateBetween:
    column: str
    lo: Any
    hi: Any

@dataclass
class PredicateInPointRadius:
    column: str
    point: Tuple[float, ...]
    radius: float

@dataclass
class PredicateKNN:
    column: str
    point: Tuple[float, ...]
    k: int

@dataclass
class PredicateFulltext:
    column: str
    query: str

@dataclass
class PredicateMultimedia:
    column: str
    query_path: str

@dataclass
class SelectPlan:
    table: str
    columns: Optional[List[str]]
    where: Optional[Any]
    limit: Optional[int] = None

@dataclass
class InsertPlan:
    table: str
    columns: Optional[List[str]]
    values: List[Any]

@dataclass
class DeletePlan:
    table: str
    where: Any

@dataclass
class CreateIndexPlan:
    index_name: str
    table: str
    column: str
    index_type: str
    language: str = "spanish"
    feature_type: str = "SIFT"

@dataclass
class DropTablePlan:
    table: str

@dataclass
class DropIndexPlan:
    field_name: str
    table: str