from .plan_types import (
    ColumnType, ColumnDef,
    CreateTablePlan, LoadDataPlan,
    SelectPlan, InsertPlan, DeletePlan,
    CreateIndexPlan, DropTablePlan, DropIndexPlan,
    PredicateEq, PredicateBetween, PredicateInPointRadius, PredicateKNN, PredicateFulltext, PredicateMultimedia,
)

from lark import Lark, Transformer, Token
from typing import Any, List

# Cargar gramática desde archivo grammar.lark
with open(__file__.replace("parser.py", "grammar.lark"), "r", encoding="utf-8") as f:
    _GRAMMAR = f.read()

_PARSER = Lark(_GRAMMAR, start="start", parser="lalr")


# helpers
def _to_int_or_float(s: str) -> Any:
    try:
        if "." in s:
            return float(s)
        return int(s)
    except ValueError:
        return float(s)

def _tok2str(x) -> str:
    if isinstance(x, Token):
        return x.value
    return str(x)


class _T(Transformer):
    # ==== TIPOS ====
    def t_int(self, _):      return ColumnType("INT")
    def t_float(self, _):    return ColumnType("FLOAT")
    def t_date(self, _):     return ColumnType("DATE")
    def t_varchar(self, it): return ColumnType("VARCHAR", int(_tok2str(it[0])))
    def t_array_2d(self, _): return ColumnType("ARRAY", 2)
    def t_array_nd(self, it): return ColumnType("ARRAY", int(_tok2str(it[0])))

    # ==== LITERALES / BÁSICOS ====
    def int_lit(self, items):
        tok = items[0]
        return int(tok.value if isinstance(tok, Token) else str(tok))

    def float_lit(self, items):
        tok = items[0]
        return float(tok.value if isinstance(tok, Token) else str(tok))

    def number(self, items):
        tok = items[0]
        s = tok.value if isinstance(tok, Token) else str(tok)
        return _to_int_or_float(s)

    def string(self, items):
        s = items[0]
        if isinstance(s, Token):
            # para quitar comillas de ESCAPED_STRING
            return s.value[1:-1]
        return str(s)

    def literal(self, items):
        return items[0]

    def null(self, _): return None
    
    def spatial_point(self, items):
        return tuple(items)
    
    def array_lit(self, items):
        return items[0]

    def ident_or_string(self, items):
        x = items[0]
        if isinstance(x, Token):
            if x.type == "IDENT":
                return x.value
            return x.value[1:-1]
        return str(x)

    # ==== LISTAS ====
    def col_list(self, items):
        return [str(x) for x in items]

    # ==== CREATE TABLE ====
    def coldef(self, items):
        name = _tok2str(items[0])
        coltype = items[1]
        is_key = False
        index = None
        VALID = {"SEQUENTIAL", "ISAM", "BTREE", "RTREE", "HASH", "INVERTED_TEXT", "MULTIMEDIA_SEQ", "MULTIMEDIA_INV"}
        for it in items[2:]:
            if it == "KEY":
                is_key = True
                if coltype.kind != "INT":
                    raise ValueError("Only INT columns can be PRIMARY KEY")
            elif it is None:
                continue
            else:
                s = _tok2str(it)
                if s in VALID:
                    index = s
        return ColumnDef(name=name, type=coltype, is_key=is_key, index=index)

    def index_kind(self, items):  # INDEX_KIND -> str
        return str(items[0])

    def create_table(self, items):
        table = _tok2str(items[0])
        columns = items[1:]
        return CreateTablePlan(table=table, columns=columns)

    # ==== LOAD DATA FROM FILE ====
    def load_data(self, items):
        filepath = self.ident_or_string([items[0]])
        table = _tok2str(items[1])
        mappings = None
        if len(items) > 2:
            mappings = {}
            for mapping in items[2:]:
                if mapping is None:
                    continue
                array_field = mapping[0]
                csv_columns = mapping[1]
                mappings[array_field] = csv_columns
        return LoadDataPlan(table=table, filepath=filepath, column_mappings=mappings)
    
    def column_mapping(self, items):
        array_field = _tok2str(items[0])
        csv_columns = [_tok2str(item) for item in items[1:]]
        return (array_field, csv_columns)

    # ==== SELECT ====
    def select_all(self, _): return None
    def select_cols(self, items):
        cols = items[0]
        return cols if isinstance(cols, list) else [str(cols)]

    # punto (x,y)
    def point(self, items):
        coords = [float(item) for item in items]
        return 
    
    def pred_eq(self, items):
        return PredicateEq(column=str(items[0]), value=items[1])

    def pred_between(self, items):
        return PredicateBetween(column=str(items[0]), lo=items[1], hi=items[2])

    def pred_in(self, items):
        col = str(items[0])
        pt = items[1]
        radius = float(items[2])
        return PredicateInPointRadius(column=col, point=pt, radius=radius)

    def pred_nearest(self, items):
        col = str(items[0])
        pt = items[1]            # (x, y)
        k = int(items[2])
        return PredicateKNN(column=col, point=pt, k=k)

    def pred_fulltext(self, items):
        col = str(items[0])
        query = items[1]
        if isinstance(query, Token):
            query = query.value[1:-1]
        return PredicateFulltext(column=col, query=str(query))

    def pred_multimedia(self, items):
        col = str(items[0])
        query_path = items[1]
        if isinstance(query_path, Token):
            query_path = query_path.value[1:-1]
        return PredicateMultimedia(column=col, query_path=str(query_path))

    def select_stmt(self, items):
        cols_or_none = items[0]
        table = _tok2str(items[1])
        where = None
        limit = None
        
        for item in items[2:]:
            if item is None:
                continue
            if isinstance(item, int):
                limit = item
            else:
                where = item
        
        return SelectPlan(table=table, columns=cols_or_none, where=where, limit=limit)

    # ==== INSERT ====
    def insert_stmt(self, items):
        table = _tok2str(items[0])
        rest = [x for x in items[1:] if x is not None]
        if rest and isinstance(rest[0], list):   # con lista de columnas
            cols = rest[0]
            vals = rest[1:]
        else:                                     # sin lista de columnas
            cols = None
            vals = rest

        return InsertPlan(table=table, columns=cols, values=vals)


    # ==== DELETE ====
    def delete_stmt(self, items):
        table = _tok2str(items[0])
        where = items[1] if len(items) > 1 else None
        return DeletePlan(table=table, where=where)

    # ==== CREATE INDEX ====
    def create_index(self, items):
        table = _tok2str(items[0])
        column = _tok2str(items[1])
        index_type = _tok2str(items[2])
        language = "spanish"
        feature_type = "SIFT"
        multimedia_directory = None
        multimedia_pattern = None
        for item in items[3:]:
            if item is None:
                continue
            if isinstance(item, Token):
                val = item.value[1:-1] if item.type == "STRING" else item.value
            else:
                val = str(item)
            if val.upper() in ("SIFT", "MFCC", "ORB", "HOG", "CHROMA", "SPECTRAL"):
                feature_type = val.upper()
            elif val.startswith("{") and "}" in val:
                multimedia_pattern = val
            elif "/" in val or "\\" in val or val.endswith(".jpg") or val.endswith(".png"):
                multimedia_directory = val
            else:
                language = val
        return CreateIndexPlan(index_name=column, table=table, column=column, index_type=index_type, language=language, feature_type=feature_type, multimedia_directory=multimedia_directory, multimedia_pattern=multimedia_pattern)

    # ==== DROP TABLE ====
    def drop_table(self, items):
        table = _tok2str(items[0])
        return DropTablePlan(table=table)

    # ==== DROP INDEX ====
    def drop_index(self, items):
        field_name = _tok2str(items[0])
        table_name = _tok2str(items[1])
        return DropIndexPlan(field_name=field_name, table=table_name)

    def start(self, items):
        return items

_TRANSFORMER = _T()

def parse(sql: str):
    sql = sql.strip().rstrip(";")
    tree = _PARSER.parse(sql)
    res = _TRANSFORMER.transform(tree)
    return res if isinstance(res, list) else [res]