import os
import streamlit as st
from pathlib import Path
from typing import Optional
class StateManager:
    def __init__(self, base_path: Path):
        self.base_path = base_path
        self.db_path = base_path / "databases" / "frontend_db"
    def get_existing_tables(self):
        if not self.db_path.exists():
            return []
        tables = []
        try:
            for item in self.db_path.iterdir():
                if item.is_dir() and not item.name.startswith('.'):
                    tables.append(item.name)
        except Exception:
            pass
        return sorted(tables)
    def get_selected_table(self) -> Optional[str]:
        return st.session_state.get('selected_table')
    def set_selected_table(self, table_name: Optional[str]):
        st.session_state['selected_table'] = table_name
    def clear_selection(self):
        st.session_state['selected_table'] = None
    def initialize_session_state(self):
        if 'selected_table' not in st.session_state:
            st.session_state['selected_table'] = None
        if 'show_docs' not in st.session_state:
            st.session_state['show_docs'] = False
        if 'last_sql' not in st.session_state:
            st.session_state['last_sql'] = ""
        if 'query_results' not in st.session_state:
            st.session_state['query_results'] = None

        if 'sql_editor_initialized' not in st.session_state:
            default_sql = """CREATE TABLE Restaurantes (
    id INT KEY INDEX BTREE,
    nombre VARCHAR[100] INDEX BTREE,
    ubicacion ARRAY[FLOAT] INDEX RTREE,
    rating FLOAT INDEX HASH,
    precio_promedio FLOAT,
    fecha_apertura VARCHAR[20],
    ciudad VARCHAR[50]
);
LOAD DATA FROM FILE "data/datasets/restaurantes.csv" INTO Restaurantes
WITH MAPPING (ubicacion = ARRAY(latitud, longitud));
SELECT * FROM Restaurantes
WHERE ubicacion NEAREST ((-34.6037, -58.3816), 10);"""
            st.session_state['sql_editor'] = default_sql
            st.session_state['sql_editor_initialized'] = True
