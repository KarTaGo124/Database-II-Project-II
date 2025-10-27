import sys
import streamlit as st
from pathlib import Path
from typing import List, Dict, Any, Optional
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
from indexes.core.database_manager import DatabaseManager
from sql_parser.parser import parse
from sql_parser.executor import Executor
@st.cache_resource
def get_cached_db_manager(db_base_dir: str) -> DatabaseManager:
    return DatabaseManager(database_name="frontend_db", base_path=db_base_dir)
@st.cache_resource
def get_cached_executor(_db: DatabaseManager) -> Executor:
    return Executor(_db)
class DatabaseService:
    def __init__(self, base_path: Path):
        self.base_path = base_path
        self.db_base_dir = str(base_path / "databases")
    def get_db(self) -> DatabaseManager:
        return get_cached_db_manager(self.db_base_dir)
    def get_executor(self) -> Executor:
        return get_cached_executor(self.get_db())
    def reset(self):
        st.cache_resource.clear()
    def list_tables(self) -> List[str]:
        try:
            return list(self.get_db().list_tables())
        except Exception:
            return []
    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        try:
            return self.get_db().get_table_info(table_name)
        except Exception:
            return None
    def get_table_preview(self, table_name: str, limit: int = 100):
        try:
            result = self.get_db().scan_all(table_name)
            records = result.data[:limit] if result.data else []
            return records, result.execution_time_ms
        except Exception as e:
            return [], 0.0
    def execute_sql(self, sql_text: str) -> List[Dict[str, Any]]:
        results = []
        try:
            plans = parse(sql_text)
        except Exception as e:
            return [{"plan": "ParseError", "error": str(e)}]
        if not plans:
            return [{"plan": "EmptyQuery", "error": "No se generaron planes ejecutables"}]
        executor = self.get_executor()
        for plan in plans:
            plan_name = type(plan).__name__
            try:
                result = executor.execute(plan)
                results.append({"plan": plan_name, "result": result})
            except Exception as e:
                results.append({"plan": plan_name, "error": str(e)})
        return results
