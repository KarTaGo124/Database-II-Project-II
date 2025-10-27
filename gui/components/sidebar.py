import streamlit as st
from services.database_service import DatabaseService
from services.state_manager import StateManager
def render_sidebar(db_service: DatabaseService, state_manager: StateManager):
    st.sidebar.title("🗄️ Base de Datos")
    st.sidebar.divider()
    try:
        tables = list(db_service.get_db().list_tables())
    except Exception:
        tables = []
    selected_table = state_manager.get_selected_table()
    if selected_table:
        if st.sidebar.button("⬅️ Volver al Menú Principal", width="stretch", type="primary"):
            state_manager.clear_selection()
            st.rerun()
        st.sidebar.divider()
    if not tables:
        st.sidebar.info("📭 No hay tablas\n\nEjecuta CREATE TABLE para comenzar")
        return
    st.sidebar.markdown("### 📊 Tablas")
    for table_name in tables:
        is_selected = selected_table == table_name
        if st.sidebar.button(
            f"{'📂' if is_selected else '📄'} {table_name}",
            key=f"tbl_{table_name}",
            width="stretch",
            type="primary" if is_selected else "secondary"
        ):
            state_manager.set_selected_table(table_name)
            st.rerun()
