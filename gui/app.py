import sys
import streamlit as st
from pathlib import Path
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
from gui.services.database_service import DatabaseService
from gui.services.state_manager import StateManager
from gui.components.sidebar import render_sidebar
from gui.components.table_view import render_table_view
from gui.components.sql_editor import render_sql_editor
from gui.components.csv_upload import render_csv_upload
from gui.components.documentation import render_documentation
from gui.components.multimedia_search import render_multimedia_search
DATA_DIR = Path(__file__).parent / "data"
def setup_page():
    st.set_page_config(
        page_title="SGBD Multi-Índice",
        layout="wide",
        initial_sidebar_state="expanded",
        page_icon="🗄️"
    )
    st.markdown("""
        <style>
        .main .block-container {
            padding-top: 2rem;
            padding-bottom: 3rem;
        }
        .stTextArea textarea {
            font-family: 'Fira Code', 'Courier New', monospace;
            font-size: 14px;
        }
        h1 {
            color:
            font-weight: 700;
        }
        h2 {
            color:
            font-weight: 600;
            margin-top: 1.5rem;
        }
        </style>
    """, unsafe_allow_html=True)
def main():
    setup_page()
    db_service = DatabaseService(DATA_DIR)
    state_manager = StateManager(DATA_DIR)
    state_manager.initialize_session_state()
    st.title("🗄️ Sistema de Gestión de Base de Datos")
    st.caption("Sistema multi-índice con soporte espacial, fulltext y multimedia (ISAM, BTREE, HASH, RTREE, SEQUENTIAL, INVERTED_TEXT, MULTIMEDIA_SEQ, MULTIMEDIA_INV)")
    render_sidebar(db_service, state_manager)
    selected_table = state_manager.get_selected_table()

    if 'active_tab' not in st.session_state:
        st.session_state['active_tab'] = 0

    if selected_table:
        render_table_view(db_service, selected_table)
    else:
        tab1, tab2, tab3, tab4 = st.tabs(["✏️ Consultas SQL", "🖼️ Búsqueda Multimedia", "📤 Subir CSV", "📚 Documentación"])
        with tab1:
            render_sql_editor(db_service)
        with tab2:
            render_multimedia_search(db_service)
        with tab3:
            render_csv_upload()
        with tab4:
            render_documentation()
if __name__ == "__main__":
    main()
