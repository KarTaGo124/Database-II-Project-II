import streamlit as st
import pandas as pd
from services.database_service import DatabaseService
from utils.formatters import format_record, format_time
def render_table_view(db_service: DatabaseService, table_name: str):
    st.header(f"📊 Tabla: {table_name}")
    info = db_service.get_table_info(table_name)
    if not info:
        st.error("No se pudo obtener información de la tabla")
        return
    tab1, tab2 = st.tabs(["📋 Datos", "ℹ️ Información"])
    with tab1:
        render_table_data(db_service, table_name)
    with tab2:
        render_table_metadata(info)
def render_table_data(db_service: DatabaseService, table_name: str):
    st.subheader("Contenido de la tabla")
    records, exec_time = db_service.get_table_preview(table_name, limit=1000)
    if not records:
        st.info("La tabla está vacía")
        return
    data_dicts = [format_record(rec) for rec in records]
    df = pd.DataFrame(data_dicts)
    st.dataframe(
        df,
        width="stretch",
        hide_index=True
    )
    st.caption(f"**{len(records)} filas** en {format_time(exec_time)}")
def render_table_metadata(info: dict):
    st.subheader("Esquema de la tabla")
    col1, col2, col3 = st.columns(3)
    col1.metric("📊 Total de campos", info.get("field_count", 0))
    col2.metric("🔑 Índice primario", info.get("primary_type", "—"))
    col3.metric("🔍 Índices secundarios", len(info.get("secondary_indexes", {})))
    sec_indexes = info.get("secondary_indexes", {})
    if sec_indexes:
        st.markdown("### Índices Secundarios")
        index_data = [
            {"Campo": field, "Tipo de Índice": idx_type}
            for field, idx_type in sec_indexes.items()
        ]
        df = pd.DataFrame(index_data)
        st.dataframe(df, width="stretch", hide_index=True)
