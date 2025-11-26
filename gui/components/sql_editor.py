import streamlit as st
import pandas as pd
from typing import List, Dict, Any
from pathlib import Path
from services.database_service import DatabaseService
from utils.formatters import format_record, format_time
from components.multimedia_results import render_multimedia_results, is_multimedia_query
def render_sql_editor(db_service: DatabaseService):
    st.header("✏️ Editor SQL")

    sql_text = st.text_area(
        "Escribe tus consultas SQL (separadas por `;`)",
        value=st.session_state.get("sql_editor", ""),
        height=300,
        key="sql_editor_widget"
    )

    st.session_state["sql_editor"] = sql_text

    col1, col2, col3 = st.columns([2, 2, 8])
    with col1:
        execute_btn = st.button("▶️ Ejecutar", type="primary", width="stretch", key="execute_btn")
    with col2:
        if st.button("🗑️ Limpiar", width="stretch", key="clear_btn"):
            st.session_state["sql_editor"] = ""
            st.session_state["query_results"] = None
            st.rerun()

    if execute_btn and sql_text.strip():
        with st.spinner("Ejecutando consultas..."):
            results = db_service.execute_sql(sql_text)
            st.session_state["query_results"] = results

            drop_operations = {"DropTablePlan", "DropIndexPlan"}
            has_drop = any(item.get("plan") in drop_operations for item in results)
            if has_drop:
                db_service.reset()

        st.rerun()

    elif execute_btn and not sql_text.strip():
        st.warning("⚠️ No hay sentencias para ejecutar")

    if st.session_state["query_results"]:
        st.divider()
        render_query_results(st.session_state["query_results"])
def render_query_results(results: List[Dict[str, Any]]):
    st.subheader("📊 Resultados")
    for i, item in enumerate(results, 1):
        plan_name = item.get("plan", "Unknown")
        icon = get_operation_icon(plan_name)
        with st.expander(f"{icon} {plan_name}", expanded=True):
            if "error" in item:
                st.error(item["error"])
                continue
            result = item["result"]
            render_single_result(result, plan_name)
def render_single_result(result, plan_name: str):
    data = getattr(result, "data", None)
    exec_time = getattr(result, "execution_time_ms", 0.0) or 0.0
    reads = getattr(result, "disk_reads", 0) or 0
    writes = getattr(result, "disk_writes", 0) or 0

    if isinstance(data, list):
        if len(data) > 0:
            if is_multimedia_query(data):
                images_dir = Path(__file__).resolve().parents[2] / "data" / "images"
                render_multimedia_results(data, images_dir=images_dir)
                render_postgresql_footer(len(data), exec_time, reads, writes)
            elif hasattr(data[0], "value_type_size"):
                data_dicts = [format_record(rec) for rec in data]
                df = pd.DataFrame(data_dicts)
                st.dataframe(
                    df,
                    width="stretch",
                    hide_index=True
                )
                render_postgresql_footer(len(df), exec_time, reads, writes)
            else:
                st.dataframe(
                    pd.DataFrame(data),
                    width="stretch",
                    hide_index=True
                )
                render_postgresql_footer(len(data), exec_time, reads, writes)
        else:
            st.info("ℹ️ Consulta ejecutada correctamente")
            render_postgresql_footer(0, exec_time, reads, writes, show_rows=True)

    elif isinstance(data, str):
        render_message_with_context(data, exec_time, reads, writes)

    elif data is True or data == "OK":
        st.success("✅ Operación completada exitosamente")
        render_postgresql_footer(0, exec_time, reads, writes, show_rows=False)

    elif data is False:
        st.error("❌ Operación fallida")
        render_postgresql_footer(0, exec_time, reads, writes, show_rows=False)

    else:
        st.info("✅ Operación completada")
        render_postgresql_footer(0, exec_time, reads, writes, show_rows=False)
def render_message_with_context(message: str, exec_time: float, reads: int, writes: int):
    import re

    if message.startswith("ERROR:"):
        st.error(f"❌ {message}")
    elif "CSV cargado:" in message or "insertados=" in message:
        match = re.search(r'insertados=(\d+)', message)
        duplicates_match = re.search(r'duplicados=(\d+)', message)
        errors_match = re.search(r'cast_err=(\d+)', message)

        inserted = int(match.group(1)) if match else 0
        duplicates = int(duplicates_match.group(1)) if duplicates_match else 0
        errors = int(errors_match.group(1)) if errors_match else 0

        if errors > 0 and inserted == 0:
            st.error(f"❌ {message}")
        elif errors > 0:
            st.warning(f"⚠️ {message}")
        elif inserted > 0 and duplicates == 0:
            st.success(f"✅ {message}")
        elif inserted > 0 and duplicates > 0:
            st.warning(f"⚠️ {message}")
        elif inserted == 0 and duplicates > 0:
            st.warning(f"⚠️ {message}")
        else:
            st.info(f"ℹ️ {message}")

    elif message == "Duplicado/No insertado":
        st.warning(f"⚠️ {message}")

    elif "OK (" in message and "registros)" in message:
        match = re.search(r'\((\d+) registros\)', message)
        count = int(match.group(1)) if match else 0
        if count > 0:
            st.success(f"✅ {message}")
        else:
            st.info(f"ℹ️ {message}")

    elif message.startswith("OK:") or message == "OK":
        st.success(f"✅ {message}")

    elif "CSV vacío" in message:
        st.info(f"ℹ️ {message}")

    else:
        st.success(f"✅ {message}")

    render_postgresql_footer(0, exec_time, reads, writes, show_rows=False)
def render_postgresql_footer(rows: int, exec_time: float, reads: int, writes: int, show_rows: bool = True):
    st.markdown("---")
    if show_rows:
        row_text = "fila" if rows == 1 else "filas"
        st.caption(f"**{rows} {row_text}** en {format_time(exec_time)} • Lecturas: {reads} • Escrituras: {writes}")
    else:
        st.caption(f"Tiempo: {format_time(exec_time)} • Lecturas: {reads} • Escrituras: {writes}")
def get_operation_icon(plan_name: str) -> str:
    icons = {
        "Create": "🏗️",
        "Load": "📂",
        "Select": "🔍",
        "Insert": "➕",
        "Delete": "❌",
        "Drop": "🗑️"
    }
    for key, icon in icons.items():
        if key in plan_name:
            return icon
    return "📋"
