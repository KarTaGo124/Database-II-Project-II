import streamlit as st
from pathlib import Path
import os
DATASETS_DIR = Path(__file__).parent.parent / "data" / "datasets"
def render_csv_upload():
    """Render CSV upload interface"""
    st.markdown("## 📤 Subir CSV")
    st.markdown("Sube archivos CSV para usarlos con el comando `LOAD DATA FROM FILE`")
    uploaded_file = st.file_uploader(
        "Selecciona un archivo CSV",
        type=["csv"],
        help="El archivo se guardará en data/datasets/ y estará disponible para usar con LOAD DATA",
        key="csv_uploader"
    )
    if uploaded_file is not None:
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            if st.button("💾 Guardar CSV", width="stretch", type="primary", key="save_csv_btn"):
                try:
                    DATASETS_DIR.mkdir(parents=True, exist_ok=True)
                    file_path = DATASETS_DIR / uploaded_file.name
                    _save_file(uploaded_file, file_path)
                except Exception as e:
                    st.error(f"❌ Error al guardar: {e}")
    st.markdown("---")
    st.markdown("### 📁 Archivos CSV disponibles")
    if DATASETS_DIR.exists():
        csv_files = sorted([f for f in DATASETS_DIR.iterdir() if f.suffix == '.csv'])
        if csv_files:
            for csv_file in csv_files:
                col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
                with col1:
                    st.text(f"📄 {csv_file.name}")
                with col2:
                    file_size = csv_file.stat().st_size
                    if file_size < 1024:
                        st.caption(f"{file_size} B")
                    elif file_size < 1024 * 1024:
                        st.caption(f"{file_size / 1024:.1f} KB")
                    else:
                        st.caption(f"{file_size / (1024 * 1024):.1f} MB")
                with col3:
                    if st.button("👁️", key=f"preview_{csv_file.name}", help="Ver primeras filas"):
                        st.session_state[f"show_preview_{csv_file.name}"] = not st.session_state.get(f"show_preview_{csv_file.name}", False)
                        st.rerun()
                with col4:
                    if st.button("🗑️", key=f"del_{csv_file.name}", help="Eliminar archivo"):
                        try:
                            csv_file.unlink()
                            st.success(f"Archivo eliminado: {csv_file.name}")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error al eliminar: {e}")

                if st.session_state.get(f"show_preview_{csv_file.name}", False):
                    try:
                        import pandas as pd
                        df = pd.read_csv(csv_file, nrows=5)
                        st.dataframe(df, width="stretch", hide_index=True)
                        st.caption(f"Primeras 5 filas de {csv_file.name}")
                    except Exception as e:
                        st.error(f"Error al leer el archivo: {e}")
        else:
            st.info("📭 No hay archivos CSV. Sube uno para comenzar.")
    else:
        st.info("📭 El directorio data/datasets/ no existe aún.")
def _save_file(uploaded_file, file_path: Path):
    """Helper function to save uploaded file"""
    try:
        bytes_data = uploaded_file.getvalue()
        with open(file_path, 'wb') as f:
            f.write(bytes_data)
        st.success(f"✅ Archivo guardado: **{file_path.name}**")
    except Exception as e:
        st.error(f"❌ Error al guardar: {e}")
