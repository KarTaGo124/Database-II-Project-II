import streamlit as st
from pathlib import Path
from PIL import Image
import tempfile
from services.database_service import DatabaseService
from components.multimedia_results import render_multimedia_results
from utils.formatters import format_time

def render_multimedia_search(db_service: DatabaseService):
    st.header("🎯 Búsqueda Multimedia")

    st.markdown("""
    Sube una imagen o audio para encontrar archivos similares en la base de datos.
    Usa descriptores de características y búsqueda KNN para encontrar los más parecidos.
    """)

    col1, col2 = st.columns([1, 2])

    with col1:
        st.subheader("Configuración")

        table_name = st.text_input("Tabla", value="Styles", help="Nombre de la tabla con imágenes")

        k = st.slider("Número de resultados (k)", min_value=1, max_value=20, value=8,
                     help="Cantidad de imágenes similares a retornar")

        media_type = st.radio("Tipo de archivo", ["Imagen", "Audio"], horizontal=True)

        if media_type == "Imagen":
            file_types = ['jpg', 'jpeg', 'png', 'bmp', 'gif', 'tiff', 'webp']
            file_help = "Soporta SIFT, ORB, HOG"
        else:
            file_types = ['mp3', 'wav', 'ogg', 'flac', 'm4a', 'aac']
            file_help = "Soporta MFCC, CHROMA, SPECTRAL"

        uploaded_file = st.file_uploader(
            f"Selecciona un archivo {media_type.lower()}",
            type=file_types,
            help=file_help
        )

        if uploaded_file is not None:
            if media_type == "Imagen":
                image = Image.open(uploaded_file)
                st.image(image, caption="Archivo de consulta", width='stretch')
            else:
                st.audio(uploaded_file, format=f'audio/{uploaded_file.type.split("/")[-1]}')
                st.caption(f"Audio: {uploaded_file.name}")

            search_btn = st.button("🔍 Buscar similares", type="primary")

            if search_btn:
                suffix = f'.{uploaded_file.name.split(".")[-1]}'
                with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_file:
                    if media_type == "Imagen":
                        image = Image.open(uploaded_file)
                        image.save(tmp_file.name)
                    else:
                        tmp_file.write(uploaded_file.getvalue())
                    tmp_path = tmp_file.name

                with st.spinner(f"Buscando archivos similares..."):
                    sql = f'SELECT * FROM {table_name} WHERE id <-> "{tmp_path}" LIMIT {k};'
                    results = db_service.execute_sql(sql)

                    if results and len(results) > 0:
                        result_item = results[0]
                        if "error" not in result_item:
                            result = result_item["result"]
                            data = getattr(result, "data", None)
                            exec_time = getattr(result, "execution_time_ms", 0.0) or 0.0
                            reads = getattr(result, "disk_reads", 0) or 0
                            writes = getattr(result, "disk_writes", 0) or 0

                            st.session_state["multimedia_results"] = {
                                "data": data,
                                "query_file": tmp_path,
                                "media_type": media_type.lower(),
                                "exec_time": exec_time,
                                "reads": reads,
                                "writes": writes
                            }
                        else:
                            st.error(result_item["error"])
                    else:
                        st.warning("No se obtuvieron resultados")

                st.rerun()

    with col2:
        if "multimedia_results" in st.session_state:
            results = st.session_state["multimedia_results"]
            data = results["data"]
            query_file = results.get("query_file")
            media_type = results.get("media_type", "imagen")
            exec_time = results["exec_time"]
            reads = results["reads"]
            writes = results["writes"]

            if data and len(data) > 0:
                render_multimedia_results(
                    data,
                    query_file_path=query_file,
                    media_type=media_type
                )

                st.markdown("---")
                st.caption(f"**{len(data)} resultados** en {format_time(exec_time)} • Lecturas: {reads} • Escrituras: {writes}")
            else:
                st.info("No se encontraron archivos similares")
        else:
            st.info("👈 Sube un archivo multimedia para comenzar la búsqueda")
