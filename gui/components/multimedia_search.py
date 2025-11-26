import streamlit as st
from pathlib import Path
from PIL import Image
import tempfile
from services.database_service import DatabaseService
from components.multimedia_results import render_multimedia_results
from utils.formatters import format_time

def render_multimedia_search(db_service: DatabaseService):
    st.header("🖼️ Búsqueda por Imagen")

    st.markdown("""
    Sube una imagen para encontrar imágenes similares en la base de datos.
    El sistema usa descriptores SIFT y búsqueda KNN para encontrar las imágenes más parecidas.
    """)

    col1, col2 = st.columns([1, 2])

    with col1:
        st.subheader("Configuración")

        table_name = st.text_input("Tabla", value="Styles", help="Nombre de la tabla con imágenes")

        k = st.slider("Número de resultados (k)", min_value=1, max_value=20, value=8,
                     help="Cantidad de imágenes similares a retornar")

        uploaded_file = st.file_uploader(
            "Selecciona una imagen",
            type=['jpg', 'jpeg', 'png'],
            help="Formatos soportados: JPG, JPEG, PNG"
        )

        if uploaded_file is not None:
            image = Image.open(uploaded_file)
            st.image(image, caption="Imagen de consulta", use_container_width=True)

            search_btn = st.button("🔍 Buscar similares", type="primary", use_container_width=True)

            if search_btn:
                with tempfile.NamedTemporaryFile(delete=False, suffix='.jpg') as tmp_file:
                    image.save(tmp_file.name)
                    tmp_path = tmp_file.name

                with st.spinner("Buscando imágenes similares..."):
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
                                "query_image": tmp_path,
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
            exec_time = results["exec_time"]
            reads = results["reads"]
            writes = results["writes"]

            if data and len(data) > 0:
                images_dir = Path(__file__).resolve().parents[2] / "data" / "images"
                render_multimedia_results(data, images_dir=images_dir)

                st.markdown("---")
                st.caption(f"**{len(data)} resultados** en {format_time(exec_time)} • Lecturas: {reads} • Escrituras: {writes}")
            else:
                st.info("No se encontraron imágenes similares")
        else:
            st.info("👈 Sube una imagen para comenzar la búsqueda")
