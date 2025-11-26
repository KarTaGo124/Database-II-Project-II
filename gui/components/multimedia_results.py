import streamlit as st
import pandas as pd
from pathlib import Path
from PIL import Image

def render_multimedia_results(data, query_image_path: str = None, images_dir: Path = None):
    if not data or len(data) == 0:
        st.info("No se encontraron resultados")
        return

    if images_dir is None:
        images_dir = Path(__file__).resolve().parents[2] / "data" / "images"

    if query_image_path:
        st.subheader("🖼️ Imagen de consulta")
        try:
            query_img = Image.open(query_image_path)
            st.image(query_img, width=200)
        except Exception as e:
            st.error(f"No se pudo cargar la imagen de consulta: {e}")

    st.subheader(f"📊 Resultados similares ({len(data)} imágenes)")

    cols_per_row = 4
    num_rows = (len(data) + cols_per_row - 1) // cols_per_row

    for row_idx in range(num_rows):
        cols = st.columns(cols_per_row)
        for col_idx in range(cols_per_row):
            idx = row_idx * cols_per_row + col_idx
            if idx >= len(data):
                break

            item = data[idx]

            if isinstance(item, tuple):
                image_id = item[0]
                similarity = item[1] if len(item) > 1 else None
            elif isinstance(item, dict):
                image_id = item.get('id', None)
                similarity = item.get('_multimedia_score', None)
            else:
                image_id = getattr(item, 'id', None)
                if image_id is None:
                    key_field = getattr(item, '_table', None)
                    if key_field and hasattr(key_field, 'key_field'):
                        image_id = getattr(item, key_field.key_field, None)
                similarity = getattr(item, '_multimedia_score', None)

            if hasattr(image_id, 'decode'):
                image_id = image_id.decode('utf-8').strip()
            elif isinstance(image_id, bytes):
                image_id = image_id.decode('utf-8').strip()
            elif image_id is not None:
                image_id = str(image_id).strip()

            if not image_id:
                continue

            image_path = images_dir / f"{image_id}.jpg"

            with cols[col_idx]:
                if image_path.exists():
                    try:
                        img = Image.open(image_path)
                        st.image(img, use_container_width=True)
                    except Exception as e:
                        st.error(f"Error: {e}")
                else:
                    st.warning(f"Imagen no encontrada")

                st.caption(f"ID: {image_id}")
                if similarity is not None:
                    st.caption(f"Similitud: {similarity:.4f}")

def is_multimedia_query(data):
    if not data or len(data) == 0:
        return False

    first_item = data[0]
    if isinstance(first_item, tuple) and len(first_item) >= 2:
        return True

    if hasattr(first_item, 'id'):
        return True

    return False
