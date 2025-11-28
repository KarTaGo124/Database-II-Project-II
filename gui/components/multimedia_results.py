import streamlit as st
import pandas as pd
from pathlib import Path
from PIL import Image

def render_multimedia_results(data, query_file_path: str = None, media_dir: Path = None, media_type: str = "image"):
    if not data or len(data) == 0:
        st.info("No se encontraron resultados")
        return

    # Auto-detect media type from query file if provided
    if query_file_path and media_type == "image":
        ext = Path(query_file_path).suffix.lower()
        if ext in ['.mp3', '.wav', '.ogg', '.flac']:
            media_type = "audio"

    # Set default directory based on media type
    if media_dir is None:
        if media_type == "audio":
            media_dir = Path(__file__).resolve().parents[2] / "data" / "audio"
        else:
            media_dir = Path(__file__).resolve().parents[2] / "data" / "images"

    if query_file_path and media_type == "image":
        st.subheader("🖼️ Imagen de consulta")
        try:
            query_img = Image.open(query_file_path)
            st.image(query_img, width=200)
        except Exception as e:
            st.error(f"No se pudo cargar la imagen de consulta: {e}")

    # Show results
    result_label = "audios" if media_type == "audio" else "imágenes"
    st.subheader(f"📊 Resultados similares ({len(data)} {result_label})")

    cols_per_row = 4 if media_type == "image" else 2
    num_rows = (len(data) + cols_per_row - 1) // cols_per_row

    for row_idx in range(num_rows):
        cols = st.columns(cols_per_row)
        for col_idx in range(cols_per_row):
            idx = row_idx * cols_per_row + col_idx
            if idx >= len(data):
                break

            item = data[idx]

            # Extract file identifier and similarity
            if isinstance(item, tuple):
                file_id = item[0]
                similarity = item[1] if len(item) > 1 else None
            elif isinstance(item, dict):
                file_id = item.get('filename', item.get('id', None))
                similarity = item.get('_multimedia_score', None)
            else:
                file_id = getattr(item, 'filename', getattr(item, 'id', None))
                if file_id is None:
                    key_field = getattr(item, '_table', None)
                    if key_field and hasattr(key_field, 'key_field'):
                        file_id = getattr(item, key_field.key_field, None)
                similarity = getattr(item, '_multimedia_score', None)

            # Decode if bytes
            if hasattr(file_id, 'decode'):
                file_id = file_id.decode('utf-8').strip()
            elif isinstance(file_id, bytes):
                file_id = file_id.decode('utf-8').strip()
            elif file_id is not None:
                file_id = str(file_id).strip()

            if not file_id:
                continue

            # Build file path
            if media_type == "audio":
                # For audio, file_id might already be the filename (e.g., "000002.mp3")
                if file_id.endswith('.mp3') or file_id.endswith('.wav'):
                    file_path = media_dir / file_id
                else:
                    file_path = media_dir / f"{file_id}.mp3"
            else:
                # For images
                if file_id.endswith('.jpg') or file_id.endswith('.png'):
                    file_path = media_dir / file_id
                else:
                    file_path = media_dir / f"{file_id}.jpg"

            # Render result
            with cols[col_idx]:
                if media_type == "audio":
                    if file_path.exists():
                        try:
                            st.audio(str(file_path))
                        except Exception as e:
                            st.error(f"Error: {e}")
                    else:
                        st.warning(f"Audio no encontrado")

                    st.caption(f"🎵 {file_path.stem}")
                else:
                    if file_path.exists():
                        try:
                            img = Image.open(file_path)
                            st.image(img, width='stretch')
                        except Exception as e:
                            st.error(f"Error: {e}")
                    else:
                        st.warning(f"Imagen no encontrada")

                    st.caption(f"ID: {file_id}")

                if similarity is not None:
                    st.caption(f"Similitud: {similarity:.4f}")

def is_multimedia_query(data):
    if not data or len(data) == 0:
        return False

    first_item = data[0]
    if isinstance(first_item, tuple) and len(first_item) >= 2:
        return True

    if hasattr(first_item, 'id') or hasattr(first_item, 'filename'):
        return True

    return False
