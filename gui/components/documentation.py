import streamlit as st
def render_documentation():
    """Renderiza documentación completa de tipos de consultas soportadas."""
    st.header("📚 Documentación de Consultas SQL")
    tabs = st.tabs([
        "📋 DDL",
        "🔍 Consultas",
        "➕ Inserción",
        "❌ Eliminación",
        "🗂️ Índices",
        "🌍 Espaciales",
        "📝 Fulltext",
        "🖼️ Multimedia"
    ])
    with tabs[0]:
        st.markdown("### Definición de Datos (DDL)")
        with st.expander("🏗️ CREATE TABLE - Crear Tabla", expanded=True):
            st.markdown("""
            Crea una nueva tabla con campos y opcionalmente índices.
            **Sintaxis:**
            ```sql
            CREATE TABLE nombre_tabla (
                campo1 tipo [KEY] [INDEX tipo_indice],
                campo2 tipo [INDEX tipo_indice],
                ...
            );
            ```
            **Tipos de datos soportados:**
            - `INT` - Entero de 4 bytes
            - `FLOAT` - Punto flotante de 4 bytes
            - `VARCHAR[n]` - Cadena de texto de longitud n (ejemplo: VARCHAR[100])
            - `DATE` - Fecha en formato YYYY-MM-DD
            - `ARRAY[FLOAT]` - Array 2D (para datos espaciales, ejemplo: latitud, longitud)
            - `ARRAY[FLOAT, n]` - Array n-dimensional (ejemplo: ARRAY[FLOAT, 3] para 3D)
            **Tipos de índices:**
            - `SEQUENTIAL` - Solo primario
            - `ISAM` - Solo primario (predeterminado)
            - `BTREE` - Primario y secundario (recomendado)
            - `HASH` - Solo secundario (búsquedas exactas muy rápidas)
            - `RTREE` - Solo secundario (para datos espaciales ARRAY)
            - `INVERTED_TEXT` - Solo secundario (búsqueda fulltext en texto)
            """)
            st.code("""CREATE TABLE Restaurantes (
    id INT KEY INDEX BTREE,
    nombre VARCHAR[100] INDEX BTREE,
    ubicacion ARRAY[FLOAT] INDEX RTREE,
    rating FLOAT INDEX HASH,
    precio_promedio FLOAT,
    fecha_apertura VARCHAR[20],
    ciudad VARCHAR[50]
);""", language="sql")
        with st.expander("📂 LOAD DATA - Cargar desde CSV"):
            st.markdown("""
            Carga datos desde un archivo CSV a una tabla existente.
            **Sintaxis básica:**
            ```sql
            LOAD DATA FROM FILE "ruta/archivo.csv" INTO nombre_tabla;
            ```
            **Con mapeo de arrays (para campos espaciales):**
            ```sql
            LOAD DATA FROM FILE "ruta/archivo.csv" INTO nombre_tabla
            WITH MAPPING (
                campo_array = ARRAY(columna_csv1, columna_csv2)
            );
            ```
            **Notas:**
            - La ruta debe ser relativa a la raíz del proyecto
            - Los nombres de columnas del CSV deben coincidir con los campos de la tabla
            - Para campos ARRAY, usa WITH MAPPING para especificar qué columnas del CSV corresponden a cada dimensión
            """)
            st.code("""LOAD DATA FROM FILE "data/datasets/restaurantes.csv" INTO Restaurantes
WITH MAPPING (
    ubicacion = ARRAY(latitud, longitud)
);""", language="sql")
        with st.expander("🗑️ DROP TABLE - Eliminar Tabla"):
            st.markdown("""
            Elimina una tabla y todos sus índices asociados.
            **Sintaxis:**
            ```sql
            DROP TABLE nombre_tabla;
            ```
            ⚠️ **Advertencia:** Esta operación es irreversible y elimina todos los datos.
            """)
            st.code("""DROP TABLE Restaurantes;""", language="sql")
    with tabs[1]:
        st.markdown("### Consultas de Datos (SELECT)")
        with st.expander("🔍 SELECT básico", expanded=True):
            st.markdown("""
            Recupera todos los registros de una tabla.
            **Sintaxis:**
            ```sql
            SELECT * FROM nombre_tabla;
            SELECT campo1, campo2 FROM nombre_tabla;
            ```
            """)
            st.code("""SELECT * FROM Restaurantes;
SELECT nombre, rating FROM Restaurantes;""", language="sql")
        with st.expander("🎯 SELECT con filtro de igualdad (WHERE =)"):
            st.markdown("""
            Busca registros que coincidan exactamente con un valor.
            **Sintaxis:**
            ```sql
            SELECT * FROM tabla WHERE campo = valor;
            ```
            **Optimización:**
            - Si hay índice en el campo, la búsqueda es O(log n)
            - Sin índice, realiza escaneo completo O(n)
            """)
            st.code("""SELECT * FROM Restaurantes WHERE id = 42;
SELECT * FROM Restaurantes WHERE nombre = "La Buena Mesa";
SELECT * FROM Restaurantes WHERE rating = 4.5;""", language="sql")
        with st.expander("📊 SELECT con rango (BETWEEN)"):
            st.markdown("""
            Recupera registros dentro de un rango de valores.
            **Sintaxis:**
            ```sql
            SELECT * FROM tabla WHERE campo BETWEEN valor_min AND valor_max;
            ```
            **Tipos soportados:**
            - Numéricos (INT, FLOAT): rango inclusivo
            - VARCHAR: orden lexicográfico
            - DATE: orden cronológico
            **Nota:** BETWEEN es inclusivo en ambos extremos: [min, max]
            """)
            st.code("""SELECT * FROM Restaurantes
WHERE rating BETWEEN 4.0 AND 5.0;
SELECT * FROM Restaurantes
WHERE id BETWEEN 100 AND 200;
SELECT * FROM Restaurantes
WHERE fecha_apertura BETWEEN "2023-01-01" AND "2023-12-31";""", language="sql")
    with tabs[2]:
        st.markdown("### Inserción de Datos (INSERT)")
        with st.expander("➕ INSERT básico", expanded=True):
            st.markdown("""
            Inserta un nuevo registro en la tabla.
            **Sintaxis con todos los campos:**
            ```sql
            INSERT INTO tabla VALUES (valor1, valor2, ...);
            ```
            **Sintaxis con campos específicos:**
            ```sql
            INSERT INTO tabla (campo1, campo2) VALUES (valor1, valor2);
            ```
            **Notas:**
            - Los valores deben coincidir con el tipo de dato del campo
            - Para arrays espaciales, usa la sintaxis (x, y) o (x, y, z, ...)
            - Si el registro ya existe (clave duplicada), la inserción falla
            """)
            st.code("""INSERT INTO Restaurantes VALUES (
    201,
    "Parrilla Nueva",
    (-34.6050, -58.3800),
    4.5,
    85.0,
    "2024-01-15",
    "Buenos Aires"
);
INSERT INTO Restaurantes (id, nombre, ubicacion, rating, precio_promedio)
VALUES (202, "Café Porteño", (-34.6020, -58.3750), 4.2, 45.0);""", language="sql")
    with tabs[3]:
        st.markdown("### Eliminación de Datos (DELETE)")
        with st.expander("❌ DELETE con condición", expanded=True):
            st.markdown("""
            Elimina registros que cumplan una condición.
            **Sintaxis:**
            ```sql
            DELETE FROM tabla WHERE condicion;
            ```
            **Condiciones soportadas:**
            - Igualdad: `campo = valor`
            - Rango: `campo BETWEEN min AND max`
            **Proceso:**
            1. Busca registros que cumplan la condición
            2. Elimina de todos los índices secundarios
            3. Elimina del índice primario
            ⚠️ **Advertencia:** Sin WHERE, eliminaría todos los registros (actualmente no soportado por seguridad)
            """)
            st.code("""DELETE FROM Restaurantes WHERE id = 1001;
DELETE FROM Restaurantes WHERE nombre = "Café Viejo";
DELETE FROM Restaurantes WHERE rating BETWEEN 0.0 AND 2.0;
DELETE FROM Restaurantes
WHERE fecha_apertura BETWEEN "2020-01-01" AND "2020-12-31";""", language="sql")
    with tabs[4]:
        st.markdown("### Gestión de Índices")
        with st.expander("🔨 CREATE INDEX - Crear Índice Secundario", expanded=True):
            st.markdown("""
            Crea un índice secundario en un campo existente para acelerar búsquedas.
            **Sintaxis:**
            ```sql
            CREATE INDEX ON tabla (campo) USING tipo_indice;
            ```
            **Tipos disponibles para índices secundarios:**
            - `BTREE` - Árbol B+, soporta búsquedas exactas y por rango
            - `HASH` - Hash extensible, solo búsquedas exactas (muy rápido)
            - `RTREE` - Árbol R, para datos espaciales
            - `INVERTED_TEXT` - Índice invertido para búsqueda fulltext en campos de texto
                        
            **Cuándo usar cada tipo:**
            - **BTREE**: Cuando necesitas rangos o datos ordenados
            - **HASH**: Cuando solo haces búsquedas exactas y quieres máxima velocidad
            - **RTREE**: Para campos ARRAY con coordenadas espaciales
            - **INVERTED_TEXT**: Para campos de texto largos donde se requieren búsquedas por palabras clave
                        
            **Proceso:**
            - El sistema escanea todos los registros existentes
            - Construye el índice con todas las entradas
            - Las operaciones futuras mantienen el índice actualizado (excepto INVERTED_TEXT que es estático)
            """)
            st.code("""CREATE INDEX ON Restaurantes (nombre) USING BTREE;
CREATE INDEX ON Restaurantes (rating) USING HASH;
CREATE INDEX ON Restaurantes (ubicacion) USING RTREE;
CREATE INDEX ON Noticias (contenido) USING INVERTED_TEXT;""", language="sql")
        with st.expander("🗑️ DROP INDEX - Eliminar Índice"):
            st.markdown("""
            Elimina un índice secundario de un campo.
            **Sintaxis:**
            ```sql
            DROP INDEX nombre_campo ON nombre_tabla;
            ```
            **Notas:**
            - Solo puede eliminar índices secundarios (no el primario)
            - Libera espacio en disco
            - Las consultas seguirán funcionando pero más lentas
            - Requiere especificar tanto el campo como la tabla
            """)
            st.code("""DROP INDEX nombre ON Restaurantes;
DROP INDEX ubicacion ON Restaurantes;
DROP INDEX rating ON Restaurantes;
DROP INDEX descripcion ON Restaurantes;
                    """, language="sql")
    with tabs[5]:
        st.markdown("### Consultas Espaciales (R-Tree)")
        st.markdown("""
        Las consultas espaciales requieren:
        1. Campo tipo `ARRAY[FLOAT]` o `ARRAY[FLOAT, n]`
        2. Índice `RTREE` en ese campo
        **Casos de uso comunes:**
        - Encontrar puntos de interés cercanos
        - Búsqueda de vecinos más próximos
        - Análisis geoespacial
        """)
        with st.expander("🎯 Búsqueda por Radio (IN RADIUS)", expanded=True):
            st.markdown("""
            Encuentra todos los puntos dentro de un radio desde un punto central.
            **Sintaxis:**
            ```sql
            SELECT * FROM tabla
            WHERE campo_espacial IN ((x, y), radio);
            ```
            **IMPORTANTE:**
            - Usa **doble paréntesis**: `IN ((x, y), radio)`
            - El radio está en las **mismas unidades que las coordenadas** (grados para lat/lon)
            **Parámetros:**
            - `(x, y)`: Coordenadas del punto central (ejemplo: latitud, longitud)
            - `radio`: Radio de búsqueda en grados decimales
              - Para GPS: ~0.01 grados ≈ 1.1 km
              - Para GPS: ~0.05 grados ≈ 5.5 km
            **Cálculo:**
            - Crea un bounding box: [x-radio, y-radio] a [x+radio, y+radio]
            - Retorna todos los puntos dentro de ese rectángulo
            - Usa el índice R-Tree para búsqueda espacial eficiente
            **Complejidad:**
            - Con R-Tree: O(log n + k) donde k = resultados
            - Sin índice: O(n) escaneo completo
            """)
            st.code("""SELECT * FROM Restaurantes
WHERE ubicacion IN ((-34.6037, -58.3816), 0.01);
SELECT nombre, ubicacion, rating FROM Restaurantes
WHERE ubicacion IN ((-34.6037, -58.3816), 0.05);
SELECT * FROM Restaurantes
WHERE ubicacion IN ((-34.6037, -58.3816), 0.005);""", language="sql")
        with st.expander("🏆 K Vecinos Más Cercanos (NEAREST K)", expanded=True):
            st.markdown("""
            Encuentra los K puntos más cercanos a un punto de referencia.
            **Sintaxis:**
            ```sql
            SELECT * FROM tabla
            WHERE campo_espacial NEAREST ((x, y), k);
            ```
            **IMPORTANTE:**
            - Usa **doble paréntesis**: `NEAREST ((x, y), k)`
            **Parámetros:**
            - `(x, y)`: Coordenadas del punto de referencia
            - `k`: Número de vecinos más cercanos a retornar
            **Características:**
            - Retorna exactamente K resultados (o menos si no hay suficientes)
            - Ordenados por distancia (más cercano primero)
            - Ideal para recomendaciones basadas en proximidad
            **Casos de uso:**
            - "Los 5 restaurantes más cercanos a mi ubicación"
            - "Las 10 tiendas más próximas"
            - Sistemas de recomendación geográfica
            """)
            st.code("""SELECT nombre, ubicacion, rating FROM Restaurantes
WHERE ubicacion NEAREST ((-34.6037, -58.3816), 5);
SELECT * FROM Restaurantes
WHERE ubicacion NEAREST ((-34.6037, -58.3816), 3);
SELECT id, nombre, ubicacion FROM Restaurantes
WHERE ubicacion NEAREST ((-34.6037, -58.3816), 10);""", language="sql")
        st.info("""
        💡 **Consejos para consultas espaciales con R-Tree:**
        - **Sintaxis especial:** Usa doble paréntesis: `IN ((x, y), radio)` y `NEAREST ((x, y), k)`
        - **Unidades:** Para GPS (lat/lon), el radio está en grados decimales:
          - 0.001° ≈ 111 metros
          - 0.01° ≈ 1.1 kilómetros
          - 0.05° ≈ 5.5 kilómetros
          - 0.1° ≈ 11 kilómetros
        - **Índice requerido:** Crea un índice RTREE en campos ARRAY[FLOAT] para mejor rendimiento
        - **Formato de coordenadas:** (latitud, longitud) - ejemplo: (-34.6037, -58.3816) para Buenos Aires
        """)

    with tabs[6]:
        st.markdown("### Búsqueda Fulltext (Índice Invertido)")
        st.markdown("""
        Las consultas fulltext requieren:
        1. Campo tipo `VARCHAR[n]` o `CHAR`
        2. Índice `INVERTED_TEXT` en ese campo
        **Características:**
        - Búsqueda por similitud de texto usando TF-IDF
        - Preprocesamiento en español (stopwords, stemming)
        - Ranking por score de relevancia (cosine similarity)
        - Índice estático (se crea una vez, no se actualiza con INSERT/DELETE)
        """)
        with st.expander("🔍 Búsqueda Fulltext (WHERE @@)", expanded=True):
            st.markdown("""
            Encuentra documentos relevantes para una consulta de texto.
            **Sintaxis:**
            ```sql
            SELECT * FROM tabla
            WHERE campo_texto @@ "palabras clave de búsqueda";
            ```
            **Parámetros:**
            - `campo_texto`: Campo VARCHAR/CHAR con índice INVERTED_TEXT
            - `"consulta"`: Texto de búsqueda entre comillas dobles
            **Características:**
            - Retorna documentos ordenados por relevancia (score de 0.0 a 1.0)
            - Sin threshold mínimo (puede retornar matches con score bajo)
            - Usa preprocesamiento: lowercase, remove punctuation, stopwords, stemming
            - Sin LIMIT: retorna todos los resultados
            - Con LIMIT N: retorna los top N resultados más relevantes
            **Algoritmo:**
            - Preprocesa la consulta (tokeniza, remueve stopwords, stemming)
            - Calcula TF-IDF para cada término
            - Retorna documentos ordenados por cosine similarity
            """)
            st.code("""SELECT * FROM Noticias
WHERE contenido @@ "economía inflación precios";
SELECT url, contenido FROM Noticias
WHERE contenido @@ "tecnología inteligencia artificial" LIMIT 5;
SELECT * FROM Noticias
WHERE contenido @@ "política elecciones gobierno" LIMIT 20;""", language="sql")
        with st.expander("📊 Flujo Completo - Ejemplo con Noticias"):
            st.markdown("""
            Ejemplo completo de creación de tabla, carga de datos y búsquedas fulltext.
            **1. Crear tabla con campo de texto:**
            ```sql
            CREATE TABLE Noticias (
                id INT KEY INDEX ISAM,
                url VARCHAR[200],
                contenido VARCHAR[5000],
                categoria VARCHAR[50]
            );
            ```
            **2. Cargar datos desde CSV:**
            ```sql
            LOAD DATA FROM FILE "data/datasets/news_es.csv" INTO Noticias;
            ```
            **3. Crear índice invertido:**
            ```sql
            CREATE INDEX ON Noticias (contenido) USING INVERTED_TEXT;
            CREATE INDEX ON Noticias (contenido) USING INVERTED_TEXT LANGUAGE "spanish";
            CREATE INDEX ON News (content) USING INVERTED_TEXT LANGUAGE "english";
            ```
            **4. Realizar búsquedas fulltext:**
            ```sql
            SELECT categoria, contenido FROM Noticias
            WHERE contenido @@ "economía inflación" LIMIT 5;
            ```
            **Nota:** El campo `_text_score` se agrega automáticamente a los resultados con el score de relevancia.
            """)
            st.code("""
CREATE TABLE Noticias (
    id INT KEY INDEX ISAM,
    url VARCHAR[200],
    contenido VARCHAR[5000],
    categoria VARCHAR[50]
); 
LOAD DATA FROM FILE "data/datasets/news_es-2.csv" INTO Noticias;
CREATE INDEX ON Noticias (contenido) USING INVERTED_TEXT;
                    
SELECT * FROM Noticias WHERE contenido @@ "economía" LIMIT 3;
SELECT categoria, contenido FROM Noticias
WHERE contenido @@ "tecnología inteligencia artificial" LIMIT 5;""", language="sql")
        st.info("""
        💡 **Consejos para búsquedas fulltext:**
        - **Operador especial:** Usa `@@` para búsquedas fulltext: `WHERE campo @@ "consulta"`
        - **Quotes dobles:** Usa comillas dobles para la consulta de texto
        - **LIMIT:** Opcional. Sin LIMIT retorna todos los resultados, con LIMIT N retorna los top N
        - **Score:** Los resultados incluyen `_text_score` (0.0 a 1.0) indicando relevancia
        - **Sin threshold:** Retorna todos los matches, incluso con score bajo
        - **Idioma:** Soporta múltiples idiomas (spanish, english, etc.)
          - Default: spanish
          - Especificar con: `LANGUAGE "idioma"` al crear el índice
        - **Índice estático:** Se crea una vez con los datos existentes, no se actualiza automáticamente
        """)

    with tabs[7]:
        st.markdown("### Búsqueda por Similitud Multimedia (KNN)")
        st.markdown("""
        Las consultas multimedia requieren:
        1. Archivos multimedia (imágenes o audio) almacenados en una carpeta conocida
        2. Índice `MULTIMEDIA_SEQ` o `MULTIMEDIA_INV` en el campo ID
        3. Descriptores extraídos automáticamente según el tipo de archivo

        **Características:**
        - Búsqueda KNN (K-Nearest Neighbors) sobre descriptores visuales o acústicos
        - Bag of Visual/Acoustic Words con TF-IDF
        - Dos métodos: Sequential (scan completo) e Inverted (indexado)
        - Retorna top-K archivos más similares
        - Auto-detección del tipo de archivo (imagen o audio)
        """)

        with st.expander("🎨 CREATE INDEX - Índice Multimedia", expanded=True):
            st.markdown("""
            Crea un índice para búsqueda por similitud de imágenes o audio.

            **Sintaxis:**
            ```sql
            CREATE INDEX ON tabla (campo_clave) USING tipo_multimedia
            FEATURE "tipo_descriptor"
            DIRECTORY "ruta/archivos/"
            PATTERN "{id}.extension";
            ```

            **IMPORTANTE:**
            - `campo_clave` debe ser el campo KEY de la tabla (campo primario)
            - El índice multimedia se crea sobre el campo clave, no sobre un campo de archivo
            - El sistema auto-detecta si es imagen o audio según la extensión

            **Tipos de índice multimedia:**
            - `MULTIMEDIA_SEQ` - Sequential scan con TF-IDF (más lento pero exacto)
            - `MULTIMEDIA_INV` - Inverted index con SPIMI (más rápido)

            **Descriptores soportados:**

            *Para imágenes:*
            - `SIFT` - Scale-Invariant Feature Transform (recomendado para imágenes)
            - `ORB` - Oriented FAST and Rotated BRIEF (rápido)
            - `HOG` - Histogram of Oriented Gradients (para detección)

            *Para audio:*
            - `MFCC` - Mel-Frequency Cepstral Coefficients (13 coeficientes)
            - `CHROMA` - Chroma features para análisis armónico (12 features)
            - `SPECTRAL` - Spectral features (centroid, rolloff, zero-crossing rate)

            **Parámetros:**
            - `DIRECTORY` - Carpeta donde están los archivos multimedia (relativa al proyecto)
            - `PATTERN` - Patrón de nombre de archivo con placeholder {id}

            **Proceso:**
            1. Extrae descriptores del tipo especificado de cada archivo
            2. Construye codebook con K-means (clustering)
            3. Auto-detecta n_clusters según tamaño del dataset (300-1000)
            4. Genera histogramas TF-IDF para cada archivo
            5. Construye índice (sequential o inverted)
            """)
            st.code("""
CREATE TABLE Styles (
    id INT KEY INDEX SEQUENTIAL,
    ...
);

CREATE INDEX ON Styles USING MULTIMEDIA_SEQ
FEATURE "SIFT"
DIRECTORY "data/images/"
PATTERN "{id}.jpg";

CREATE TABLE Songs (
    id INT KEY INDEX SEQUENTIAL,
    ...
);

CREATE INDEX ON Songs USING MULTIMEDIA_INV
FEATURE "MFCC"
DIRECTORY "data/audio/"
PATTERN "{id}.mp3";""", language="sql")

        with st.expander("🔍 Búsqueda KNN (WHERE <->)", expanded=True):
            st.markdown("""
            Encuentra los K archivos multimedia más similares a un archivo de consulta.

            **Sintaxis:**
            ```sql
            SELECT * FROM tabla
            WHERE campo_id <-> "ruta/archivo.ext"
            LIMIT k;
            ```

            **Operador especial:**
            - `<->` - Operador de similitud multimedia (Diamond operator)

            **Parámetros:**
            - `ruta/archivo.ext` - Archivo de consulta (imagen o audio, ruta completa o relativa)
            - `LIMIT k` - Número de resultados similares a retornar

            **Características:**
            - Retorna top-K archivos más similares
            - Ordenadas por score de similitud (cosine similarity)
            - Auto-detecta tipo de archivo y usa el descriptor correspondiente
            - Métrica: cosine similarity entre histogramas TF-IDF

            **Casos de uso:**

            *Para imágenes:*
            - Búsqueda de productos similares por imagen
            - Recomendación visual
            - Detección de duplicados
            - Búsqueda inversa de imágenes

            *Para audio:*
            - Búsqueda de canciones similares
            - Recomendación musical por similitud acústica
            - Detección de covers o versiones
            - Identificación de género musical
            """)
            st.code("""SELECT * FROM Styles
WHERE id <-> "data/images/15970.jpg" LIMIT 8;

SELECT id, productDisplayName FROM Styles
WHERE id <-> "query_image.jpg" LIMIT 10;

SELECT * FROM Products
WHERE product_id <-> "uploaded_image.png" LIMIT 5;

SELECT * FROM Songs
WHERE id <-> "data/audio/query_song.mp3" LIMIT 10;

SELECT title, artist FROM Music
WHERE track_id <-> "uploaded_audio.wav" LIMIT 5;""", language="sql")

        with st.expander("📊 Ejemplo Imágenes - Fashion Dataset"):
            st.markdown("""
            Ejemplo completo de creación de tabla, carga de datos y búsquedas con imágenes.

            **1. Crear tabla con datos de productos:**
            ```sql
            CREATE TABLE Styles (
                id INT KEY INDEX SEQUENTIAL,
                gender VARCHAR[20],
                masterCategory VARCHAR[50],
                articleType VARCHAR[50],
                productDisplayName VARCHAR[200]
            );
            ```

            **2. Cargar datos desde CSV:**
            ```sql
            LOAD DATA FROM FILE "data/datasets/styles_1000.csv" INTO Styles;
            ```

            **3. Crear índice multimedia con descriptor de imágenes:**
            ```sql
            CREATE INDEX ON Styles USING MULTIMEDIA_SEQ
            FEATURE "SIFT"
            DIRECTORY "data/images/"
            PATTERN "{id}.jpg";
            ```

            **4. Realizar búsquedas KNN:**
            ```sql
            SELECT * FROM Styles
            WHERE id <-> "data/images/15970.jpg" LIMIT 8;
            ```

            **Nota:** El sistema extrae automáticamente descriptores SIFT de las imágenes y construye el índice con auto-detección de n_clusters.
            """)
            st.code("""CREATE TABLE Styles (
    id INT KEY INDEX SEQUENTIAL,
    gender VARCHAR[20],
    masterCategory VARCHAR[50],
    subCategory VARCHAR[50],
    articleType VARCHAR[50],
    baseColour VARCHAR[50],
    season VARCHAR[20],
    year INT,
    usage VARCHAR[20],
    productDisplayName VARCHAR[200]
);

LOAD DATA FROM FILE "data/datasets/styles_1000.csv" INTO Styles;

CREATE INDEX ON Styles USING MULTIMEDIA_SEQ
FEATURE "SIFT"
DIRECTORY "data/images/"
PATTERN "{id}.jpg";

SELECT * FROM Styles WHERE id <-> "data/images/15970.jpg" LIMIT 8;""", language="sql")

        with st.expander("🎵 Ejemplo Audio - Music Dataset"):
            st.markdown("""
            Ejemplo completo de creación de tabla, carga de datos y búsquedas con audio.

            **1. Crear tabla con datos de canciones:**
            ```sql
            CREATE TABLE Songs (
                id INT KEY INDEX SEQUENTIAL,
                title VARCHAR[200],
                artist VARCHAR[100],
                genre VARCHAR[50],
                duration FLOAT
            );
            ```

            **2. Cargar datos desde CSV:**
            ```sql
            LOAD DATA FROM FILE "data/datasets/songs.csv" INTO Songs;
            ```

            **3. Crear índice multimedia con descriptor de audio:**
            ```sql
            CREATE INDEX ON Songs USING MULTIMEDIA_INV
            FEATURE "MFCC"
            DIRECTORY "data/audio/"
            PATTERN "{id}.mp3";
            ```

            **4. Realizar búsquedas KNN:**
            ```sql
            SELECT * FROM Songs
            WHERE id <-> "data/audio/query_song.mp3" LIMIT 10;
            ```

            **Nota:** El sistema extrae automáticamente descriptores MFCC del audio y construye el índice invertido.
            """)
            st.code("""CREATE TABLE Songs (
    id INT KEY INDEX SEQUENTIAL,
    title VARCHAR[200],
    artist VARCHAR[100],
    genre VARCHAR[50],
    duration FLOAT
);

LOAD DATA FROM FILE "data/datasets/songs.csv" INTO Songs;

CREATE INDEX ON Songs USING MULTIMEDIA_INV
FEATURE "MFCC"
DIRECTORY "data/audio/"
PATTERN "{id}.mp3";

SELECT * FROM Songs WHERE id <-> "data/audio/query_song.mp3" LIMIT 10;""", language="sql")

        st.info("""
        💡 **Consejos para búsquedas multimedia:**
        - **Operador especial:** Usa `<->` para búsquedas KNN: `WHERE campo <-> "archivo.ext"`
        - **Índice requerido:** Crea índice MULTIMEDIA_SEQ o MULTIMEDIA_INV antes de buscar
        - **LIMIT obligatorio:** Siempre especifica LIMIT k para definir cuántos resultados
        - **Estructura de carpetas:**
          - Imágenes: `data/images/` con archivos .jpg, .png, etc.
          - Audio: `data/audio/` con archivos .mp3, .wav, etc.
          - Pattern típico: `{id}.extension` donde {id} es la clave primaria
        - **Descriptores para imágenes:**
          - SIFT: Más robusto y preciso (recomendado)
          - ORB: Más rápido pero menos preciso
          - HOG: Para detección de objetos
        - **Descriptores para audio:**
          - MFCC: Características generales (recomendado)
          - CHROMA: Análisis armónico y tonal
          - SPECTRAL: Características espectrales
        - **Performance:**
          - MULTIMEDIA_SEQ: O(n) - escaneo completo, más lento pero exacto
          - MULTIMEDIA_INV: O(log n) - índice invertido, más rápido
        - **K recomendado:** Entre 5 y 20 para mejores resultados
        - **Auto-scaling:** n_clusters se ajusta automáticamente (300-1000) según tamaño del dataset
        """)

