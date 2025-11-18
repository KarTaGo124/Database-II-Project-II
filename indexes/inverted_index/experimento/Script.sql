--  CREACIÓN DE TABLAS PARA LOS DIFERENTES TAMAÑOS DEL CORPUS

CREATE TABLE amazon1000  (doc_id INTEGER PRIMARY KEY, text TEXT);
CREATE TABLE amazon2000  (doc_id INTEGER PRIMARY KEY, text TEXT);
CREATE TABLE amazon4000  (doc_id INTEGER PRIMARY KEY, text TEXT);
CREATE TABLE amazon8000  (doc_id INTEGER PRIMARY KEY, text TEXT);
CREATE TABLE amazon16000 (doc_id INTEGER PRIMARY KEY, text TEXT);
CREATE TABLE amazon32000 (doc_id INTEGER PRIMARY KEY, text TEXT);
CREATE TABLE amazon64000 (doc_id INTEGER PRIMARY KEY, text TEXT);

--  IMPORTACIÓN DE CSV :(Se realizó mediante pgAdmin Import/Export)


--  CREACIÓN DE COLUMNA TSVECTOR PARA BÚSQUEDA TEXTUAL

ALTER TABLE amazon1000  ADD COLUMN tsv tsvector;
ALTER TABLE amazon2000  ADD COLUMN tsv tsvector;
ALTER TABLE amazon4000  ADD COLUMN tsv tsvector;
ALTER TABLE amazon8000  ADD COLUMN tsv tsvector;
ALTER TABLE amazon16000 ADD COLUMN tsv tsvector;
ALTER TABLE amazon32000 ADD COLUMN tsv tsvector;
ALTER TABLE amazon64000 ADD COLUMN tsv tsvector;

--  CÁLCULO DEL TSVECTOR 

UPDATE amazon1000  SET tsv = to_tsvector('english', text);
UPDATE amazon2000  SET tsv = to_tsvector('english', text);
UPDATE amazon4000  SET tsv = to_tsvector('english', text);
UPDATE amazon8000  SET tsv = to_tsvector('english', text);
UPDATE amazon16000 SET tsv = to_tsvector('english', text);
UPDATE amazon32000 SET tsv = to_tsvector('english', text);
UPDATE amazon64000 SET tsv = to_tsvector('english', text);

--  ÍNDICES GIN PARA OPTIMIZAR LAS CONSULTAS

CREATE INDEX idx_amazon1000  ON amazon1000  USING GIN(tsv);
CREATE INDEX idx_amazon2000  ON amazon2000  USING GIN(tsv);
CREATE INDEX idx_amazon4000  ON amazon4000  USING GIN(tsv);
CREATE INDEX idx_amazon8000  ON amazon8000  USING GIN(tsv);
CREATE INDEX idx_amazon16000 ON amazon16000 USING GIN(tsv);
CREATE INDEX idx_amazon32000 ON amazon32000 USING GIN(tsv);
CREATE INDEX idx_amazon64000 ON amazon64000 USING GIN(tsv);

--  CONSULTAS PARA MEDIR RENDIMIENTO

-- Consulta 1000
EXPLAIN ANALYZE
SELECT doc_id, text FROM amazon1000
WHERE tsv @@ plainto_tsquery('english', 'battery');

-- Consulta 2000
EXPLAIN ANALYZE
SELECT doc_id, text FROM amazon2000
WHERE tsv @@ plainto_tsquery('english', 'wonderful product');

-- Consulta 4000
EXPLAIN ANALYZE
SELECT doc_id, text FROM amazon4000
WHERE tsv @@ plainto_tsquery('english', 'amazing');

-- Consulta 8000
EXPLAIN ANALYZE
SELECT doc_id, text FROM amazon8000
WHERE tsv @@ plainto_tsquery('english', 'refund');

-- Consulta 16000
EXPLAIN ANALYZE
SELECT doc_id, text FROM amazon16000
WHERE tsv @@ plainto_tsquery('english', 'functional');

-- Consulta 32000
EXPLAIN ANALYZE
SELECT doc_id, text FROM amazon32000
WHERE tsv @@ plainto_tsquery('english', 'recommended');

-- Consulta 64000
EXPLAIN ANALYZE
SELECT doc_id, text FROM amazon64000
WHERE tsv @@ plainto_tsquery('english', 'cheap');
