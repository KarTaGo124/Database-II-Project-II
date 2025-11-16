#!/usr/bin/env python3
"""
Test completo de multiprocessing para clases MultimediaSequential y MultimediaInverted
"""

import os
import sys
import numpy as np
import cv2
import logging
import time
from pathlib import Path

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Agregar el directorio raíz al path
sys.path.append('/home/zamirlm/Documents/Utec/Ciclo2025-2/BD2/Database-II-Project-II')

from indexes.multimedia_index.multimedia_sequential import MultimediaSequential
from indexes.multimedia_index.multimedia_inverted import MultimediaInverted

class MockRecord:
    """Mock de Record para testing"""
    def __init__(self, filename, doc_id):
        self.filename = filename
        self.doc_id = doc_id
    
    def get_key(self):
        return self.doc_id
    
    def __getattr__(self, name):
        if name == 'filename':
            return self.filename
        elif name == 'image_field':
            return self.filename
        else:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")


def create_test_images(num_images: int = 30) -> tuple[str, list]:
    """Crea imágenes sintéticas para testing"""
    test_dir = "/tmp/test_multimedia_complete"
    os.makedirs(test_dir, exist_ok=True)
    
    records = []
    
    for i in range(num_images):
        # Crear imagen sintética con patrones más complejos
        img = np.zeros((300, 300, 3), dtype=np.uint8)
        
        if i % 5 == 0:
            # Imagen con círculos concéntricos
            for radius in range(30, 150, 20):
                cv2.circle(img, (150, 150), radius, (255 - radius, radius, 128), 2)
        elif i % 5 == 1:
            # Imagen con cuadrícula
            for x in range(0, 300, 30):
                cv2.line(img, (x, 0), (x, 300), (255, 255, 255), 1)
            for y in range(0, 300, 30):
                cv2.line(img, (0, y), (300, y), (255, 255, 255), 1)
        elif i % 5 == 2:
            # Imagen con líneas diagonales
            for x in range(0, 300, 15):
                cv2.line(img, (x, 0), (300, x), (255, 128, 0), 2)
        elif i % 5 == 3:
            # Imagen con rectángulos anidados
            for size in range(50, 200, 30):
                top_left = (150 - size//2, 150 - size//2)
                bottom_right = (150 + size//2, 150 + size//2)
                cv2.rectangle(img, top_left, bottom_right, (size, 255-size, 128), 2)
        else:
            # Imagen con patrones aleatorios y formas
            img = np.random.randint(0, 255, (300, 300, 3), dtype=np.uint8)
            cv2.circle(img, (100, 100), 40, (0, 255, 0), -1)
            cv2.rectangle(img, (200, 200), (280, 280), (255, 0, 0), -1)
        
        filename = f"test_image_{i:03d}.jpg"
        filepath = os.path.join(test_dir, filename)
        cv2.imwrite(filepath, img)
        
        # Crear mock record
        record = MockRecord(filename, f"doc_{i:03d}")
        records.append(record)
    
    return test_dir, records


def test_multiprocessing_complete():
    """Test completo de multiprocessing para multimedia indexes"""
    print("Iniciando test completo de multiprocessing multimedia...")
    
    # Crear imágenes sintéticas
    files_dir, records = create_test_images(30)
    print(f"Creadas {len(records)} imágenes sintéticas en {files_dir}")
    
    # Configurar directorios
    index_dir_seq = "/tmp/test_multimedia_sequential"
    index_dir_inv = "/tmp/test_multimedia_inverted"
    os.makedirs(index_dir_seq, exist_ok=True)
    os.makedirs(index_dir_inv, exist_ok=True)
    
    try:
        # Test MultimediaSequential con multiprocessing
        print("\n Probando MultimediaSequential con multiprocessing...")
        
        multimedia_seq = MultimediaSequential(
            index_dir=index_dir_seq,
            files_dir=files_dir,
            field_name="image_field",
            feature_type="SIFT",
            n_clusters=50
        )
        
        start_time = time.time()
        multimedia_seq.build(records, use_multiprocessing=True, n_workers=3)
        build_time_seq = time.time() - start_time
        
        print(f"MultimediaSequential construido en {build_time_seq:.2f}s")
        print(f" Codebook shape: {multimedia_seq.codebook.shape}")
        print(f" Histogramas: {len(multimedia_seq.histograms)}")
        print(f" Normas: {len(multimedia_seq.norms)}")
        
        # Test búsqueda en sequential
        query_result = multimedia_seq.search(records[0].filename, top_k=5)
        print(f" Búsqueda: {len(query_result.data)} resultados en {query_result.execution_time_ms:.2f}ms")
        
        # Test MultimediaInverted con multiprocessing
        print("\n🧪 Probando MultimediaInverted con multiprocessing...")
        
        multimedia_inv = MultimediaInverted(
            index_dir=index_dir_inv,
            files_dir=files_dir,
            field_name="image_field",
            feature_type="SIFT",
            n_clusters=50
        )
        
        start_time = time.time()
        multimedia_inv.build(records, use_multiprocessing=True, n_workers=3)
        build_time_inv = time.time() - start_time
        
        print(f"MultimediaInverted construido en {build_time_inv:.2f}s")
        print(f" Codebook shape: {multimedia_inv.codebook.shape}")
        print(f" Índice invertido: {len(multimedia_inv.inverted_index)} codewords")
        print(f" Normas: {len(multimedia_inv.norms)}")
        
        # Test búsqueda en inverted
        query_result_inv = multimedia_inv.search(records[0].filename, top_k=5)
        print(f" Búsqueda: {len(query_result_inv.data)} resultados en {query_result_inv.execution_time_ms:.2f}ms")
        
        # Comparar performance
        print(f"\nComparación de performance:")
        print(f" Sequential build: {build_time_seq:.2f}s")
        print(f" Inverted build: {build_time_inv:.2f}s")
        print(f" Sequential search: {query_result.execution_time_ms:.2f}ms")
        print(f" Inverted search: {query_result_inv.execution_time_ms:.2f}ms")
        
        # Test sin multiprocessing para comparar
        print(f"\n Comparando con procesamiento secuencial...")
        
        multimedia_seq_nosmp = MultimediaSequential(
            index_dir="/tmp/test_multimedia_seq_nosmp",
            files_dir=files_dir,
            field_name="image_field",
            feature_type="SIFT",
            n_clusters=50
        )
        
        start_time = time.time()
        multimedia_seq_nosmp.build(records[:10], use_multiprocessing=False)  # Solo 10 imágenes
        build_time_nosmp = time.time() - start_time
        
        print(f"Sequential sin multiprocessing (10 imágenes): {build_time_nosmp:.2f}s")
        
        # Obtener estadísticas detalladas
        print(f"\nEstadísticas detalladas:")
        
        stats_seq = multimedia_seq.get_statistics()
        print(f"   Sequential:")
        for key, value in stats_seq.items():
            print(f"      {key}: {value}")
        
        stats_inv = multimedia_inv.get_statistics()
        print(f"   Inverted:")
        for key, value in stats_inv.items():
            print(f"      {key}: {value}")
        
        print("\n¡Todos los tests de multiprocessing completo pasaron exitosamente!")
        return True
        
    except Exception as e:
        print(f" Error en test: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Limpiar archivos de prueba
        import shutil
        dirs_to_clean = [
            files_dir,
            index_dir_seq,
            index_dir_inv,
            "/tmp/test_multimedia_seq_nosmp"
        ]
        
        for dir_path in dirs_to_clean:
            if os.path.exists(dir_path):
                shutil.rmtree(dir_path)
        
        print(" Archivos de prueba limpiados")


if __name__ == "__main__":
    success = test_multiprocessing_complete()
    sys.exit(0 if success else 1)
