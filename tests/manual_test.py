#!/usr/bin/env python3
"""
Script para ejecutar tests de multimedia
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock Record class para tests
class Record:
    def __init__(self, key, filename):
        self.key = key
        self.data = {'filename': filename}
    
    def get_key(self):
        return self.key
    
    def __getattr__(self, name):
        if name == 'filename':
            return self.data.get('filename')
        return self.data.get(name)

def test_multimedia_with_synthetic_data():
    try:
        import numpy as np
        import cv2
        import tempfile
        import shutil
        from indexes.multimedia_index.multimedia_sequential import MultimediaSequential
        from indexes.multimedia_index.multimedia_inverted import MultimediaInverted
        

        
        print("="*60)
        print("TESTS DE MULTIMEDIA CON DATOS SINTÉTICOS")
        print("="*60)
        
        # Crear directorio temporal
        temp_dir = tempfile.mkdtemp()
        files_dir = os.path.join(temp_dir, "test_files")
        index_dir = os.path.join(temp_dir, "test_index")
        os.makedirs(files_dir, exist_ok=True)
        
        print("Test 1: Generando imágenes sintéticas...")
        image_files = []
        for i in range(5):
            # Generar imagen sintética con diferentes patrones
            img = np.random.randint(0, 255, (100, 100), dtype=np.uint8)
            # Agregar algunos patrones únicos
            if i % 2 == 0:
                cv2.rectangle(img, (20, 20), (80, 80), 255, 2)
            else:
                cv2.circle(img, (50, 50), 30, 255, 2)
            
            filename = f"test_image_{i}.png"
            filepath = os.path.join(files_dir, filename)
            cv2.imwrite(filepath, img)
            image_files.append(filename)
            
        print(f"✓ Generadas {len(image_files)} imágenes sintéticas")
        
        print("\nTest 2: Creando registros mock...")
        records = []
        for i, filename in enumerate(image_files):
            record = Record(key=i, filename=filename)
            records.append(record)
        print(f"✓ Creados {len(records)} registros")
        
        print("\nTest 3: Probando MultimediaSequential...")
        seq_index = MultimediaSequential(
            index_dir=os.path.join(index_dir, "sequential"), 
            files_dir=files_dir,
            field_name='filename',
            feature_type='SIFT',
            n_clusters=10
        )
        
        # Construir codebook primero
        print("  - Construyendo codebook...")
        seq_index.build_codebook(image_files)
        print(f"  ✓ Codebook construido: {seq_index.codebook.shape}")
        
        # Construir índice
        print("  - Construyendo índice secuencial...")
        seq_index.build(records)
        print(f"  ✓ Histogramas: {len(seq_index.histograms)}")
        print(f"  ✓ Normas: {len(seq_index.norms)}")
        
        # Búsqueda
        print("  - Probando búsqueda...")
        result = seq_index.search(image_files[0], top_k=3)
        print(f"  ✓ Resultados: {len(result.data)}")
        print(f"  ✓ Tiempo: {result.execution_time_ms:.2f}ms")
        
        print("\nTest 4: Probando MultimediaInverted...")
        inv_index = MultimediaInverted(
            index_dir=os.path.join(index_dir, "inverted"),
            files_dir=files_dir,
            field_name='filename', 
            feature_type='SIFT',
            n_clusters=10
        )
        
        # Usar el mismo codebook
        inv_index.codebook = seq_index.codebook
        inv_index.idf = seq_index.idf
        
        # Construir índice invertido
        print("  - Construyendo índice invertido...")
        inv_index.build(records)
        print(f"  ✓ Índice invertido: {len(inv_index.inverted_index)} términos")
        print(f"  ✓ Normas: {len(inv_index.norms)}")
        
        # Búsqueda
        print("  - Probando búsqueda...")
        result = inv_index.search(image_files[0], top_k=3)
        print(f"  ✓ Resultados: {len(result.data)}")
        print(f"  ✓ Tiempo: {result.execution_time_ms:.2f}ms")
        
        print("\nTest 5: Comparando resultados...")
        seq_result = seq_index.search(image_files[0], top_k=3)
        inv_result = inv_index.search(image_files[0], top_k=3)
        
        print(f"  Sequential top-3: {[doc_id for doc_id, score in seq_result.data]}")
        print(f"  Inverted top-3: {[doc_id for doc_id, score in inv_result.data]}")
        
        # Verificar estadísticas
        print(f"\nTest 6: Estadísticas...")
        seq_stats = seq_index.get_statistics()
        inv_stats = inv_index.get_statistics() if hasattr(inv_index, 'get_statistics') else {}
        print(f"  Sequential - Clusters: {seq_stats.get('n_clusters', 'N/A')}")
        print(f"  Sequential - Codebook: {seq_stats.get('codebook_built', 'N/A')}")
        print(f"  Inverted - Clusters: {inv_stats.get('n_clusters', 'N/A')}")
        
        # Limpiar
        shutil.rmtree(temp_dir)
        
        print("\n" + "="*60)
        print("🎉 TODOS LOS TESTS DE MULTIMEDIA PASARON")
        print("="*60)
        
        return True
        
    except Exception as e:
        print(f"❌ Error en test multimedia: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_multimedia_memory_usage():
    try:
        import psutil
        import numpy as np
        
        print("="*50) 
        print("TEST DE USO DE MEMORIA")
        print("="*50)
        
        # Simular cálculo de batch size
        total_ram = psutil.virtual_memory().available
        ram_to_use = int(total_ram * 0.8)
        n_clusters = 100
        bytes_per_hist = n_clusters * 4
        batch_size = max(1, ram_to_use // (bytes_per_hist * 2))
        
        print(f"✓ RAM total disponible: {total_ram / (1024**3):.2f} GB")
        print(f"✓ RAM a usar (80%): {ram_to_use / (1024**3):.2f} GB")
        print(f"✓ Bytes por histograma: {bytes_per_hist}")
        print(f"✓ Batch size calculado: {batch_size}")
        
        # Simular procesamiento de batch
        print(f"\nTest: Simulando histogramas en memoria...")
        histograms = {}
        for i in range(min(100, batch_size)):  # Limitar para test
            hist = np.random.rand(n_clusters).astype(np.float32)
            histograms[i] = hist
            
        memory_used = sum(hist.nbytes for hist in histograms.values())
        print(f"✓ Histogramas creados: {len(histograms)}")
        print(f"✓ Memoria usada: {memory_used / (1024**2):.2f} MB")
        print(f"✓ Está dentro del límite: {memory_used < ram_to_use}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error en test de memoria: {e}")
        return False

if __name__ == '__main__':
    success = True
    
    print("Ejecutando tests de multimedia...\n")
    
    success &= test_multimedia_with_synthetic_data() 
    print("\n")
    
    success &= test_multimedia_memory_usage()
    
    if success:
        print("\n🎉 TODOS LOS TESTS DE MULTIMEDIA COMPLETADOS EXITOSAMENTE")
    else:
        print("\n❌ ALGUNOS TESTS FALLARON")
    
    sys.exit(0 if success else 1)
