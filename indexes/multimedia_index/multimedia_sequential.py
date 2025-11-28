import os
import numpy as np
import heapq
import pickle
import time
import psutil
from concurrent.futures import ProcessPoolExecutor
from .multimedia_base import MultimediaIndexBase
from ..core.performance_tracker import OperationResult

class MultimediaSequential(MultimediaIndexBase):

    def __init__(self, index_dir: str, files_dir: str, field_name: str,
                 feature_type: str, n_clusters: int = None, filename_pattern: str = None):
        self.method_dir = os.path.join(index_dir, "sequential")
        os.makedirs(self.method_dir, exist_ok=True)

        self.histograms_file = os.path.join(self.method_dir, "histograms.dat")
        self.norms_file = os.path.join(self.method_dir, "norms.dat")
        self.idf_file = os.path.join(self.method_dir, "idf.dat")
        self.metadata_file = os.path.join(self.method_dir, "metadata.json")

        self.histograms = {}
        self.norms = {}

        super().__init__(index_dir, files_dir, field_name, feature_type, n_clusters, filename_pattern=filename_pattern)

    def build(self, records, use_multiprocessing: bool = True, n_workers: int = None):
        start_time = time.time()

        filenames = []
        doc_ids = []
        for rec in records:
            fname = self.resolve_filename(rec)
            if fname:
                filenames.append(fname)
                doc_ids.append(rec.get_key())

        if self.codebook is None:
            print("Construyendo codebook...")
            if use_multiprocessing:
                codebook_batch_size = 200
                self.build_codebook(
                    filenames=filenames,
                    n_workers=n_workers,
                    batch_size=codebook_batch_size
                )
            else:
                self.build_codebook(filenames=filenames, n_workers=1, batch_size=len(filenames))

        total_ram = psutil.virtual_memory().available
        ram_to_use = int(total_ram * 0.8)
        bytes_per_hist = self.n_clusters * 4
        batch_size = max(1, ram_to_use // (bytes_per_hist * 2))  

        all_histograms = {}
        
        print(f"Construyendo histogramas para {len(filenames)} archivos en batches de {batch_size}...")
        
        for batch_start in range(0, len(filenames), batch_size):
            batch_files = filenames[batch_start:batch_start + batch_size]
            batch_doc_ids = doc_ids[batch_start:batch_start + batch_size]
            
            print(f"Procesando batch de histogramas {batch_start//batch_size + 1}: {len(batch_files)} archivos")

            for i, f in enumerate(batch_files):
                hist = self.build_histogram(f, normalize=True)
                if hist is not None:
                    all_histograms[batch_doc_ids[i]] = hist

        self.histograms = all_histograms
        self.calculate_idf(self.histograms)

        self.norms = {doc_id: np.linalg.norm(hist) for doc_id, hist in self.histograms.items()}

        self._persist()

        elapsed = (time.time() - start_time) * 1000
        return OperationResult(
            data=f"Built sequential index with {len(self.histograms)} files",
            execution_time_ms=elapsed,
            disk_reads=len(filenames),
            disk_writes=4
        )

    def search(self, query_filename: str, top_k: int = 8) -> OperationResult:
        start_time = time.time()
        query_vec = self.get_tf_idf_vector(query_filename)
        if query_vec is None or len(self.histograms) == 0:
            return OperationResult(data=[], execution_time_ms=0, disk_reads=0, disk_writes=0)

        query_basename = os.path.splitext(os.path.basename(query_filename))[0]

        scores = {}
        q_norm = np.linalg.norm(query_vec)
        for doc_id, doc_vec in self.histograms.items():
            doc_id_str = str(doc_id).strip()
            if hasattr(doc_id, 'decode'):
                doc_id_str = doc_id.decode('utf-8').strip()
            doc_basename = os.path.splitext(doc_id_str)[0]
            
            if doc_basename == query_basename:
                continue
                
            d_norm = self.norms.get(doc_id, 1.0)
            if q_norm > 0 and d_norm > 0:
                score = float(np.dot(query_vec, doc_vec) / (q_norm * d_norm))
            else:
                score = 0.0
            scores[doc_id] = score

        top_docs = heapq.nlargest(top_k, scores.items(), key=lambda x: x[1])
        exec_time = (time.time() - start_time) * 1000

        return OperationResult(data=top_docs, execution_time_ms=exec_time, disk_reads=0, disk_writes=0)

    def _persist(self):
        with open(self.histograms_file, 'wb') as f:
            pickle.dump(self.histograms, f)
        with open(self.norms_file, 'wb') as f:
            pickle.dump(self.norms, f)
        with open(self.idf_file, 'wb') as f:
            pickle.dump(self.idf, f)
        self._save_metadata()

    def _load_if_exists(self):
        if os.path.exists(self.histograms_file):
            with open(self.histograms_file, 'rb') as f:
                self.histograms = pickle.load(f)
        if os.path.exists(self.norms_file):
            with open(self.norms_file, 'rb') as f:
                self.norms = pickle.load(f)
        if os.path.exists(self.idf_file):
            with open(self.idf_file, 'rb') as f:
                self.idf = pickle.load(f)
        self._load_metadata()

    def warm_up(self):
        super()._load_if_exists()
        self._load_if_exists()

    def _save_metadata(self):
        metadata = {
            'n_clusters': self.n_clusters,
            'feature_type': self.feature_type,
            'field_name': self.field_name,
            'histograms_file': self.histograms_file,
            'norms_file': self.norms_file,
            'idf_file': self.idf_file
        }
        import json
        with open(self.metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)


    def _load_metadata(self):
        if os.path.exists(self.metadata_file):
            import json
            with open(self.metadata_file, 'r') as f:
                metadata = json.load(f)
            self.n_clusters = metadata.get('n_clusters', self.n_clusters)
            self.feature_type = metadata.get('feature_type', self.feature_type)
            self.field_name = metadata.get('field_name', self.field_name)