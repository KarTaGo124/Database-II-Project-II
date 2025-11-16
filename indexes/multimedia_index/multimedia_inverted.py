import os
import numpy as np
import pickle
import time
from .multimedia_base import MultimediaIndexBase
from ..core.performance_tracker import OperationResult
import math
import psutil
from concurrent.futures import ProcessPoolExecutor

class MultimediaInverted(MultimediaIndexBase):

    def __init__(self, index_dir: str, files_dir: str, field_name: str,
                 feature_type: str, n_clusters: int = 100):
        self.method_dir = os.path.join(index_dir, "inverted")
        os.makedirs(self.method_dir, exist_ok=True)

        self.postings_file = os.path.join(self.method_dir, "postings.dat")
        self.norms_file = os.path.join(self.method_dir, "norms.dat")
        self.idf_file = os.path.join(self.method_dir, "idf.dat")
        self.metadata_file = os.path.join(self.method_dir, "metadata.json")

        self.inverted_index = {}
        self.norms = {}

        super().__init__(index_dir, files_dir, field_name, feature_type, n_clusters)

    def build(self, records, use_multiprocessing: bool = True, n_workers: int = None):
        
        filenames = []
        doc_ids = []
        for rec in records:
            fname = getattr(rec, self.field_name, None)
            if fname:
                filenames.append(fname)
                doc_ids.append(rec.get_key())

        if self.codebook is None:
            print("Construyendo codebook...")
            if use_multiprocessing:
                codebook_batch_size = 50
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
        self.calculate_idf(all_histograms)
        inverted_index = {i: [] for i in range(self.n_clusters)}
        norms = {}

        for doc_id, hist in all_histograms.items():
            norm = np.linalg.norm(hist)
            norms[doc_id] = norm
            for codeword_id, tf in enumerate(hist):
                if tf > 0:
                    inverted_index[codeword_id].append((doc_id, tf))

        self.inverted_index = inverted_index
        self.norms = norms
        
        self._persist()
        
    def search(self, query_filename: str, top_k: int = 8) -> OperationResult:
        start_time = time.time()
        query_vec = self.get_tf_idf_vector(query_filename)
        if query_vec is None:
            return OperationResult(data=[], execution_time_ms=0, disk_reads=0, disk_writes=0)

        scores = {}
        for codeword_id, q_weight in enumerate(query_vec):
            if q_weight > 0:  
                postings = self._read_postings_list(codeword_id)
                for doc_id, tf in postings:
                    doc_weight = tf * self.idf.get(codeword_id, 0.0)
                    scores[doc_id] = scores.get(doc_id, 0.0) + q_weight * doc_weight
        
        q_norm = np.linalg.norm(query_vec)
        for doc_id in scores:
            d_norm = self.norms.get(doc_id, 1.0)
            if q_norm > 0 and d_norm > 0:
                scores[doc_id] /= (q_norm * d_norm)
            else:
                scores[doc_id] = 0.0

        top_docs = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_k]
        exec_time = (time.time() - start_time) * 1000
        return OperationResult(data=top_docs, execution_time_ms=exec_time, disk_reads=0, disk_writes=0)



    def _read_postings_list(self, codeword_id: int):
        if self.inverted_index and codeword_id in self.inverted_index:
            return self.inverted_index[codeword_id]
        if os.path.exists(self.postings_file):
            with open(self.postings_file, 'rb') as f:
                postings = pickle.load(f)
            return postings.get(codeword_id, [])
        return []


    def _write_postings_list(self, codeword_id: int, postings: list):
        if os.path.exists(self.postings_file):
            with open(self.postings_file, 'rb') as f:
                all_postings = pickle.load(f)
        else:
            all_postings = {}
        all_postings[codeword_id] = postings
        with open(self.postings_file, 'wb') as f:
            pickle.dump(all_postings, f)

    def _persist(self):
        with open(self.postings_file, 'wb') as f:
            pickle.dump(self.inverted_index, f)
        with open(self.norms_file, 'wb') as f:
            pickle.dump(self.norms, f)
        with open(self.idf_file, 'wb') as f:
            pickle.dump(self.idf, f)
        self._save_metadata()

    def _load_if_exists(self):
        if os.path.exists(self.postings_file):
            with open(self.postings_file, 'rb') as f:
                self.inverted_index = pickle.load(f)
        if os.path.exists(self.norms_file):
            with open(self.norms_file, 'rb') as f:
                self.norms = pickle.load(f)
        if os.path.exists(self.idf_file):
            with open(self.idf_file, 'rb') as f:
                self.idf = pickle.load(f)
        self._load_metadata()

    def _save_metadata(self):
        metadata = {
            'n_clusters': self.n_clusters,
            'feature_type': self.feature_type,
            'field_name': self.field_name,
            'postings_file': self.postings_file,
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