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
        super().__init__(index_dir, files_dir, field_name, feature_type, n_clusters)

        self.method_dir = os.path.join(index_dir, "inverted")
        os.makedirs(self.method_dir, exist_ok=True)

        self.postings_file = os.path.join(self.method_dir, "postings.dat")
        self.norms_file = os.path.join(self.method_dir, "norms.dat")
        self.idf_file = os.path.join(self.method_dir, "idf.dat")
        self.metadata_file = os.path.join(self.method_dir, "metadata.json")

        self.inverted_index = {}
        self.norms = {}

        self._load_if_exists()

    def build(self, records):
        filenames = []
        doc_ids = []
        for rec in records:
            fname = getattr(rec, self.field_name, None)
            if fname:
                filenames.append(fname)
                doc_ids.append(rec.get_key())
        total_ram = psutil.virtual_memory().available
        ram_to_use = int(total_ram * 0.8)
        bytes_per_hist = self.n_clusters * 4
        batch_size = max(1, ram_to_use // (bytes_per_hist * 2))  # factor 2 por seguridad
        all_histograms = {}
        for batch_start in range(0, len(filenames), batch_size):
            batch_files = filenames[batch_start:batch_start + batch_size]
            batch_doc_ids = doc_ids[batch_start:batch_start + batch_size]

            with ProcessPoolExecutor() as executor:
                results = list(executor.map(
                    lambda f: self.build_histogram(f, normalize=True), batch_files
                ))
            for doc_id, hist in zip(batch_doc_ids, results):
                if hist is not None:
                    all_histograms[doc_id] = hist
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
        query_vec = self.get_tf_idf_vector(query_filename)
        if query_vec is None:
            return OperationResult(data=[], execution_time_ms=0, disk_reads=0, disk_writes=0)

        scores = {}
        for codeword_id, q_weight in enumerate(query_vec):
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
        return OperationResult(data=top_docs, execution_time_ms=0, disk_reads=0, disk_writes=0)



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
        pass

    def _save_metadata(self):
        pass

    def _load_metadata(self):
        pass
