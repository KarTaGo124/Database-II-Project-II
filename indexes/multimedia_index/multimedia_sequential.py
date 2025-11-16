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
                 feature_type: str, n_clusters: int = 100):
        super().__init__(index_dir, files_dir, field_name, feature_type, n_clusters)

        self.method_dir = os.path.join(index_dir, "sequential")
        os.makedirs(self.method_dir, exist_ok=True)

        self.histograms_file = os.path.join(self.method_dir, "histograms.dat")
        self.norms_file = os.path.join(self.method_dir, "norms.dat")
        self.idf_file = os.path.join(self.method_dir, "idf.dat")
        self.metadata_file = os.path.join(self.method_dir, "metadata.json")

        self.histograms = {}
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

        self.histograms = all_histograms
        self.calculate_idf(self.histograms)

        self.norms = {doc_id: np.linalg.norm(hist) for doc_id, hist in self.histograms.items()}

        self._persist()

    def search(self, query_filename: str, top_k: int = 8) -> OperationResult:
        start_time = time.time()
        query_vec = self.get_tf_idf_vector(query_filename)
        if query_vec is None or len(self.histograms) == 0:
            return OperationResult(data=[], execution_time_ms=0, disk_reads=0, disk_writes=0)

        scores = {}
        q_norm = np.linalg.norm(query_vec)
        for doc_id, doc_vec in self.histograms.items():
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

    def _save_metadata(self):
        metadata = {
            'n_clusters': self.n_clusters,
            'feature_type': self.feature_type,
            'field_name': self.field_name,
            'histograms_file': self.histograms_file,
            'norms_file': self.norms_file,
            'idf_file': self.idf_file
        }
        with open(self.metadata_file, 'w') as f:
            pickle.dump(metadata, f)


    def _load_metadata(self):
        if os.path.exists(self.metadata_file):
            with open(self.metadata_file, 'rb') as f:
                metadata = pickle.load(f)
            self.n_clusters = metadata.get('n_clusters', self.n_clusters)
            self.feature_type = metadata.get('feature_type', self.feature_type)
            self.field_name = metadata.get('field_name', self.field_name)