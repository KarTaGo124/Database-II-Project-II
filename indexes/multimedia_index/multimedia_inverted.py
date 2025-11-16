import os
import numpy as np
import pickle
import time
from .multimedia_base import MultimediaIndexBase
from ..core.performance_tracker import OperationResult
import math
import psutil
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

            from concurrent.futures import ProcessPoolExecutor
            with ProcessPoolExecutor() as executor:
                results = list(executor.map(
                    lambda f: self.build_histogram(f, normalize=True), batch_files
                ))
            for doc_id, hist in zip(batch_doc_ids, results):
                if hist is not None:
                    all_histograms[doc_id] = hist
    def search(self, query_filename: str, top_k: int = 8) -> OperationResult:
        pass

    def _read_postings_list(self, codeword_id: int):
        pass

    def _write_postings_list(self, codeword_id: int, postings: list):
        pass

    def _persist(self):
        pass

    def _load_if_exists(self):
        pass

    def _save_metadata(self):
        pass

    def _load_metadata(self):
        pass
