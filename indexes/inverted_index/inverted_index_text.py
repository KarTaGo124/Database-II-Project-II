import os
import pickle
import numpy as np
import time
import json
import heapq
from typing import List, Dict, Tuple
from ..core.performance_tracker import OperationResult
from .text_preprocessor import TextPreprocessor
from .spimi_builder import SPIMIBuilder
import struct
from ..core.record import Record

class InvertedTextIndex:

    def __init__(self, index_dir: str, field_name: str):
        self.index_dir = index_dir
        self.field_name = field_name

        self.postings_file = os.path.join(index_dir, "postings.dat")
        self.vocabulary_file = os.path.join(index_dir, "vocabulary.dat")
        self.doc_norms_file = os.path.join(index_dir, "doc_norms.dat")
        self.metadata_file = os.path.join(index_dir, "metadata.json")

        self.preprocessor = TextPreprocessor()
        self.vocabulary = {}
        self.doc_norms = {}
        self.idf = {}
        self.num_documents = 0

        self._load_if_exists()

    def build(self, records: List[Record]):
        records_list = list(records) if not isinstance(records, list) else records
        self.num_documents = len(records_list)

        self._build_with_spimi(records_list)
        self._calculate_tf_idf()
        self._calculate_document_norms()
        self._persist()

    def _build_with_spimi(self, records: List[Record]):
        temp_dir = os.path.join(self.index_dir, "temp_blocks")
        spimi = SPIMIBuilder(block_size_mb=50, temp_dir=temp_dir)

        def doc_generator():
            for record in records:
                doc_id = record.get_key()
                if doc_id is not None:
                    yield (doc_id, record)

        output_file = os.path.join(self.index_dir, "postings.dat")
        spimi.build_index(doc_generator(), self.field_name, output_file)

    def _calculate_tf_idf(self):
        if not os.path.exists(self.postings_file):
            return

        term_doc_freq = {}
        vocabulary = {}

        with open(self.postings_file, 'rb') as f:
            while True:
                offset = f.tell()

                term_len_bytes = f.read(4)
                if not term_len_bytes:
                    break

                term_len = struct.unpack('I', term_len_bytes)[0]
                term = f.read(term_len).decode('utf-8')

                postings_len_bytes = f.read(4)
                postings_len = struct.unpack('I', postings_len_bytes)[0]
                postings_bytes = f.read(postings_len)
                postings = pickle.loads(postings_bytes)

                df = len(postings)
                term_doc_freq[term] = df
                vocabulary[term] = {
                    'offset': offset,
                    'df': df
                }

        self.vocabulary = vocabulary
        self._calculate_idf(term_doc_freq)

    def _calculate_idf(self, term_doc_freq: Dict[str, int]):
        for term, df in term_doc_freq.items():
            if df > 0:
                self.idf[term] = np.log(self.num_documents / df)
            else:
                self.idf[term] = 0.0

            if term in self.vocabulary:
                self.vocabulary[term]['idf'] = self.idf[term]

    def _calculate_document_norms(self):
        if not os.path.exists(self.postings_file):
            return

        doc_vectors = {}

        with open(self.postings_file, 'rb') as f:
            while True:
                term_len_bytes = f.read(4)
                if not term_len_bytes:
                    break

                term_len = struct.unpack('I', term_len_bytes)[0]
                term = f.read(term_len).decode('utf-8')

                postings_len_bytes = f.read(4)
                postings_len = struct.unpack('I', postings_len_bytes)[0]
                postings_bytes = f.read(postings_len)
                postings = pickle.loads(postings_bytes)

                idf = self.idf.get(term, 0.0)

                for doc_id, tf in postings:
                    tf_idf = tf * idf
                    if doc_id not in doc_vectors:
                        doc_vectors[doc_id] = 0.0
                    doc_vectors[doc_id] += tf_idf ** 2

        self.doc_norms = {doc_id: np.sqrt(norm_squared) for doc_id, norm_squared in doc_vectors.items()}

    def search(self, query: str, top_k: int = None) -> OperationResult:
        start_time = time.time()

        query_terms = self._preprocess_query(query)

        if not query_terms:
            return OperationResult(
                data=[],
                execution_time_ms=0,
                disk_reads=0,
                disk_writes=0
            )

        query_vector = self._build_query_vector(query_terms)
        scores = self._search_terms_in_index(query_vector)
        top_results = self._get_top_k_documents(scores, top_k)

        execution_time = (time.time() - start_time) * 1000

        return OperationResult(
            data=top_results,
            execution_time_ms=execution_time,
            disk_reads=0,
            disk_writes=0
        )

    def _preprocess_query(self, query: str) -> List[str]:
        return self.preprocessor.preprocess(query)

    def _build_query_vector(self, query_terms: List[str]) -> Dict[str, float]:
        term_freq = {}
        for term in query_terms:
            term_freq[term] = term_freq.get(term, 0) + 1

        query_vector = {}
        for term, tf in term_freq.items():
            idf = self.idf.get(term, 0.0)
            if idf > 0:
                query_vector[term] = tf * idf

        return query_vector

    def _search_terms_in_index(self, query_vector: Dict[str, float]) -> Dict[int, float]:
        doc_scores = {}

        for term, query_weight in query_vector.items():
            postings = self._read_postings_list(term)
            if not postings:
                continue

            idf = self.idf.get(term, 0.0)

            for doc_id, tf in postings:
                doc_weight = tf * idf

                if doc_id not in doc_scores:
                    doc_scores[doc_id] = 0.0
                doc_scores[doc_id] += query_weight * doc_weight

        query_norm = np.sqrt(sum(w ** 2 for w in query_vector.values()))

        for doc_id in doc_scores:
            doc_norm = self.doc_norms.get(doc_id, 1.0)
            if query_norm > 0 and doc_norm > 0:
                doc_scores[doc_id] = doc_scores[doc_id] / (query_norm * doc_norm)
            else:
                doc_scores[doc_id] = 0.0

        return doc_scores

    def _get_top_k_documents(self, scores: Dict[int, float], k: int = None) -> List[Tuple[int, float]]:
        if not scores:
            return []

        if k is None:
            return sorted(scores.items(), key=lambda x: x[1], reverse=True)

        if len(scores) <= k:
            return sorted(scores.items(), key=lambda x: x[1], reverse=True)

        top_k_heap = []
        for doc_id, score in scores.items():
            if len(top_k_heap) < k:
                heapq.heappush(top_k_heap, (score, doc_id))
            else:
                if score > top_k_heap[0][0]:
                    heapq.heapreplace(top_k_heap, (score, doc_id))

        results = [(doc_id, score) for score, doc_id in sorted(top_k_heap, reverse=True)]
        return results

    def _read_postings_list(self, term: str) -> List[Tuple[int, int]]:
        if term not in self.vocabulary:
            return []

        offset = self.vocabulary[term]['offset']

        try:
            with open(self.postings_file, 'rb') as f:
                f.seek(offset)

                term_len_bytes = f.read(4)
                term_len = struct.unpack('I', term_len_bytes)[0]
                stored_term = f.read(term_len).decode('utf-8')

                if stored_term != term:
                    return []

                postings_len_bytes = f.read(4)
                postings_len = struct.unpack('I', postings_len_bytes)[0]
                postings_bytes = f.read(postings_len)
                postings = pickle.loads(postings_bytes)

                return postings
        except Exception:
            return []

    def _persist(self):
        os.makedirs(self.index_dir, exist_ok=True)

        with open(self.vocabulary_file, 'wb') as f:
            pickle.dump(self.vocabulary, f)

        with open(self.doc_norms_file, 'wb') as f:
            pickle.dump(self.doc_norms, f)

        idf_file = os.path.join(self.index_dir, 'idf.dat')
        with open(idf_file, 'wb') as f:
            pickle.dump(self.idf, f)

        self._save_metadata()

    def _load_if_exists(self):
        if os.path.exists(self.vocabulary_file):
            with open(self.vocabulary_file, 'rb') as f:
                self.vocabulary = pickle.load(f)

        if os.path.exists(self.doc_norms_file):
            with open(self.doc_norms_file, 'rb') as f:
                self.doc_norms = pickle.load(f)

        idf_file = os.path.join(self.index_dir, 'idf.dat')
        if os.path.exists(idf_file):
            with open(idf_file, 'rb') as f:
                self.idf = pickle.load(f)

        self._load_metadata()

    def _save_metadata(self):
        os.makedirs(self.index_dir, exist_ok=True)
        metadata = {
            'field_name': self.field_name,
            'num_documents': self.num_documents,
            'vocabulary_size': len(self.vocabulary),
            'timestamp': int(time.time())
        }
        with open(self.metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)

    def _load_metadata(self):
        if not os.path.exists(self.metadata_file):
            return

        with open(self.metadata_file, 'r', encoding='utf-8') as f:
            metadata = json.load(f)

        self.field_name = metadata.get('field_name', self.field_name)
        self.num_documents = metadata.get('num_documents', 0)