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

class InvertedTextIndex:

    def __init__(self, index_dir: str, field_name: str):
        self.index_dir = index_dir
        self.field_name = field_name

        self.index_file = os.path.join(index_dir, "inverted_index.dat")
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

    def build(self, records):
        records_list = list(records) if not isinstance(records, list) else records
        self.num_documents = len(records_list)

        self._build_with_spimi(records_list)
        self._calculate_tf_idf()
        self._calculate_document_norms()
        self._persist()

    def _build_with_spimi(self, records):
        temp_dir = os.path.join(self.index_dir, "temp_blocks")
        spimi = SPIMIBuilder(block_size_mb=50, temp_dir=temp_dir)

        def doc_generator():
            for record in records:
                doc_id = record.get('id') or record.get('__id__')
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
        pass

    def search(self, query: str, top_k: int = 10) -> OperationResult:
        pass

    def _preprocess_query(self, query: str) -> List[str]:
        return self.preprocessor.preprocess(query)

    def _build_query_vector(self, query_terms: List[str]) -> Dict[str, float]:
        pass

    def _search_terms_in_index(self, query_vector: Dict[str, float]) -> Dict[int, float]:
        pass

    def _calculate_cosine_similarity(self, doc_id: int, query_vector: Dict[str, float],
                                    doc_vector: Dict[str, float]) -> float:
        pass

    def _get_top_k_documents(self, scores: Dict[int, float], k: int) -> List[Tuple[int, float]]:
        pass

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

        #  vocabulario 
        if os.path.exists(self.vocabulary_file):
            with open(self.vocabulary_file, 'rb') as f:
                self.vocabulary = pickle.load(f)
        
        #  normas de documentos
        if os.path.exists(self.doc_norms_file):
            with open(self.doc_norms_file, 'rb') as f:
                self.doc_norms = pickle.load(f)
        
        # IDF
        idf_file = os.path.join(self.index_dir, 'idf.dat')
        if os.path.exists(idf_file):
            with open(idf_file, 'rb') as f:
                self.idf = pickle.load(f)
        
        # metadata
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

    def _read_postings_list(self, term: str):
        pass

    def _write_postings_list(self, term: str, postings: list):
        pass