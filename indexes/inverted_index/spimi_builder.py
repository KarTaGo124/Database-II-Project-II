import os
import pickle
import struct
from typing import List, Dict, Iterator
from .text_preprocessor import TextPreprocessor

B = 10 #number of buffers

class SPIMIBuilder:
    TERM_STRUCT = 'ii'
    def __init__(self, block_size_mb: int = 50, temp_dir: str = "data/temp_blocks"):
        self.block_size_mb = block_size_mb
        self.temp_dir = temp_dir
        self.preprocessor = TextPreprocessor()
        self.block_counter = 0
        os.makedirs(self.temp_dir, exist_ok=True)

    def build_index(self, documents: Iterator, field_name: str, output_file: str):
        try:
            block_files = self._process_documents_in_blocks(documents, field_name)
            if not block_files:
                return None

            self.merge_blocks(block_files, output_file)
            return output_file
        finally:
            self._cleanup_temp_files()

    def _process_documents_in_blocks(self, documents: Iterator, field_name: str):
        block_data = {} #dict
        block_files = [] #list of blocks

        for doc_id, doc in documents:
            text = doc.get(field_name) #get text document based on the field name where it actually is
            if not text:
                continue

            tokens = self.preprocessor.preprocess(text) #all the preprocessing

            for token in tokens: #add al tokens
                if token not in block_data:
                    block_data[token] = set() #if not token in dic, create it
                block_data[token].add(doc_id) #add doc id in token

            if self._get_block_size_mb(block_data) >= self.block_size_mb: #check for size after entering whole document in a block
                block_file = self._create_block(block_data) #dump block
                block_files.append(block_file)
                block_data = {}

        if block_data:
            block_file = self._create_block(block_data)
            block_files.append(block_file)

        return block_files

    def _create_block(self, block_data: Dict) -> str:
        filename = f"block_{self.block_counter:06d}.pkl"
        block_file = os.path.join(self.temp_dir, filename)

        serializable_block = {term: sorted(list(postings)) for term, postings in block_data.items()}
        self._write_block_to_disk(serializable_block, block_file)

        self.block_counter += 1
        return block_file

    def _write_block_to_disk(self, block_data: Dict, block_file: str):
        with open(block_file, "wb") as f:
            pickle.dump(block_data, f, protocol=pickle.HIGHEST_PROTOCOL)

    def _get_block_size_mb(self, block_data: Dict) -> float:
        temp_serializable = {k: list(v) for k, v in block_data.items()}
        data_bytes = pickle.dumps(temp_serializable, protocol=pickle.HIGHEST_PROTOCOL)
        size_mb = len(data_bytes) / (1024 * 1024)
        return size_mb

    def merge_blocks(self, block_files: List[str], output_file: str):
        pass

    def _open_all_blocks(self, block_files: List[str]) -> List:
        blocks = []
        for bf in block_files:
            with open(bf, "rb") as f:
                data: Dict[str, List[int]] = pickle.load(f)
            terms_sorted = sorted(data.keys())
            blocks.append({
                "terms": terms_sorted,
                "postings": data,
                "idx": 0,
                "path": bf,
                "fileobj": None
            })
        return blocks

    def _merge_with_buffers(self, block_iterators: List, output_file: str):
        pass

    def _get_next_term(self, iterators: List) -> str:
        min_term = None
        for blk in iterators:
            idx = blk["idx"]
            terms = blk["terms"]
            if idx < len(terms):
                term = terms[idx]
                if min_term is None or term < min_term:
                    min_term = term
        return min_term

    def _cleanup_temp_files(self):
        try:
            for fname in os.listdir(self.temp_dir):
                path = os.path.join(self.temp_dir, fname)
                try:
                    os.remove(path)
                except Exception:
                    pass
        except FileNotFoundError:
            pass
