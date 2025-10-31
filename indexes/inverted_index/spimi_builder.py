import os
import pickle
from typing import List, Dict, Iterator
from .text_preprocessor import TextPreprocessor

class SPIMIBuilder:

    def __init__(self, block_size_mb: int = 50, temp_dir: str = "data/temp_blocks"):
        self.block_size_mb = block_size_mb
        self.temp_dir = temp_dir
        self.preprocessor = TextPreprocessor()
        self.block_counter = 0
        os.makedirs(self.temp_dir, exist_ok=True)

    def build_index(self, documents: Iterator, field_name: str):
        block_files = self._process_documents_in_blocks(documents, field_name)
        return block_files

    def _process_documents_in_blocks(self, documents: Iterator, field_name: str):
        pass

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
        pass

    def merge_blocks(self, block_files: List[str], output_file: str):
        pass

    def _open_all_blocks(self, block_files: List[str]) -> List:
        pass

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
