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

    def build_index(self, documents: Iterator, field_name: str):
        pass

    def _process_documents_in_blocks(self, documents: Iterator, field_name: str):
        pass

    def _create_block(self, block_data: Dict) -> str:
        pass

    def _write_block_to_disk(self, block_data: Dict, block_file: str):
        pass

    def _get_block_size_mb(self, block_data: Dict) -> float:
        pass

    def merge_blocks(self, block_files: List[str], output_file: str):
        pass

    def _open_all_blocks(self, block_files: List[str]) -> List:
        pass

    def _merge_with_buffers(self, block_iterators: List, output_file: str):
        pass

    def _get_next_term(self, iterators: List) -> str:
        pass

    def _cleanup_temp_files(self):
        pass
