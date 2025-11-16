import os
import pickle
import struct
import heapq
import psutil
from typing import List, Dict, Iterator, Tuple

from .text_preprocessor import TextPreprocessor

class SPIMIBuilder:
    def __init__(self, block_size_mb: int = None, temp_dir: str = "data/temp_blocks", max_buffers: int = None):
        if block_size_mb is None:
            available_memory_mb = psutil.virtual_memory().available // (1024 * 1024)
            self.block_size_mb = max(50, available_memory_mb // 4)  # Usar 1/4 de la memoria disponible
        else:
            self.block_size_mb = block_size_mb
            
        if max_buffers is None:
            available_memory_mb = psutil.virtual_memory().available // (1024 * 1024)
            self.max_buffers = max(10, min(100, available_memory_mb // 10))
        else:
            self.max_buffers = max_buffers
            
        self.temp_dir = temp_dir
        self.preprocessor = TextPreprocessor()
        self.block_counter = 0
        self.merge_pass_counter = 0
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
        block_data = {}
        block_files = []
        current_size_in_bytes = 0
        block_size_bytes = self.block_size_mb * 1024 * 1024
        docs_processed = 0

        for doc_id, doc in documents:
            try:
                text = getattr(doc, field_name, None) if hasattr(doc, field_name) else doc.get(field_name)
                if not text:
                    continue

                if isinstance(text, bytes):
                    text = text.decode('utf-8', errors='ignore').rstrip('\x00').strip()
                    
                tokens = self.preprocessor.preprocess(str(text))
                if not tokens:
                    continue

                term_freq = {}
                for token in tokens:
                    term_freq[token] = term_freq.get(token, 0) + 1

                for token, tf in term_freq.items():
                    if token not in block_data:
                        block_data[token] = []
                        current_size_in_bytes += len(token.encode('utf-8')) + 64  # overhead
                    
                    block_data[token].append((doc_id, tf))
                    current_size_in_bytes += 16 
                    if current_size_in_bytes >= block_size_bytes:
                        print(f"Bloque {self.block_counter} completado con {len(block_data)} términos únicos")
                        block_file = self._create_block(block_data)
                        block_files.append(block_file)
                        block_data = {}
                        current_size_in_bytes = 0

                docs_processed += 1
            except Exception as e:
                continue

        if block_data:
            block_file = self._create_block(block_data)
            block_files.append(block_file)

        return block_files

    def _create_block(self, block_data: Dict) -> str:
        filename = f"block_{self.block_counter:06d}.dat"
        block_file = os.path.join(self.temp_dir, filename)

        serializable_block = {term: sorted(block_data[term]) for term in sorted(block_data.keys())}
        self._write_block_to_disk(serializable_block, block_file)

        self.block_counter += 1
        return block_file

    def _write_block_to_disk(self, block_data: Dict, block_file: str):
        with open(block_file, "wb") as f:
            for term, postings in block_data.items():
                term_bytes = term.encode('utf-8')
                postings_bytes = pickle.dumps(postings, protocol=pickle.HIGHEST_PROTOCOL)

                f.write(struct.pack('I', len(term_bytes)))
                f.write(term_bytes)
                f.write(struct.pack('I', len(postings_bytes)))
                f.write(postings_bytes)
