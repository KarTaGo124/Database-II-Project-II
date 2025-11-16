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
            self.block_size_mb = max(50, available_memory_mb // 4)  
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
            self._cleanup_temp_files(output_file)

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
                        current_size_in_bytes += len(token.encode('utf-8')) + 64  
                    
                    block_data[token].append((doc_id, tf))
                    current_size_in_bytes += 16 
                    if current_size_in_bytes >= block_size_bytes:
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

    def merge_blocks(self, block_files: List[str], output_file: str):
        if not block_files:
            return

        current_files = block_files
        
        while len(current_files) > 1:
            next_files = []
            
            for i in range(0, len(current_files), self.max_buffers):
                batch = current_files[i:i + self.max_buffers]
                
                if len(current_files) <= self.max_buffers:
                    output = output_file
                else:
                    output = os.path.join(
                        self.temp_dir, 
                        f"merged_pass{self.merge_pass_counter}_batch{i//self.max_buffers:03d}.dat"
                    )
                
                self._merge_batch(batch, output)
                next_files.append(output)
            
            if current_files != block_files:
                for f in current_files:
                    try:
                        os.remove(f)
                    except:
                        pass
            
            current_files = next_files
            self.merge_pass_counter += 1

        if len(current_files) == 1 and current_files[0] != output_file:
            os.rename(current_files[0], output_file)

    def _merge_batch(self, block_files: List[str], output_file: str):
        block_readers = self._open_batch_blocks(block_files)
        if not block_readers:
            return
            
        try:
            self._merge_with_buffers(block_readers, output_file)
        finally:
            for reader in block_readers:
                if reader["file_handle"] and not reader["file_handle"].closed:
                    reader["file_handle"].close()

    def _open_batch_blocks(self, block_files: List[str]) -> List:
        block_readers = []
        for bf in block_files:
            try:
                f = open(bf, "rb")
                reader = {
                    "file_handle": f,
                    "iterator": self._read_block_terms(f),
                    "current_term": None,
                    "current_postings": None,
                    "has_next": True,
                    "filename": bf
                }

                try:
                    reader["current_term"], reader["current_postings"] = next(reader["iterator"])
                except StopIteration:
                    reader["has_next"] = False
                    f.close()
                
                if reader["has_next"]:
                    block_readers.append(reader)
                    
            except Exception as e:
                continue

        return block_readers

    def _read_block_terms(self, file_handle):
        while True:
            try:
                term_len_bytes = file_handle.read(4)
                if not term_len_bytes or len(term_len_bytes) < 4:
                    break
                    
                term_len = struct.unpack('I', term_len_bytes)[0]
                if term_len == 0:
                    break
                    
                term = file_handle.read(term_len).decode('utf-8')

                postings_len_bytes = file_handle.read(4)
                if len(postings_len_bytes) < 4:
                    break
                    
                postings_len = struct.unpack('I', postings_len_bytes)[0]
                postings_bytes = file_handle.read(postings_len)
                
                if len(postings_bytes) < postings_len:
                    break
                    
                postings = pickle.loads(postings_bytes)
                yield term, postings
                
            except (struct.error, EOFError, pickle.PickleError):
                break

    def _merge_with_buffers(self, block_readers: List, output_file: str):
        min_heap = []
        
        for i, reader in enumerate(block_readers):
            if reader["has_next"]:
                heapq.heappush(min_heap, (reader["current_term"], i))

        merged_terms = 0
        with open(output_file, "wb") as f_out:
            while min_heap:
                current_term, block_idx = heapq.heappop(min_heap)
                
                postings_to_merge = [block_readers[block_idx]["current_postings"]]
                
                try:
                    block_readers[block_idx]["current_term"], block_readers[block_idx]["current_postings"] = next(block_readers[block_idx]["iterator"])
                    heapq.heappush(min_heap, (block_readers[block_idx]["current_term"], block_idx))
                except StopIteration:
                    block_readers[block_idx]["has_next"] = False
                    if block_readers[block_idx]["file_handle"]:
                        block_readers[block_idx]["file_handle"].close()

                while min_heap and min_heap[0][0] == current_term:
                    _, next_block_idx = heapq.heappop(min_heap)
                    postings_to_merge.append(block_readers[next_block_idx]["current_postings"])
                    
                    try:
                        block_readers[next_block_idx]["current_term"], block_readers[next_block_idx]["current_postings"] = next(block_readers[next_block_idx]["iterator"])
                        heapq.heappush(min_heap, (block_readers[next_block_idx]["current_term"], next_block_idx))
                    except StopIteration:
                        block_readers[next_block_idx]["has_next"] = False
                        if block_readers[next_block_idx]["file_handle"]:
                            block_readers[next_block_idx]["file_handle"].close()

                merged_postings = self._merge_postings(postings_to_merge)
                
                term_bytes = current_term.encode('utf-8')
                postings_bytes = pickle.dumps(merged_postings, protocol=pickle.HIGHEST_PROTOCOL)

                f_out.write(struct.pack('I', len(term_bytes)))
                f_out.write(term_bytes)
                f_out.write(struct.pack('I', len(postings_bytes)))
                f_out.write(postings_bytes)
                
                merged_terms += 1
                if merged_terms % 10000 == 0:
                    print(f"Merged {merged_terms} términos...")

    def _merge_postings(self, postings_lists: List[List[Tuple[int, int]]]) -> List[Tuple[int, int]]:
        doc_tf_map = {}
        
        for postings in postings_lists:
            for doc_id, tf in postings:
                doc_tf_map[doc_id] = doc_tf_map.get(doc_id, 0) + tf
        
        return [(doc_id, doc_tf_map[doc_id]) for doc_id in sorted(doc_tf_map.keys())]

    def get_memory_usage(self):
        memory = psutil.virtual_memory()
        return {
            'total_mb': memory.total // (1024 * 1024),
            'available_mb': memory.available // (1024 * 1024),
            'used_mb': memory.used // (1024 * 1024),
            'percent': memory.percent
        }

    def _cleanup_temp_files(self, output_file: str = None):
        try:
            if os.path.exists(self.temp_dir):
                for fname in os.listdir(self.temp_dir):
                    path = os.path.join(self.temp_dir, fname)
                    try:
                        if os.path.isfile(path):
                            if output_file and os.path.abspath(path) == os.path.abspath(output_file):
                                continue
                            os.remove(path)
                    except Exception as e:
                        print(f"Error eliminando {path}: {e}")
        except FileNotFoundError:
            pass
