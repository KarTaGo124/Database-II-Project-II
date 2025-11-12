import os
import pickle
import struct
import heapq
from typing import List, Dict, Iterator
from .text_preprocessor import TextPreprocessor

class SPIMIBuilder:
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
        current_size_in_bytes = 0
        # Convert block_size_mb to bytes
        block_size_bytes = self.block_size_mb * 1024 * 1024

        for doc_id, doc in documents:
            text = doc.get(field_name) #get text document based on the field name where it actually is
            if not text:
                continue

            tokens = self.preprocessor.preprocess(text) #all the preprocessing

            for token in tokens: #add all tokens
                if token not in block_data:
                    block_data[token] = [] #if not token in dic, create it
                    current_size_in_bytes += len(token)
                block_data[token].append(doc_id) #add doc id in token
                current_size_in_bytes += 4 # Assuming 4 bytes per doc_id

                # Check block size after adding each token
                if current_size_in_bytes >= block_size_bytes:
                    block_file = self._create_block(block_data) #dump block
                    block_files.append(block_file)
                    block_data = {} #reset block_data for the next block
                    current_size_in_bytes = 0 #reset size counter

        if block_data: #leftover block
            block_file = self._create_block(block_data)
            block_files.append(block_file)

        return block_files

    def _create_block(self, block_data: Dict) -> str:
        filename = f"block_{self.block_counter:06d}.pkl"
        block_file = os.path.join(self.temp_dir, filename)

        serializable_block = {term: block_data[term] for term in sorted(block_data.keys())}
        self._write_block_to_disk(serializable_block, block_file)

        self.block_counter += 1
        return block_file

    def _write_block_to_disk(self, block_data: Dict, block_file: str):
        with open(block_file, "wb") as f:
            for term, postings in block_data.items():
                term_bytes = term.encode('utf-8')
                postings_bytes = pickle.dumps(postings, protocol=pickle.HIGHEST_PROTOCOL)

                # write term length, term, postings length, postings
                f.write(struct.pack('I', len(term_bytes)))
                f.write(term_bytes)
                f.write(struct.pack('I', len(postings_bytes)))
                f.write(postings_bytes)

    def merge_blocks(self, block_files: List[str], output_file: str):
        if not block_files:
            return
        block_iterators = self._open_all_blocks(block_files)
        self._merge_with_buffers(block_iterators, output_file)

    def _open_all_blocks(self, block_files: List[str]) -> List:
        block_readers = []
        for bf in block_files:
            f = open(bf, "rb")
            reader = {
                "file_handle": f,
                "iterator": self._read_block_terms(f),
                "current_term": None,
                "current_postings": None,
                "has_next": True
            }

            try: #assign first element to each reader
                reader["current_term"], reader["current_postings"] = next(reader["iterator"])
            except StopIteration: #else block empty dont crash
                reader["has_next"] = False #no next
                f.close()
            
            if reader["has_next"]:#if next, append to block readers, only blocks not empty
                block_readers.append(reader)

        return block_readers

    def _read_block_terms(self, file_handle): #generator function, continous streaming througt the programs execution!!!
        while True:
            try:
                term_len_bytes = file_handle.read(4)
                if not term_len_bytes:
                    break
                term_len = struct.unpack('I', term_len_bytes)[0]
                term = file_handle.read(term_len).decode('utf-8')

                postings_len_bytes = file_handle.read(4)
                postings_len = struct.unpack('I', postings_len_bytes)[0]
                postings_bytes = file_handle.read(postings_len)
                postings = pickle.loads(postings_bytes)

                yield term, postings #return term posting, but function doesnt end, next time we call next() it will continue and give the next pair
            except (struct.error, EOFError):
                break

    def _merge_with_buffers(self, block_readers: List, output_file: str):
        min_heap = []
        # Initialize heap with the first term from each block reader
        for i, reader in enumerate(block_readers):
            heapq.heappush(min_heap, (reader["current_term"], i))

        with open(output_file, "wb") as f_out:
            while min_heap:
                current_term, block_idx = heapq.heappop(min_heap)
                
                postings_to_merge = [block_readers[block_idx]["current_postings"]]
                
                # Advance the reader for the block we just popped
                try:
                    block_readers[block_idx]["current_term"], block_readers[block_idx]["current_postings"] = next(block_readers[block_idx]["iterator"])
                    heapq.heappush(min_heap, (block_readers[block_idx]["current_term"], block_idx))
                except StopIteration:
                    block_readers[block_idx]["has_next"] = False
                    block_readers[block_idx]["file_handle"].close()

                # Check for other blocks with the same term
                while min_heap and min_heap[0][0] == current_term:
                    _, next_block_idx = heapq.heappop(min_heap)
                    postings_to_merge.append(block_readers[next_block_idx]["current_postings"])
                    
                    # Advance the reader for this block as well
                    try:
                        block_readers[next_block_idx]["current_term"], block_readers[next_block_idx]["current_postings"] = next(block_readers[next_block_idx]["iterator"])
                        heapq.heappush(min_heap, (block_readers[next_block_idx]["current_term"], next_block_idx))
                    except StopIteration:
                        block_readers[next_block_idx]["has_next"] = False
                        block_readers[next_block_idx]["file_handle"].close()

                # Merge postings and write to final index file
                merged_postings = self._merge_postings(postings_to_merge)
                
                term_bytes = current_term.encode('utf-8')
                postings_bytes = pickle.dumps(merged_postings, protocol=pickle.HIGHEST_PROTOCOL)

                f_out.write(struct.pack('I', len(term_bytes)))
                f_out.write(term_bytes)
                f_out.write(struct.pack('I', len(postings_bytes)))
                f_out.write(postings_bytes)

    def _merge_postings(self, postings_lists: List[List[int]]) -> List[int]:
        merged_list = []
        min_heap = []
        
        # Initialize heap with the first element of each list
        for i, p_list in enumerate(postings_lists):
            if p_list:
                heapq.heappush(min_heap, (p_list[0], i, 0)) # (doc_id, list_idx, element_idx)

        while min_heap:
            doc_id, list_idx, element_idx = heapq.heappop(min_heap)

            # Add to merged list, avoiding duplicates
            if not merged_list or merged_list[-1] != doc_id:
                merged_list.append(doc_id)

            # Push the next element from the same list back to the heap
            next_element_idx = element_idx + 1
            if next_element_idx < len(postings_lists[list_idx]):
                next_doc_id = postings_lists[list_idx][next_element_idx]
                heapq.heappush(min_heap, (next_doc_id, list_idx, next_element_idx))
                
        return merged_list

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
