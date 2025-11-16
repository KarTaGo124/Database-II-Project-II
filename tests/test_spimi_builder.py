import unittest
import tempfile
import os
import shutil
from unittest.mock import Mock
import sys
import pickle
import struct
import logging
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from indexes.inverted_index.spimi_builder import SPIMIBuilder

class TestSPIMIBuilder(unittest.TestCase):
    
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.output_file = os.path.join(self.temp_dir, "test_index.dat")
        self.spimi = SPIMIBuilder(
            block_size_mb=1,  # Bloque pequeño para testing
            temp_dir=self.temp_dir,
            max_buffers=3  # Pocos buffers para testing
        )
        
    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_init_with_default_values(self):
        spimi = SPIMIBuilder()
        self.assertGreater(spimi.block_size_mb, 0)
        self.assertGreater(spimi.max_buffers, 0)
        self.assertEqual(spimi.block_counter, 0)
        self.assertEqual(spimi.merge_pass_counter, 0)
    
    def test_init_with_custom_values(self):
        spimi = SPIMIBuilder(block_size_mb=100, max_buffers=20)
        self.assertEqual(spimi.block_size_mb, 100)
        self.assertEqual(spimi.max_buffers, 20)
    
    def test_memory_usage(self):
        memory_info = self.spimi.get_memory_usage()
        self.assertIn('total_mb', memory_info)
        self.assertIn('available_mb', memory_info)
        self.assertIn('used_mb', memory_info)
        self.assertIn('percent', memory_info)
        self.assertGreater(memory_info['total_mb'], 0)
    
    def create_mock_documents(self, num_docs=10):
        documents = []
        for i in range(num_docs):
            doc = Mock()
            doc.content = f"Este es el documento número {i}. Contiene texto de prueba para indexar."
            documents.append((i, doc))
        return documents
    
    def create_dict_documents(self, num_docs=10):
        documents = []
        for i in range(num_docs):
            doc = {
                'content': f"documento {i} texto prueba indexar contenido único {i % 3}"
            }
            documents.append((i, doc))
        return documents
    
    def test_process_documents_small_dataset(self):
        documents = self.create_mock_documents(5)
        
        block_files = self.spimi._process_documents_in_blocks(iter(documents), 'content')
        
        self.assertGreater(len(block_files), 0)
        for bf in block_files:
            self.assertTrue(os.path.exists(bf))
    
    def test_process_documents_dict_format(self):
        documents = self.create_dict_documents(5)
        
        block_files = self.spimi._process_documents_in_blocks(iter(documents), 'content')
        
        self.assertGreater(len(block_files), 0)
        for bf in block_files:
            self.assertTrue(os.path.exists(bf))
    
    def test_build_index_complete_process(self):
        documents = self.create_dict_documents(10)
        
        result_file = self.spimi.build_index(iter(documents), 'content', self.output_file)
        
        self.assertEqual(result_file, self.output_file)
        self.assertTrue(os.path.exists(self.output_file))
        self.assertGreater(os.path.getsize(self.output_file), 0)
    
    def test_read_index_file(self):
        documents = self.create_dict_documents(5)
        
        self.spimi.build_index(iter(documents), 'content', self.output_file)
        
        terms_found = set()
        with open(self.output_file, 'rb') as f:
            try:
                while True:
                    term_len_bytes = f.read(4)
                    if not term_len_bytes:
                        break
                    term_len = struct.unpack('I', term_len_bytes)[0]
                    
                    term = f.read(term_len).decode('utf-8')
                    terms_found.add(term)
                    
                    postings_len_bytes = f.read(4)
                    postings_len = struct.unpack('I', postings_len_bytes)[0]
                    
                    postings_bytes = f.read(postings_len)
                    postings = pickle.loads(postings_bytes)
                    
                    self.assertIsInstance(postings, list)
                    for doc_id, tf in postings:
                        self.assertIsInstance(doc_id, int)
                        self.assertIsInstance(tf, int)
                        self.assertGreater(tf, 0)
                        
            except struct.error:
                pass 
        self.assertGreater(len(terms_found), 0)
        print(f"Términos encontrados en el índice: {sorted(list(terms_found))}")
    
    def test_merge_postings(self):
        postings1 = [(1, 2), (3, 1)]
        postings2 = [(1, 1), (2, 3)]
        postings3 = [(3, 2), (4, 1)]
        
        result = self.spimi._merge_postings([postings1, postings2, postings3])
        
        expected = [(1, 3), (2, 3), (3, 3), (4, 1)]  
        self.assertEqual(result, expected)
    
    def test_cleanup_temp_files(self):
    
        test_files = []
        for i in range(3):
            test_file = os.path.join(self.temp_dir, f"test_{i}.dat")
            with open(test_file, 'w') as f:
                f.write("test")
            test_files.append(test_file)
        
        for tf in test_files:
            self.assertTrue(os.path.exists(tf))
        
        self.spimi._cleanup_temp_files()
        
        for tf in test_files:
            self.assertFalse(os.path.exists(tf))
    
    def test_empty_documents(self):
        documents = []
        
        result_file = self.spimi.build_index(iter(documents), 'content', self.output_file)
        
        self.assertIsNone(result_file)
    
    def test_documents_without_field(self):
        documents = []
        for i in range(3):
            doc = {'other_field': f'content {i}'}
            documents.append((i, doc))
        
        result_file = self.spimi.build_index(iter(documents), 'nonexistent_field', self.output_file)
        
        self.assertIsNone(result_file)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    
    # Ejecutar tests
    unittest.main(verbosity=2)
