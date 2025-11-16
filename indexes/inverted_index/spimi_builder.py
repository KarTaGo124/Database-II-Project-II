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
