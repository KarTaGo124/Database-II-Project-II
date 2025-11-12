import os
import sys
import shutil

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from indexes.inverted_index.inverted_index_text import InvertedTextIndex

def test_basic_index_and_search():
    pass



if __name__ == "__main__":
    success = test_basic_index_and_search()
    sys.exit(0 if success else 1)
