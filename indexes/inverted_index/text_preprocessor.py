import re
from typing import List

class TextPreprocessor:

    def __init__(self, language: str = 'spanish'):
        self.language = language
        self.stopwords = set()
        self.stemmer = None
        self._load_stopwords()
        self._load_stemmer()

    def _load_stopwords(self):
        pass

    def _load_stemmer(self):
        pass

    def preprocess(self, text: str) -> List[str]:
        pass

    def tokenize(self, text: str) -> List[str]:
        pass

    def remove_punctuation(self, text: str) -> str:
        pass

    def to_lowercase(self, text: str) -> str:
        pass

    def filter_stopwords(self, tokens: List[str]) -> List[str]:
        pass

    def stem_tokens(self, tokens: List[str]) -> List[str]:
        pass
