import re
from typing import List

from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer
import nltk 

nltk.download('stopwords', quiet=True)


class TextPreprocessor:

    def __init__(self, language: str = 'spanish'):
        self.language = language
        self.stopwords = set(stopwords.words(language))
        self.stemmer = SnowballStemmer(language)

    def preprocess(self, text: str) -> List[str]:
        text = self.to_lowercase(text)
        text = self.remove_punctuation(text)
        tokens = self.tokenize(text)
        tokens = self.filter_stopwords(tokens)
        tokens = self.stem_tokens(tokens)
        return tokens

    def tokenize(self, text: str) -> List[str]:
        return text.split()

    def remove_punctuation(self, text: str) -> str:
        return re.sub(r'[^a-záéíóúüñ ]', '', text)

    def to_lowercase(self, text: str) -> str:
        return text.lower()

    def filter_stopwords(self, tokens: List[str]) -> List[str]:
        return [t for t in tokens if t not in self.stopwords and len(t) > 1]

    def stem_tokens(self, tokens: List[str]) -> List[str]:
        return [self.stemmer.stem(t) for t in tokens]
