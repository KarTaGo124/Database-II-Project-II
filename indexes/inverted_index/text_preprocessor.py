from typing import List
import re

from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer
import nltk

try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords', quiet=True)


class TextPreprocessor:

    def __init__(self, language: str = 'spanish'):
        self.language = language
        try:
            self.stopwords = set(stopwords.words(language))
        except Exception as e:
            print(f"Warning: Could not load stopwords for '{language}': {e}")
            print("Attempting to download stopwords...")
            nltk.download('stopwords')
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
        if self.language == 'spanish':
            return re.sub(r'[^a-záéíóúüñ\s]', ' ', text, flags=re.IGNORECASE)

        elif self.language == 'english':
            return re.sub(r'[^a-z\s]', ' ', text, flags=re.IGNORECASE)

        else:
            return ''.join(char if char.isalpha() or char.isspace() else ' ' for char in text)

    def to_lowercase(self, text: str) -> str:
        return text.lower()

    def filter_stopwords(self, tokens: List[str]) -> List[str]:
        return [t for t in tokens if t not in self.stopwords and len(t) > 1]

    def stem_tokens(self, tokens: List[str]) -> List[str]:
        return [self.stemmer.stem(t) for t in tokens]
