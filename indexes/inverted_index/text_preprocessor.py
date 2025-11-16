import re
from typing import List
import logging
import nltk
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer

class TextPreprocessor:

    def __init__(self, language: str = 'spanish'):
        self.language = language.lower()
        self._nltk_data()
        self._load_stopwords()
        self._load_stemmer()
        
    def _nltk_data(self):
        try:
            stopwords.fileids()
        except LookupError:
            logging.info("Descargando corpus de stopwords.")
            nltk.download('stopwords', quiet=True)
        
        try:
            available_languages = stopwords.fileids()
            if self.language not in available_languages:
                logging.warning(f"Idioma '{self.language}' no disponible. Idiomas disponibles: {available_languages}")
                logging.warning("Usando 'english' como idioma por defecto")
                self.language = 'english'
        except Exception as e:
            logging.error(f"Error verificando idiomas disponibles: {e}")
            self.language = 'english'
            
    def _load_stopwords(self):
        try:
            self.stopwords = set(stopwords.words(self.language))
        except Exception:
            self.stopwords = set()

    def _load_stemmer(self):
        try:
            if self.language in SnowballStemmer.languages:
                self.stemmer = SnowballStemmer(self.language)
            else:
                logging.warning(f"Stemmer para '{self.language}' no disponible. Usando 'english'")
                self.stemmer = SnowballStemmer('english')
        except Exception as e:
            logging.error(f"Error cargando stemmer: {e}")
            self.stemmer = None

    def preprocess(self, text: str) -> List[str]:
        text = self.to_lowercase(text)
        text =  self.remove_punctuation(text)
        tokens = self.tokenize(text)
        tokens = self.filter_stopwords(tokens)
        tokens = self.stem_tokens(tokens)
        return tokens

    def tokenize(self, text: str) -> List[str]:
        tokens = re.findall(r"\b[\w']+\b", text, flags=re.UNICODE)
        tokens = [t for t in tokens if not t.isnumeric()]
        return tokens
    
    def remove_punctuation(self, text: str) -> str:
        return re.sub(r"[^\w\s']+", ' ', text, flags=re.UNICODE)

    def to_lowercase(self, text: str) -> str:
        return text.lower()

    def filter_stopwords(self, tokens: List[str]) -> List[str]:
        if not self.stopwords:
            return tokens
        return [t for t in tokens if t not in self.stopwords]

    def stem_tokens(self, tokens: List[str]) -> List[str]:
        if not self.stemmer:
            return tokens
        return [self.stemmer.stem(t) for t in tokens]