import unittest
import sys
import os
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from indexes.inverted_index.text_preprocessor import TextPreprocessor


class TestTextPreprocessor(unittest.TestCase):
    
    def setUp(self):
        logging.basicConfig(level=logging.INFO)
        
    def test_spanish_initialization(self):
        preprocessor = TextPreprocessor(language='spanish')
        self.assertEqual(preprocessor.language, 'spanish')
        self.assertIsNotNone(preprocessor.stopwords)
        self.assertIsNotNone(preprocessor.stemmer)
        
    def test_english_initialization(self):
        preprocessor = TextPreprocessor(language='english')
        self.assertEqual(preprocessor.language, 'english')
        self.assertIsNotNone(preprocessor.stopwords)
        self.assertIsNotNone(preprocessor.stemmer)
        
    def test_invalid_language_fallback(self):
        preprocessor = TextPreprocessor(language='klingon')
        self.assertEqual(preprocessor.language, 'english')
        
    def test_case_normalization(self):
        preprocessor = TextPreprocessor(language='SPANISH')
        self.assertEqual(preprocessor.language, 'spanish')
        
    def test_basic_preprocessing_spanish(self):
        preprocessor = TextPreprocessor(language='spanish')
        text = "¡Hola mundo! Este es un texto de prueba con números 123."
        tokens = preprocessor.preprocess(text)
        
        self.assertIsInstance(tokens, list)
        self.assertGreater(len(tokens), 0)
        
        for token in tokens:
            self.assertFalse(token.isnumeric())
            
        for token in tokens:
            self.assertEqual(token, token.lower())
            
    def test_basic_preprocessing_english(self):
        preprocessor = TextPreprocessor(language='english')
        text = "Hello World! This is a test text with numbers 456."
        tokens = preprocessor.preprocess(text)
        
        self.assertIsInstance(tokens, list)
        self.assertGreater(len(tokens), 0)
        
        for token in tokens:
            self.assertFalse(token.isnumeric())
            
    def test_tokenization(self):
        preprocessor = TextPreprocessor()
        text = "palabra1, palabra2! palabra3? números123"
        tokens = preprocessor.tokenize(text)
        
        self.assertGreater(len(tokens), 0)
        
        non_numeric_tokens = [t for t in tokens if not t.isnumeric()]
        self.assertEqual(len(non_numeric_tokens), 4) 
        
    def test_remove_punctuation(self):
        preprocessor = TextPreprocessor()
        text = "¡Hola, mundo! ¿Cómo estás?"
        result = preprocessor.remove_punctuation(text)
        
        self.assertNotIn('!', result)
        self.assertNotIn(',', result)
        self.assertNotIn('?', result)
        
    def test_lowercase_conversion(self):
        preprocessor = TextPreprocessor()
        text = "TEXTO EN MAYÚSCULAS"
        result = preprocessor.to_lowercase(text)
        self.assertEqual(result, text.lower())
        
    def test_stopwords_filtering_spanish(self):
        preprocessor = TextPreprocessor(language='spanish')
        tokens = ['el', 'gato', 'está', 'en', 'la', 'casa']
        filtered = preprocessor.filter_stopwords(tokens)
        
        self.assertIn('gato', filtered)
        self.assertIn('casa', filtered)
        
    def test_stopwords_filtering_english(self):
        preprocessor = TextPreprocessor(language='english')
        tokens = ['the', 'cat', 'is', 'on', 'the', 'house']
        filtered = preprocessor.filter_stopwords(tokens)
        
        self.assertIn('cat', filtered)
        self.assertIn('house', filtered)
        
    def test_stemming_spanish(self):
        preprocessor = TextPreprocessor(language='spanish')
        tokens = ['corriendo', 'corrió', 'correr']
        stemmed = preprocessor.stem_tokens(tokens)
        
        self.assertTrue(all(isinstance(token, str) for token in stemmed))
        self.assertEqual(len(stemmed), 3)
        
    def test_stemming_english(self):
        preprocessor = TextPreprocessor(language='english')
        tokens = ['running', 'ran', 'run']
        stemmed = preprocessor.stem_tokens(tokens)
        
        self.assertTrue(all(isinstance(token, str) for token in stemmed))
        self.assertEqual(len(stemmed), 3)
        
    def test_empty_text(self):
        preprocessor = TextPreprocessor()
        result = preprocessor.preprocess("")
        self.assertEqual(result, [])
        
    def test_only_punctuation(self):
        preprocessor = TextPreprocessor()
        result = preprocessor.preprocess("!@#$%^&*()")
        self.assertEqual(result, [])
        
    def test_only_numbers(self):
        preprocessor = TextPreprocessor()
        result = preprocessor.preprocess("123 456 789")
        self.assertEqual(result, [])
        
    def test_mixed_content(self):
        preprocessor = TextPreprocessor(language='spanish')
        text = """
        ¡Este es un texto de PRUEBA muy completo! 
        Incluye números como 123, signos de puntuación ¿verdad?, 
        y palabras en diferentes casos: MAYÚSCULAS, minúsculas, MiXtAs.
        También tiene acentos: café, niño, corazón.
        """
        tokens = preprocessor.preprocess(text)
        
        self.assertIsInstance(tokens, list)
        self.assertGreater(len(tokens), 0)
        
        for token in tokens:
            self.assertIsInstance(token, str)
            self.assertGreater(len(token), 0)
            
    def test_unicode_support(self):
        preprocessor = TextPreprocessor(language='spanish')
        text = "café niño corazón águila"
        tokens = preprocessor.preprocess(text)
        
        self.assertGreater(len(tokens), 0)
        

def run_comprehensive_test():
    print("PRUEBA")    
    languages = ['spanish', 'english', 'french', 'german', 'invalid_language']
    
    for lang in languages:
        print(f" Probando idioma: {lang}")
        try:
            preprocessor = TextPreprocessor(language=lang)
            print(f"Idioma configurado: {preprocessor.language}")
            print(f"Stopwords cargadas: {len(preprocessor.stopwords)}")
            print(f"Stemmer disponible: {preprocessor.stemmer is not None}")
            
            if lang in ['spanish', 'invalid_language']:
                test_text = "¡Hola mundo! Este es un texto de prueba."
            else:
                test_text = "Hello world! This is a test text."
                
            result = preprocessor.preprocess(test_text)
            print(f"Texto original: {test_text}")
            print(f"Tokens procesados: {result}")
            print()
            
        except Exception as e:
            print(f"Error con idioma {lang}: {e}")
            print()


if __name__ == '__main__':
    print("TESTS")
    
    run_comprehensive_test()

    unittest.main(verbosity=2)
