import os
import numpy as np
import json
import logging
from typing import List, Optional, Dict, Any, Tuple
import cv2
import librosa
from sklearn.cluster import MiniBatchKMeans
from concurrent.futures import ProcessPoolExecutor, as_completed
import pickle
import hashlib


def _extract_features_batch_worker(batch_data):
    
    batch_filenames, files_dir, feature_type, features_dir, cache_size = batch_data
    
    feature_extractors = {
        'SIFT': _extract_sift_global,
        'ORB': _extract_orb_global,
        'HOG': _extract_hog_global,
        'MFCC': _extract_mfcc_global,
        'CHROMA': _extract_chroma_global,
        'SPECTRAL': _extract_spectral_global
    }
    
    batch_descriptors = []
    
    for filename in batch_filenames:
        try:
            file_path = os.path.join(files_dir, filename)
            if not os.path.exists(file_path):
                continue
            
            base_name = os.path.splitext(filename)[0]
            file_hash = hashlib.md5(filename.encode()).hexdigest()[:8]
            feature_file = f"{base_name}_{file_hash}_{feature_type}.npy"
            features_path = os.path.join(features_dir, feature_file)
            
            features = None
            if os.path.exists(features_path):
                try:
                    features = np.load(features_path)
                except Exception:
                    pass
            
            if features is None:
                extractor_func = feature_extractors.get(feature_type)
                if extractor_func:
                    features = extractor_func(file_path)
                    if features is not None and len(features) > 0:
                        os.makedirs(features_dir, exist_ok=True)
                        np.save(features_path, features)
            
            if features is not None and len(features) > 0:
                if len(features) > 1000:
                    indices = np.random.choice(len(features), 1000, replace=False)
                    features = features[indices]
                batch_descriptors.append(features)
                
        except Exception as e:
            logging.error(f"Error procesando {filename}: {e}")
    
    return batch_descriptors


def _extract_sift_global(image_path: str) -> Optional[np.ndarray]:
    try:
        img = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)
        if img is None:
            return None

        if img.shape[0] > 1024 or img.shape[1] > 1024:
            scale = 1024 / max(img.shape)
            new_size = (int(img.shape[1] * scale), int(img.shape[0] * scale))
            img = cv2.resize(img, new_size)

        sift = cv2.SIFT_create(nfeatures=500)
        keypoints, descriptors = sift.detectAndCompute(img, None)

        if descriptors is None or len(descriptors) == 0:
            return None
        return descriptors.astype(np.float32)
    except Exception:
        return None


def _extract_orb_global(image_path: str) -> Optional[np.ndarray]:
    try:
        img = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)
        if img is None:
            return None
        
        orb = cv2.ORB_create(nfeatures=500)
        keypoints, descriptors = orb.detectAndCompute(img, None)
        
        if descriptors is None or len(descriptors) == 0:
            return None
        return descriptors.astype(np.float32)
    except Exception:
        return None


def _extract_hog_global(image_path: str) -> Optional[np.ndarray]:
    try:
        img = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)
        if img is None:
            return None
        
        img = cv2.resize(img, (128, 128))
        hog = cv2.HOGDescriptor()
        descriptors = hog.compute(img)
        
        if descriptors is None or len(descriptors) == 0:
            return None
        return descriptors.reshape(-1, descriptors.shape[0]).astype(np.float32)
    except Exception:
        return None


def _extract_mfcc_global(audio_path: str) -> Optional[np.ndarray]:
    try:
        y, sr = librosa.load(audio_path, sr=22050, duration=30.0)
        if len(y) == 0:
            return None
        
        mfcc = librosa.feature.mfcc(y=y, sr=sr, n_mfcc=13, hop_length=512)
        mfcc = mfcc.T
        
        if mfcc.shape[0] == 0:
            return None
        return mfcc.astype(np.float32)
    except Exception:
        return None


def _extract_chroma_global(audio_path: str) -> Optional[np.ndarray]:
    try:
        y, sr = librosa.load(audio_path, sr=22050, duration=30.0)
        if len(y) == 0:
            return None
        
        chroma = librosa.feature.chroma_stft(y=y, sr=sr, hop_length=512)
        chroma = chroma.T
        
        if chroma.shape[0] == 0:
            return None
        return chroma.astype(np.float32)
    except Exception:
        return None


def _extract_spectral_global(audio_path: str) -> Optional[np.ndarray]:
    try:
        y, sr = librosa.load(audio_path, sr=22050, duration=30.0)
        if len(y) == 0:
            return None
        
        spectral_centroids = librosa.feature.spectral_centroid(y=y, sr=sr)[0]
        spectral_rolloff = librosa.feature.spectral_rolloff(y=y, sr=sr)[0]
        zero_crossing_rate = librosa.feature.zero_crossing_rate(y)[0]
        
        features = np.column_stack([spectral_centroids, spectral_rolloff, zero_crossing_rate])
        
        if features.shape[0] == 0:
            return None
        return features.astype(np.float32)
    except Exception:
        return None


class MultimediaIndexBase:

    FEATURE_EXTRACTORS = {
        'image': {
            'SIFT': '_extract_sift',
            'ORB': '_extract_orb',
            'HOG': '_extract_hog'
        },
        'audio': {
            'MFCC': '_extract_mfcc',
            'CHROMA': '_extract_chroma',
            'SPECTRAL': '_extract_spectral'
        }
    }

    IMAGE_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.bmp', '.gif', '.tiff', '.webp']
    AUDIO_EXTENSIONS = ['.mp3', '.wav', '.ogg', '.flac', '.m4a', '.aac']

    def __init__(self, index_dir: str, files_dir: str, field_name: str,
                 feature_type: str, n_clusters: int = None, cache_size: int = 1000, filename_pattern: str = None):
        self.index_dir = index_dir
        self.files_dir = files_dir
        self.field_name = field_name
        self.feature_type = feature_type
        self.n_clusters = n_clusters
        self.cache_size = cache_size
        self.filename_pattern = filename_pattern

        os.makedirs(index_dir, exist_ok=True)
        self.features_dir = os.path.join(index_dir, "features")
        os.makedirs(self.features_dir, exist_ok=True)

        self.codebook_file = os.path.join(index_dir, f"codebook_{feature_type}.npy")
        self.metadata_file = os.path.join(index_dir, f"metadata_{feature_type}.json")
        self.idf_file = os.path.join(index_dir, f"idf_{feature_type}.pkl")

        self.codebook = None
        self.idf = {}
        self._feature_cache = {}

        self._validate_feature_type()
        self._load_if_exists()

    def _validate_feature_type(self):
        media_type = None
        for mtype, extractors in self.FEATURE_EXTRACTORS.items():
            if self.feature_type in extractors:
                media_type = mtype
                break
        if media_type is None:
            available = [f for extractors in self.FEATURE_EXTRACTORS.values() for f in extractors]
            raise ValueError(f"Feature type '{self.feature_type}' not supported. Available: {available}")

    def _detect_media_type(self, filename: str) -> str:
        ext = os.path.splitext(filename)[1].lower()
        if ext in self.IMAGE_EXTENSIONS:
            return 'image'
        elif ext in self.AUDIO_EXTENSIONS:
            return 'audio'
        else:
            raise ValueError(f"Unknown media type for extension: {ext}")

    def get_file_path(self, filename: str) -> str:
        if os.path.isabs(filename):
            return filename
        return os.path.join(self.files_dir, filename)

    def resolve_filename(self, record) -> str:
        if self.filename_pattern:
            key_value = record.get_key()
            resolved = self.filename_pattern.replace("{id}", str(key_value))
            return resolved
        else:
            return getattr(record, self.field_name, None)

    def _get_features_save_path(self, filename: str) -> str:
        base_name = os.path.basename(os.path.splitext(filename)[0])
        file_hash = hashlib.md5(filename.encode()).hexdigest()[:8]
        feature_file = f"{base_name}_{file_hash}_{self.feature_type}.npy"
        return os.path.join(self.features_dir, feature_file)

    def extract_features(self, filename: str, use_saved: bool = True) -> Optional[np.ndarray]:
        if filename in self._feature_cache:
            return self._feature_cache[filename]

        features_path = self._get_features_save_path(filename)
        
        if use_saved and os.path.exists(features_path):
            try:
                features = np.load(features_path)
                if len(self._feature_cache) < self.cache_size:
                    self._feature_cache[filename] = features
                return features
            except Exception as e:
                logging.warning(f"Error loading cached features for {filename}: {e}")
        
        file_path = self.get_file_path(filename)
        if not os.path.exists(file_path):
            logging.debug(f"File not found: {file_path}")
            return None
        
        try:
            media_type = self._detect_media_type(filename)
            extractor_name = self.FEATURE_EXTRACTORS[media_type][self.feature_type]
            extractor_method = getattr(self, extractor_name)
            features = extractor_method(file_path)
            
            if features is not None and len(features) > 0:
                np.save(features_path, features)
                if len(self._feature_cache) < self.cache_size:
                    self._feature_cache[filename] = features
                return features
        except Exception as e:
            logging.error(f"Error extracting features from {filename}: {e}")
        
        return None

    def build_codebook(self, filenames: List[str], n_workers: int = None, batch_size: int = 100):

        if n_workers is None:
            n_workers = min(4, os.cpu_count() or 1)

        if self.n_clusters is None:
            self.n_clusters = 300
            logging.info(f"Using default n_clusters={self.n_clusters}")

        logging.info(f"Construyendo codebook con {len(filenames)} archivos usando {n_workers} workers")
        logging.info(f"Tamaño de batch: {batch_size}")

        batches = [filenames[i:i + batch_size] for i in range(0, len(filenames), batch_size)]
        logging.info(f"Total de batches: {len(batches)}")
        
        batch_data_list = [
            (batch, self.files_dir, self.feature_type, self.features_dir, self.cache_size)
            for batch in batches
        ]
        
        all_descriptors = []
        
        with ProcessPoolExecutor(max_workers=n_workers) as executor:
            future_to_batch = {
                executor.submit(_extract_features_batch_worker, batch_data): i
                for i, batch_data in enumerate(batch_data_list)
            }
            
            for future in as_completed(future_to_batch):
                batch_idx = future_to_batch[future]
                try:
                    batch_descriptors = future.result()
                    all_descriptors.extend(batch_descriptors)
                    logging.info(f"Batch {batch_idx + 1}/{len(batches)} completado: {len(batch_descriptors)} archivos procesados")
                except Exception as e:
                    logging.error(f"Error en batch {batch_idx + 1}: {e}")
        
        if len(all_descriptors) == 0:
            raise ValueError("No se pudieron extraer descriptores de ningún archivo")

        logging.info(f"Combinando {len(all_descriptors)} conjuntos de descriptores...")
        combined_descriptors = np.vstack(all_descriptors)
        logging.info(f"Total descriptores combinados: {len(combined_descriptors)}")

        max_descriptors = 200000

        if len(combined_descriptors) > max_descriptors:
            logging.info(f"Submuestreando de {len(combined_descriptors)} a {max_descriptors} descriptores")
            indices = np.random.choice(len(combined_descriptors), max_descriptors, replace=False)
            combined_descriptors = combined_descriptors[indices]
        else:
            logging.info(f"Usando todos los {len(combined_descriptors)} descriptores (bajo el límite de {max_descriptors})")

        logging.info(f"Entrenando codebook con {self.n_clusters} clusters...")
        kmeans = MiniBatchKMeans(
            n_clusters=self.n_clusters,
            random_state=42,
            batch_size=min(2000, len(combined_descriptors) // 10),
            n_init=3,
            max_iter=100
        )
        kmeans.fit(combined_descriptors)

        self.codebook = kmeans.cluster_centers_
        logging.info(f"Codebook construido exitosamente: {self.codebook.shape}")
        
        self._save_codebook()
        self._save_metadata()

    def build_histogram(self, filename: str, normalize: bool = True) -> Optional[np.ndarray]:
        if self.codebook is None:
            raise ValueError("Codebook not built yet. Call build_codebook first.")

        features = self.extract_features(filename)
        if features is None or len(features) == 0:
            return None

        histogram = np.zeros(self.n_clusters, dtype=np.float32)

        distances = np.linalg.norm(self.codebook[np.newaxis, :, :] - features[:, np.newaxis, :], axis=2)
        closest_codewords = np.argmin(distances, axis=1)
        
        unique, counts = np.unique(closest_codewords, return_counts=True)
        histogram[unique] = counts

        if normalize and histogram.sum() > 0:
            histogram = histogram / histogram.sum()

        return histogram
    
    def calculate_idf(self, all_histograms: Dict[str, np.ndarray]):
        n_docs = len(all_histograms)
        if n_docs == 0:
            return

        doc_freq = np.zeros(self.n_clusters)

        for histogram in all_histograms.values():
            doc_freq += (histogram > 0).astype(int)

        self.idf = {}
        for i in range(self.n_clusters):
            if doc_freq[i] > 0:
                self.idf[i] = np.log(n_docs / doc_freq[i])
            else:
                self.idf[i] = 0.0

        self._save_idf()

    def get_tf_idf_vector(self, filename: str) -> Optional[np.ndarray]:
        histogram = self.build_histogram(filename)
        if histogram is None or len(self.idf) == 0:
            return None

        tf_idf = np.zeros(self.n_clusters, dtype=np.float32)
        for i in range(self.n_clusters):
            tf_idf[i] = histogram[i] * self.idf.get(i, 0.0)

        return tf_idf

    def _extract_sift(self, image_path: str) -> Optional[np.ndarray]:
        try:
            img = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)
            if img is None:
                return None
            
            if img.shape[0] > 1024 or img.shape[1] > 1024:
                scale = 1024 / max(img.shape)
                new_size = (int(img.shape[1] * scale), int(img.shape[0] * scale))
                img = cv2.resize(img, new_size)

            sift = cv2.SIFT_create(nfeatures=500)
            keypoints, descriptors = sift.detectAndCompute(img, None)
            
            if descriptors is None or len(descriptors) == 0:
                return None
            return descriptors.astype(np.float32)
        except Exception:
            return None

    def _extract_orb(self, image_path: str) -> Optional[np.ndarray]:
        try:
            img = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)
            if img is None:
                return None
            
            orb = cv2.ORB_create(nfeatures=500)
            keypoints, descriptors = orb.detectAndCompute(img, None)
            
            if descriptors is None or len(descriptors) == 0:
                return None
            return descriptors.astype(np.float32)
        except Exception:
            return None

    def _extract_hog(self, image_path: str) -> Optional[np.ndarray]:
        try:
            img = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)
            if img is None:
                return None
            
            img = cv2.resize(img, (128, 128))
            hog = cv2.HOGDescriptor()
            descriptors = hog.compute(img)
            
            if descriptors is None or len(descriptors) == 0:
                return None
            return descriptors.reshape(-1, descriptors.shape[0]).astype(np.float32)
        except Exception:
            return None

    def _extract_mfcc(self, audio_path: str) -> Optional[np.ndarray]:
        try:
            y, sr = librosa.load(audio_path, sr=22050, duration=30.0)
            if len(y) == 0:
                return None
            
            mfcc = librosa.feature.mfcc(y=y, sr=sr, n_mfcc=13, hop_length=512)
            mfcc = mfcc.T
            
            if mfcc.shape[0] == 0:
                return None
            return mfcc.astype(np.float32)
        except Exception:
            return None

    def _extract_chroma(self, audio_path: str) -> Optional[np.ndarray]:
        try:
            y, sr = librosa.load(audio_path, sr=22050, duration=30.0)
            if len(y) == 0:
                return None
            
            chroma = librosa.feature.chroma_stft(y=y, sr=sr, hop_length=512)
            chroma = chroma.T
            
            if chroma.shape[0] == 0:
                return None
            return chroma.astype(np.float32)
        except Exception:
            return None

    def _extract_spectral(self, audio_path: str) -> Optional[np.ndarray]:
        try:
            y, sr = librosa.load(audio_path, sr=22050, duration=30.0)
            if len(y) == 0:
                return None
            
            spectral_centroids = librosa.feature.spectral_centroid(y=y, sr=sr)[0]
            spectral_rolloff = librosa.feature.spectral_rolloff(y=y, sr=sr)[0]
            zero_crossing_rate = librosa.feature.zero_crossing_rate(y)[0]
            
            features = np.column_stack([spectral_centroids, spectral_rolloff, zero_crossing_rate])
            
            if features.shape[0] == 0:
                return None
            return features.astype(np.float32)
        except Exception:
            return None

    def _save_codebook(self):
        if self.codebook is not None:
            np.save(self.codebook_file, self.codebook)

    def _save_idf(self):
        with open(self.idf_file, 'wb') as f:
            pickle.dump(self.idf, f)

    def _load_if_exists(self):
        if os.path.exists(self.codebook_file):
            try:
                self.codebook = np.load(self.codebook_file)
                self._load_metadata()
                if os.path.exists(self.idf_file):
                    with open(self.idf_file, 'rb') as f:
                        self.idf = pickle.load(f)
            except Exception as e:
                logging.error(f"Error loading existing data: {e}")

    def _save_metadata(self):
        metadata = {
            'n_clusters': self.n_clusters,
            'feature_type': self.feature_type,
            'field_name': self.field_name,
            'codebook_shape': self.codebook.shape if self.codebook is not None else None,
            'cache_size': self.cache_size
        }
        with open(self.metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)

    def _load_metadata(self):
        if os.path.exists(self.metadata_file):
            try:
                with open(self.metadata_file, 'r') as f:
                    metadata = json.load(f)
                self.n_clusters = metadata.get('n_clusters', self.n_clusters)
                self.feature_type = metadata.get('feature_type', self.feature_type)
                self.field_name = metadata.get('field_name', self.field_name)
                self.cache_size = metadata.get('cache_size', self.cache_size)
            except Exception as e:
                logging.error(f"Error loading metadata: {e}")

    def get_statistics(self) -> Dict[str, Any]:
        return {
            'n_clusters': self.n_clusters,
            'feature_type': self.feature_type,
            'codebook_built': self.codebook is not None,
            'codebook_size_mb': os.path.getsize(self.codebook_file) / (1024**2) if os.path.exists(self.codebook_file) else 0,
            'idf_calculated': len(self.idf) > 0,
            'cache_size': len(self._feature_cache),
            'features_cached_count': len(self._feature_cache)
        }
