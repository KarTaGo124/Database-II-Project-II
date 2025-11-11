import os
import numpy as np
import json
from typing import List
import cv2
import librosa
from sklearn.cluster import KMeans
from multiprocessing import Pool

class MultimediaIndexBase:

    FEATURE_EXTRACTORS = {
        'image': {
            'SIFT': '_extract_sift',
        },
        'audio': {
            'MFCC': '_extract_mfcc',
        }
    }

    IMAGE_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.bmp', '.gif']
    AUDIO_EXTENSIONS = ['.mp3', '.wav', '.ogg', '.flac', '.m4a']

    def __init__(self, index_dir: str, files_dir: str, field_name: str,
                 feature_type: str, n_clusters: int = 100):
        self.index_dir = index_dir
        self.files_dir = files_dir
        self.field_name = field_name
        self.feature_type = feature_type
        self.n_clusters = n_clusters

        self.features_dir = files_dir.replace("_files", "_features")
        os.makedirs(self.features_dir, exist_ok=True)

        self.codebook_file = os.path.join(index_dir, "codebook.npy")
        self.metadata_file = os.path.join(index_dir, "metadata.json")

        self.codebook = None
        self.idf = {}

        self._validate_feature_type()
        self._load_codebook_if_exists()

    def _validate_feature_type(self):
        media_type = None
        for mtype, extractors in self.FEATURE_EXTRACTORS.items():
            if self.feature_type in extractors:
                media_type = mtype
                break
        if media_type is None:
            raise ValueError(f"Feature type '{self.feature_type}' not supported")

    def _detect_media_type(self, filename: str) -> str:
        ext = os.path.splitext(filename)[1].lower()
        if ext in self.IMAGE_EXTENSIONS:
            return 'image'
        elif ext in self.AUDIO_EXTENSIONS:
            return 'audio'
        else:
            raise ValueError(f"Unknown media type for extension: {ext}")

    def get_file_path(self, filename: str) -> str:
        return os.path.join(self.files_dir, filename)

    def _get_features_save_path(self, filename: str) -> str:
        base_name = os.path.splitext(filename)[0]
        feature_file = f"{base_name}_{self.feature_type}.npy"
        return os.path.join(self.features_dir, feature_file)

    def extract_features(self, filename: str, use_saved: bool = True) -> np.ndarray:
        features_path = self._get_features_save_path(filename)
        
        if use_saved and os.path.exists(features_path):
            return np.load(features_path)
        
        file_path = self.get_file_path(filename)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        media_type = self._detect_media_type(filename)
        extractor_name = self.FEATURE_EXTRACTORS[media_type][self.feature_type]
        extractor_method = getattr(self, extractor_name)
        features = extractor_method(file_path)
        
        np.save(features_path, features)
        return features

    def build_codebook(self, filenames: List[str], n_workers: int = 4):
        def extract_one(filename):
            try:
                return self.extract_features(filename)
            except Exception as e:
                print(f"Error extracting features from {filename}: {e}")
                return None

        with Pool(processes=n_workers) as pool:
            all_descriptors = pool.map(extract_one, filenames)

        all_descriptors = [d for d in all_descriptors if d is not None and len(d) > 0]

        if len(all_descriptors) == 0:
            raise ValueError("No se pudieron extraer descriptores de ningún archivo")

        all_descriptors = np.vstack(all_descriptors)

        kmeans = KMeans(n_clusters=self.n_clusters, random_state=42, n_init=10)
        kmeans.fit(all_descriptors)

        self.codebook = kmeans.cluster_centers_
        self._save_codebook()
        self._save_metadata()


    def build_histogram(self, filename: str) -> np.ndarray:
        if self.codebook is None:
            raise ValueError("Codebook not built yet. Call build_codebook first.")

        features = self.extract_features(filename)

        histogram = np.zeros(self.n_clusters, dtype=np.float32)

        for descriptor in features:
            distances = np.linalg.norm(self.codebook - descriptor, axis=1)
            closest_codeword = np.argmin(distances)
            histogram[closest_codeword] += 1

        return histogram
    
    def calculate_idf(self, all_histograms: dict):
        n_docs = len(all_histograms)
        doc_freq = np.zeros(self.n_clusters)

        for histogram in all_histograms.values():
            doc_freq += (histogram > 0).astype(int)

        self.idf = {}
        for i in range(self.n_clusters):
            if doc_freq[i] > 0:
                self.idf[i] = np.log(n_docs / doc_freq[i])
            else:
                self.idf[i] = 0

    def _extract_sift(self, image_path: str) -> np.ndarray:
        img = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)
        if img is None:
            raise ValueError(f"Cannot read image: {image_path}")
        sift = cv2.SIFT_create()
        keypoints, descriptors = sift.detectAndCompute(img, None)
        if descriptors is None or len(descriptors) == 0:
            raise ValueError(f"No SIFT features detected in: {image_path}")
        return descriptors.astype(np.float32)

    def _extract_mfcc(self, audio_path: str) -> np.ndarray:
        y, sr = librosa.load(audio_path, sr=None)
        if len(y) == 0:
            raise ValueError(f"Cannot read audio: {audio_path}")
        mfcc = librosa.feature.mfcc(y=y, sr=sr, n_mfcc=13)
        mfcc = mfcc.T
        if mfcc.shape[0] == 0:
            raise ValueError(f"No MFCC features extracted from: {audio_path}")
        return mfcc.astype(np.float32)

    def _save_codebook(self):
        if self.codebook is not None:
            np.save(self.codebook_file, self.codebook)

    def _load_codebook_if_exists(self):
        if os.path.exists(self.codebook_file):
            self.codebook = np.load(self.codebook_file)
            self._load_metadata()

    def _save_metadata(self):
        metadata = {
            'n_clusters': self.n_clusters,
            'feature_type': self.feature_type,
            'field_name': self.field_name
        }
        with open(self.metadata_file, 'w') as f:
            json.dump(metadata, f)

    def _load_metadata(self):
        if os.path.exists(self.metadata_file):
            with open(self.metadata_file, 'r') as f:
                metadata = json.load(f)
            self.n_clusters = metadata.get('n_clusters', self.n_clusters)
            self.feature_type = metadata.get('feature_type', self.feature_type)
            self.field_name = metadata.get('field_name', self.field_name)
