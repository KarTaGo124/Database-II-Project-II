import os
import numpy as np
from typing import List

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
        pass

    def _detect_media_type(self, filename: str) -> str:
        pass

    def get_file_path(self, filename: str) -> str:
        pass

    def _get_features_save_path(self, filename: str) -> str:
        pass

    def extract_features(self, filename: str, use_saved: bool = True) -> np.ndarray:
        pass

    def build_codebook(self, filenames: List[str]):
        pass

    def build_histogram(self, filename: str) -> np.ndarray:
        pass

    def calculate_idf(self, all_histograms: dict):
        pass

    def _extract_sift(self, image_path: str) -> np.ndarray:
        pass

    def _extract_mfcc(self, audio_path: str) -> np.ndarray:
        pass

    def _save_codebook(self):
        pass

    def _load_codebook_if_exists(self):
        pass

    def _save_metadata(self):
        pass

    def _load_metadata(self):
        pass
