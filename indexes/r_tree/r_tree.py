import math
import os
from rtree import index
from typing import List, Optional
from ..core.record import IndexRecord
from ..core.performance_tracker import PerformanceTracker, OperationResult

class RTreeSecondaryIndex:
    def __init__(self, field_name: str, filename: str, dimension: int = 2):
        self.field_name = field_name
        self.filename = filename
        self.dimension = dimension
        self.performance = PerformanceTracker()

        p = index.Property()
        p.dimension = dimension
        if filename:
            self.idx = index.Index(filename, properties=p)
        else:
            self.idx = index.Index(properties=p)
    
    def insert(self, index_record: IndexRecord) -> OperationResult:
        self.performance.start_operation()

        try:
            coords = index_record.index_value

            if coords is None:
                raise ValueError(f"Campo {self.field_name} no encontrado en el index_record")

            if not isinstance(coords, (list, tuple)):
                raise ValueError(f"Campo {self.field_name} debe ser lista o tupla de coordenadas")

            if len(coords) != self.dimension:
                raise ValueError(f"Campo {self.field_name} debe tener {self.dimension} dimensiones")

            coords = list(coords)
            primary_key = index_record.primary_key

            bbox = tuple(coords + coords)
            self.idx.insert(primary_key, bbox)
            self.performance.track_write()

            return self.performance.end_operation(True)

        except Exception:
            return self.performance.end_operation(False)
    
    def search(self, value) -> OperationResult:
        self.performance.start_operation()

        try:
            if not isinstance(value, (list, tuple)):
                raise ValueError(f"Valor de búsqueda debe ser lista o tupla de coordenadas")

            if len(value) != self.dimension:
                raise ValueError(f"Valor de búsqueda debe tener {self.dimension} dimensiones")

            bbox = tuple(list(value) + list(value))
            candidate_ids = list(self.idx.intersection(bbox))
            self.performance.track_read()

            return self.performance.end_operation(candidate_ids)

        except Exception:
            return self.performance.end_operation([])
    
    def range_search(self, coords, param2, spatial_type: str) -> OperationResult:
        if spatial_type == "radius":
            return self.radius_search(coords, param2)
        elif spatial_type == "knn":
            return self.knn_search(coords, param2)
        else:
            raise NotImplementedError(
                "Range search is not supported for R-Tree spatial indexes. "
                "R-Tree is optimized for spatial queries. "
                "Use spatial_type='radius' or spatial_type='knn'."
            )
    
    def knn_search(self, coords: List[float], k: int) -> OperationResult:
        self.performance.start_operation()

        try:
            if not isinstance(coords, (list, tuple)) or len(coords) != self.dimension:
                raise ValueError(f"Coordenadas deben tener {self.dimension} dimensiones")

            if k <= 0:
                raise ValueError("k debe ser mayor que 0")

            bbox = tuple(list(coords) + list(coords))
            nearest_pks = list(self.idx.nearest(bbox, k))
            self.performance.track_read()

            return self.performance.end_operation(nearest_pks)

        except Exception:
            return self.performance.end_operation([])
    
    def radius_search(self, coords: List[float], radius: float) -> OperationResult:
        self.performance.start_operation()

        try:
            if not isinstance(coords, (list, tuple)) or len(coords) != self.dimension:
                raise ValueError(f"Centro debe tener {self.dimension} dimensiones")

            if radius < 0:
                raise ValueError("Radio debe ser mayor o igual a 0")

            min_coords = [c - radius for c in coords]
            max_coords = [c + radius for c in coords]
            bbox = tuple(min_coords + max_coords)

            candidate_pks = list(self.idx.intersection(bbox))
            self.performance.track_read()

            return self.performance.end_operation(candidate_pks)

        except Exception:
            return self.performance.end_operation([])
    
    def delete(self, coords, primary_key=None) -> OperationResult:
        self.performance.start_operation()

        try:
            if not isinstance(coords, (list, tuple)) or len(coords) != self.dimension:
                raise ValueError(f"Coordenadas deben tener {self.dimension} dimensiones")

            bbox = tuple(list(coords) + list(coords))

            if primary_key is not None:
                self.idx.delete(primary_key, bbox)
                self.performance.track_write()
                return self.performance.end_operation(True)
            else:
                candidate_ids = list(self.idx.intersection(bbox))
                self.performance.track_read()
                deleted_pks = []

                for pk in candidate_ids:
                    try:
                        self.idx.delete(pk, bbox)
                        deleted_pks.append(pk)
                        self.performance.track_write()
                    except Exception:
                        continue

                return self.performance.end_operation(deleted_pks)

        except Exception:
            return self.performance.end_operation(False if primary_key is not None else [])
    
    def _euclidean_distance(self, p1: List[float], p2: List[float]) -> float:
        if len(p1) != len(p2):
            raise ValueError("Puntos deben tener la misma dimensión")
        return math.sqrt(sum((p1[i] - p2[i]) ** 2 for i in range(len(p1))))
    
    def drop_index(self):
        removed_files = []

        try:
            if hasattr(self, 'idx') and self.idx is not None:
                try:
                    self.idx.close()
                except Exception:
                    pass

            import time
            import gc
            gc.collect()

            for ext in ['.dat', '.idx']:
                filepath = f"{self.filename}{ext}"
                if os.path.exists(filepath):
                    for attempt in range(5):
                        try:
                            os.remove(filepath)
                            removed_files.append(filepath)
                            break
                        except PermissionError:
                            if attempt < 4:
                                time.sleep(0.2 * (attempt + 1))
                        except Exception:
                            break
        except Exception:
            pass

        return removed_files
    
    def close(self):
        try:
            if hasattr(self, 'idx') and self.idx is not None:
                self.idx.close()
                self.idx = None
        except Exception:
            pass

    def __del__(self):
        try:
            if hasattr(self, 'idx') and self.idx is not None:
                self.idx.close()
                self.idx = None
        except Exception:
            pass
