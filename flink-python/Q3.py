import json
import numpy as np
import os

from pyflink.common import Row
from pyflink.datastream import MapFunction
from sklearn.cluster import DBSCAN

VERBOSE = os.getenv("VERBOSE", "false").lower() in ("1", "true", "yes")

class OutlierClustering(MapFunction):
    def __init__(self, eps=3, min_samples=2):
        self.eps = eps
        self.min_samples = min_samples

    def map(self, value):
        seq_id = value.batch_id
        print_id = value.print_id
        tile_id = value.tile_id
        saturated_count = value.saturated_count
        outlier_points = value.outlier_points  # list of (x, y, deviation)

        try:
            print(f"seq_id: {seq_id}, print_id: {print_id}, tile_id: {tile_id}, saturated: {saturated_count}, outlier_points len: {len(outlier_points)}")

            if len(outlier_points) == 0:
                return Row(
                    seq_id=seq_id,
                    print_id=print_id,
                    tile_id=tile_id,
                    saturated=saturated_count,
                    centroids=[]
                )

            print("[OutlierClustering] value getted")

            # Extract only 2D coordinates (x, y) for clustering
            points = np.array([(x, y) for x, y, _ in outlier_points])

            print("[OutlierClustering] Points extracted")

            # Apply DBSCAN clustering algorithm
            clustering = DBSCAN(eps=self.eps, min_samples=self.min_samples, metric='euclidean').fit(points)
            labels = clustering.labels_

            print(f"[OutlierClustering] clustering and labels extracted - labels: {labels}")

            unique_labels = set(labels)
            if -1 in unique_labels:
                unique_labels.remove(-1)  # Remove noise

            # CALCULATE CENTROIDS
            centroids = []
            for label in unique_labels:
                # Get all points in the current cluster
                cluster_points = points[labels == label]

                # Compute centroid as mean of coordinates
                cx, cy = cluster_points.mean(axis=0)

                count = len(cluster_points)
                centroids.append((float(cx), float(cy), count))

                print(f"centroid: ({cx}, {cy}), count: {count}")

            if VERBOSE:
                print(f"[OutlierClustering] seq_id: {seq_id}, print_id: {print_id}, tile_id: {tile_id}, saturated: {saturated_count}, centroids: {centroids}")

            return Row(
                seq_id=seq_id,
                print_id=print_id,
                tile_id=tile_id,
                saturated=saturated_count,
                centroids=centroids
            )
        except Exception as e:
            print(f"[OutlierClustering] Error processing image: {e}")
            return Row(
                seq_id=seq_id,
                print_id=print_id,
                tile_id=tile_id,
                saturated=saturated_count,
                centroids=[(-1, -1, -1)]
            )

class ExtractCSVFieldsQ3(MapFunction):
    def map(self, row):
        def fix_centroid(c):
            if c is None or c == (-1, -1, -1):
                return {'x': -1, 'y': -1, 'count': -1}
            else:
                x, y, count = c
                return {'x': float(x), 'y': float(y), 'count': int(count)}

        centroids = [fix_centroid(c) for c in row['centroids']]
        centroids_json = json.dumps(centroids)

        print(f"[ExtractCSVFieldsQ3] row: {row}")

        csv_line = f"{row['seq_id']},{row['print_id']},{row['tile_id']},{row['saturated']},{centroids_json}"

        return csv_line