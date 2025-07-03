import io
import json
import numpy as np
import os
from PIL import Image
from typing import Iterable

from pyflink.common import Row
from pyflink.datastream import ProcessWindowFunction, MapFunction

VERBOSE = os.getenv("VERBOSE", "false").lower() in ("1", "true", "yes")

class TemperatureDeviation(ProcessWindowFunction):
    def process(self, key, context, elements: Iterable):
        try:
            events = list(elements)
            events.sort(key=lambda e: e.layer)

            print_id = events[0].print_id
            batch_id = events[0].batch_id
            tile_id = events[0].tile_id
            layers = [int(e.layer) for e in events]

            if VERBOSE:
                print(f"[TemperatureDeviationFunction] tile_id: {tile_id}, layers: {layers}")

            image3d = np.stack([Image.open(io.BytesIO(e.tif)) for e in events], axis=0)

            # Add padding of 0 layers, 2 points in height, and 2 points in width with NaN values
            image3d = image3d.astype(np.float32)
            image3d_padded = np.pad(image3d, ((0, 0), (2, 2), (2, 2)), mode='constant', constant_values=np.nan)

            # Fixed masks
            shape = (3, 5, 5)
            center = (2, 2, 2) # Last layer, in the middle

            l, x, y = np.indices(shape)

            dist = np.abs(l - center[0]) + np.abs(x - center[1]) + np.abs(y - center[2])

            mask_nearest = dist <= 2
            mask_external = (dist > 2) & (dist <= 4)

            top_layer_idx = image3d.shape[0] - 1
            height, width = image3d.shape[1], image3d.shape[2]

            outlier_points = []

            for x_p in range(height):
                for y_p in range(width):
                    patch = image3d_padded[
                        top_layer_idx - 2:top_layer_idx + 1,
                        x_p:x_p + 5,
                        y_p:y_p + 5
                    ]  # shape (3,5,5)

                    local_neighbors = patch[mask_nearest]
                    external_neighbors = patch[mask_external]

                    avg_local = np.nanmean(local_neighbors)
                    avg_external = np.nanmean(external_neighbors)

                    # Nan check
                    if np.isnan(avg_local) or np.isnan(avg_external):
                        continue

                    local_temp_deviation = abs(float(avg_local) - float(avg_external))

                    if local_temp_deviation > 6000:
                        # if VERBOSE:
                        #     print(f"layers: {layers}, tile_id: {tile_id}, point: ({x_p}, {y_p}), local_temp_deviation: {local_temp_deviation}")

                        outlier_points.append((int(x_p), int(y_p), float(local_temp_deviation)))

            if VERBOSE:
                print(f"layers: {layers}, tile_id: {tile_id}, outlier_points len: {len(outlier_points) if outlier_points else 'no points'}")

            result_row = Row(
                print_id=print_id,
                batch_id=batch_id,
                tile_id=tile_id,
                layers=layers,
                outlier_points=outlier_points
            )

            return [result_row]
        except Exception as e:
            print(f"Error processing window: {e}")
            return [
                Row(
                    print_id="",
                    batch_id=-1,
                    tile_id=-1,
                    layers=[],
                    outlier_points=[]
                )
            ]

class OutlierRanker(MapFunction):
    def map(self, row):
        print_id, batch_id, tile_id, layers, outlier_points = row

        sorted_points = sorted(outlier_points, key=lambda p: p[2], reverse=True)
        top5 = sorted_points[:5]
        while len(top5) < 5:
            top5.append((None, None, None))

        seq_id = batch_id

        # costruisco il dict
        result = {
            "seq_id": seq_id,
            "print_id": print_id,
            "tile_id": tile_id,
            "top5": []
        }

        for p in top5:
            x, y, dev = p
            if x is None:
                result["top5"].append({
                    "point": (-1, -1),
                    "deviation": -1.0
                })
            else:
                result["top5"].append({
                    "point": (x, y),
                    "deviation": dev
                })

        if VERBOSE:
            print(f"Output JSON for tile_id: {tile_id} - {layers}: {result}")
        print(f"Output JSON for tile_id: {tile_id} - {layers}: {result}")

        return json.dumps(result)
