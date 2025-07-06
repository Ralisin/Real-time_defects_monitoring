import io
import numpy as np
import os
import tifffile as tiff
from typing import Iterable

from pyflink.common import Row
from pyflink.datastream import ProcessWindowFunction, MapFunction
from scipy.ndimage import convolve

VERBOSE = os.getenv("VERBOSE", "false").lower() in ("1", "true", "yes")

class TemperatureDeviation(ProcessWindowFunction):
    def __init__(self):
        shape = (3, 5, 5)
        center = (2, 2, 2)
        l, x, y = np.indices(shape)
        dist = np.abs(l - center[0]) + np.abs(x - center[1]) + np.abs(y - center[2])
        self.mask_nearest = dist <= 2
        self.mask_external = (dist > 2) & (dist <= 4)

    def process(self, key, context, elements: Iterable):
        try:
            events = list(elements)
            events.sort(key=lambda e: e.layer)

            print_id = events[2].print_id
            batch_id = events[2].batch_id

            tile_id = events[2].tile_id
            saturated_count = events[2].saturated_count
            layers = [int(e.layer) for e in events]

            image3d = np.stack([tiff.imread(io.BytesIO(e.tif)) for e in events], axis=0)

            # Add padding of 0 layers, 2 points in height, and 2 points in width with NaN values
            image3d = image3d.astype(np.float32)
            image3d_padded = np.pad(image3d, ((0, 0), (2, 2), (2, 2)), mode='constant', constant_values=np.nan)

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

                    local_neighbors = patch[self.mask_nearest]
                    external_neighbors = patch[self.mask_external]

                    avg_local = np.nanmean(local_neighbors)
                    avg_external = np.nanmean(external_neighbors)

                    # Nan check
                    if np.isnan(avg_local) or np.isnan(avg_external):
                        continue

                    local_temp_deviation = abs(float(avg_local) - float(avg_external))

                    if local_temp_deviation > 6000:
                        outlier_points.append((int(x_p), int(y_p), float(local_temp_deviation)))

            if VERBOSE:
                print(f"[TemperatureDeviation] layers: {layers}, batch_id: {batch_id}, tile_id: {tile_id}, outlier_points len: {len(outlier_points)}")

            result_row = Row(
                print_id=print_id,
                batch_id=batch_id,
                tile_id=tile_id,
                layers=layers,
                saturated_count=saturated_count,
                outlier_points=outlier_points
            )

            return [result_row]
        except Exception as e:
            print(f"[TemperatureDeviation] Error processing window: {e}")
            return [
                Row(
                    print_id="",
                    batch_id=-1,
                    tile_id=-1,
                    layers=[],
                    saturated_count=-1,
                    outlier_points=[]
                )
            ]

class TemperatureDeviationConvolve(ProcessWindowFunction):
    def __init__(self):
        shape = (3, 5, 5)
        center = (2, 2, 2)
        l, x, y = np.indices(shape)
        dist = np.abs(l - center[0]) + np.abs(x - center[1]) + np.abs(y - center[2])
        self.mask_nearest = dist <= 2
        self.mask_external = (dist > 2) & (dist <= 4)

    def process(self, key, context, elements: Iterable):
        try:
            events = list(elements)
            events.sort(key=lambda e: e.layer)

            print_id = events[2].print_id
            batch_id = events[2].batch_id

            tile_id = events[2].tile_id
            saturated_count = events[2].saturated_count
            layers = [int(e.layer) for e in events]

            if VERBOSE:
                print(f"[TemperatureDeviationConvolve] batch_id: {batch_id}, tile_id: {tile_id}, layers: {layers}")

            image3d = np.stack([tiff.imread(io.BytesIO(e.tif)) for e in events], axis=0)
            image3d = image3d.astype(np.float32)

            # Add padding of 0 layers, 2 points in height, and 2 points in width with NaN values
            image3d_padded = np.pad(image3d, ((0, 0), (2, 2), (2, 2)), mode='constant', constant_values=np.nan)

            top_layer_idx = image3d.shape[0] - 1
            image_patch = image3d_padded[top_layer_idx - 2:top_layer_idx + 1]  # (3, H, W)

            sum_nearest = convolve(np.nan_to_num(image_patch, nan=0.0), self.mask_nearest, mode='constant', cval=0.0)
            sum_external = convolve(np.nan_to_num(image_patch, nan=0.0), self.mask_external, mode='constant', cval=0.0)

            count_nearest = convolve(~np.isnan(image_patch), self.mask_nearest, mode='constant', cval=0.0)
            count_external = convolve(~np.isnan(image_patch), self.mask_external, mode='constant', cval=0.0)

            with np.errstate(divide='ignore', invalid='ignore'):
                avg_nearest = sum_nearest / count_nearest
                avg_external = sum_external / count_external

            deviation = np.abs(avg_nearest - avg_external)
            valid_mask = ~np.isnan(avg_nearest) & ~np.isnan(avg_external)
            outlier_mask = (deviation > 6000) & valid_mask

            x_coords, y_coords = np.where(outlier_mask[2])
            outlier_points = [
                (int(x), int(y), float(deviation[2, x, y]))
                for x, y in zip(x_coords, y_coords)
            ]

            if VERBOSE:
                print(f"[TemperatureDeviationConvolve] layers: {layers}, batch_id: {batch_id}, tile_id: {tile_id}, outlier_points len: {len(outlier_points)}")

            result_row = Row(
                print_id=print_id,
                batch_id=batch_id,
                tile_id=tile_id,
                layers=layers,
                saturated_count=saturated_count,
                outlier_points=outlier_points
            )

            return [result_row]
        except Exception as e:
            print(f"[TemperatureDeviationConvolve] Error processing window: {e}")
            return [
                Row(
                    print_id="",
                    batch_id=-1,
                    tile_id=-1,
                    layers=[],
                    saturated_count=-1,
                    outlier_points=[]
                )
            ]

class OutlierRanker(MapFunction):
    def map(self, row):
        print_id, batch_id, tile_id, layers, saturated_count, outlier_points = row

        try:
            sorted_points = sorted(outlier_points, key=lambda p: p[2], reverse=True)
            top5 = sorted_points[:5]
            while len(top5) < 5:
                top5.append((None, None, None))

            seq_id = batch_id

            fields = []

            for p in top5:
                x, y, dev = p
                if x is None:
                    fields.append((-1, -1))
                    fields.append(-1.0)
                else:
                    fields.append((x, y))
                    fields.append(dev)

            return Row(
                seq_id=seq_id,
                print_id=print_id,
                tile_id=tile_id,
                p1_point=fields[0],
                dp1=fields[1],
                p2_point=fields[2],
                dp2=fields[3],
                p3_point=fields[4],
                dp3=fields[5],
                p4_point=fields[6],
                dp4=fields[7],
                p5_point=fields[8],
                dp5=fields[9],
            )

        except Exception as e:
            print(f"[OutlierRanker] Error processing image: {e}")
            fields = []
            for _ in range(5):
                fields.append((-2, -2))
                fields.append(-2.0)

            return Row(
                seq_id=batch_id,
                print_id=print_id,
                tile_id=tile_id,
                p1_point=fields[0],
                dp1=fields[1],
                p2_point=fields[2],
                dp2=fields[3],
                p3_point=fields[4],
                dp3=fields[5],
                p4_point=fields[6],
                dp4=fields[7],
                p5_point=fields[8],
                dp5=fields[9],
            )

class ExtractCSVFieldsQ2(MapFunction):
    def map(self, row):
        def point_to_str(point):
            if point is None or point == (-1, -1):
                return "(-1;-1)"
            else:
                return f"({point[0]};{point[1]})"

        csv_line = (
            f"{row.seq_id},{row.print_id},{row.tile_id}," +
            f"{point_to_str(row.p1_point)},{row.dp1}," +
            f"{point_to_str(row.p2_point)},{row.dp2}," +
            f"{point_to_str(row.p3_point)},{row.dp3}," +
            f"{point_to_str(row.p4_point)},{row.dp4}," +
            f"{point_to_str(row.p5_point)},{row.dp5}"
        )

        return csv_line
