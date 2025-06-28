import io
import json
import numpy as np
from PIL import Image

from pyflink.common import Row
from pyflink.datastream import MapFunction

EMPTY_THRESH = 5000
SATURATION_THRESH = 65000

class DetectSaturatedPixels(MapFunction):
    def map(self, row):
        print_id = row[0]
        batch_id = row[1]
        tile_id = row[2]
        tif_bytes = row[4]

        try:
            image = Image.open(io.BytesIO(tif_bytes))
            np_image = np.array(image)

            saturated_count = np.sum(np_image > SATURATION_THRESH)

            return json.dumps({
                "seq_id": batch_id,
                "print_id": print_id,
                "tile_id": tile_id,
                "saturated": int(saturated_count)
            })
        except Exception as e:
            print(f"Error processing image: {e}")
            return json.dumps({
                "seq_id": batch_id,
                "print_id": print_id,
                "tile_id": tile_id,
                "saturated": -1  # -1 indicates a processing error
            })

class Q1ToJsonMapFunction(MapFunction):
    def map(self, row):
        return json.dumps({
            "seq_id": row[0],
            "print_id": row[1],
        })

class FilterPixels(MapFunction):
    def map(self, row):
        print_id = row[0]
        batch_id = row[1]
        tile_id = row[2]
        layer = row[3]
        tif_bytes = row[4]

        try:
            image = Image.open(io.BytesIO(tif_bytes))
            np_image = np.array(image)

            mask = (EMPTY_THRESH <= np_image) & (np_image <= SATURATION_THRESH)
            filtered_np_image = np.where(mask, np_image, 0)

            filtered_image = Image.fromarray(filtered_np_image.astype(np.uint16), mode=image.mode)

            # Write back the image into TIFF format
            output_bytes_io = io.BytesIO()
            filtered_image.save(output_bytes_io, format='TIFF')
            filtered_bytes = output_bytes_io.getvalue()

            return Row(
                print_id=print_id,
                batch_id=batch_id,
                tile_id=tile_id,
                layer=layer,
                tif=filtered_bytes
            )
        except Exception as e:
            print(f"Error processing image: {e}")
            return Row(
                print_id=print_id,
                batch_id=batch_id,
                tile_id=tile_id,
                layer=layer,
                tif=tif_bytes
            )
