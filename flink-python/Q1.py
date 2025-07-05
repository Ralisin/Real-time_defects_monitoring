from PIL import Image
import io
import json
import numpy as np
import os

from pyflink.common import Row
from pyflink.datastream import MapFunction

VERBOSE = os.getenv("VERBOSE", "false").lower() in ("1", "true", "yes")

EMPTY_THRESH = 5000
SATURATION_THRESH = 65000

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

            saturated_count = np.sum(np_image > SATURATION_THRESH)

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
                saturated_count=saturated_count,
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

class ExtractCSVFieldsQ1(MapFunction):
    def map(self, row):
        return f"{row['batch_id']},{row['print_id']},{row['tile_id']},{row['saturated_count']}"
