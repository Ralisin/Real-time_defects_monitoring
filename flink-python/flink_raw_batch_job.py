import base64
import io
import json
import msgpack
import numpy as np
import os
from PIL import Image
import heapq

from Q1 import *

from pyflink.common import WatermarkStrategy, Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSink,
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema
)
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction
from pyflink.common.serialization import ByteArraySchema, SimpleStringSchema

VERBOSE = os.getenv("VERBOSE", "false").lower() in ("1", "true", "yes")

class MsgpackBytesToRowMapper(MapFunction):
    def map(self, value):
        try:
            if VERBOSE:
                print(f"Received value: {repr(value)[:100]}")
                print(f"Value type: {type(value)}")
                print(f"Value length: {len(value) if hasattr(value, '__len__') else 'no length'}")

            if isinstance(value, bytes):
                message_bytes = value
            elif isinstance(value, (list, tuple)):
                message_bytes = bytes(value)
            elif isinstance(value, str):
                message_bytes = value.encode('latin1')
            else:
                message_bytes = str(value).encode('utf-8')

            if VERBOSE:
                print(f"First 20 bytes: {message_bytes[:20]}")

            # Decodifica msgpack con raw=False
            obj = msgpack.unpackb(message_bytes, raw=False)

            # Funzione ricorsiva per convertire bytes in base64
            def convert_bytes(o):
                if isinstance(o, bytes):
                    return base64.b64encode(o).decode('ascii')
                elif isinstance(o, dict):
                    return {k: convert_bytes(v) for k, v in o.items()}
                elif isinstance(o, list):
                    return [convert_bytes(i) for i in o]
                else:
                    return o

            if VERBOSE:
                converted_obj = convert_bytes(obj)

                json_obj = json.dumps(converted_obj)
                print(f"Successfully decoded msgpack: {json_obj[:100]}...")

            return Row(
                print_id=obj["print_id"],
                batch_id=obj["batch_id"],
                tile_id=obj["tile_id"],
                layer=obj["layer"],
                tif=obj["tif"]
            )

        except Exception as e:
            print(f"Error decoding msgpack: {e}")
            error_obj = {
                "error": str(e),
                "error_type": type(e).__name__,
                "value_type": str(type(value)),
                "value_repr": repr(value)[:200] if hasattr(value, '__repr__') else "no repr"
            }
            return json.dumps(error_obj, indent=2)

def main():
    print("=== Starting Flink Kafka MsgPack Consumer ===")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    print("Creating Kafka source with ByteArraySchema and Kafka Sinks...")

    # Usa ByteArraySchema per ricevere i dati come byte array
    source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:9092") \
        .set_topics("raw-batch") \
        .set_group_id("flink-msgpack-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(ByteArraySchema()) \
        .build()

    kafka_sink_q1 = KafkaSink.builder() \
        .set_bootstrap_servers("kafka:9092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("saturated-pixels")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ).build()

    print("Creating data stream from source...")

    # Create data stream from source
    ds = env.from_source(
        source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="Kafka Source"
    )

    print("Adding transformation mapper...")

    # Map Kafka messages to structured data
    obj_ds = ds.map(
        MsgpackBytesToRowMapper(),
        output_type=Types.ROW_NAMED(
            ["print_id", "batch_id", "tile_id", "layer", "tif"],
            [
                Types.STRING(),                      # print_id
                Types.INT(),                         # batch_id
                Types.INT(),                         # tile_id
                Types.INT(),                         # layer
                Types.PRIMITIVE_ARRAY(Types.BYTE())  # tif
            ]
        )
    )

    # Q1
    saturated_ds = obj_ds.map(DetectSaturatedPixels(), output_type=Types.STRING())
    saturated_ds.sink_to(kafka_sink_q1)

    # Data filtered (all outrange points are set to 0)
    filtered_ds = obj_ds.map(FilterPixels(), output_type=Types.ROW_NAMED(
        ["print_id", "batch_id", "tile_id", "layer", "tif"],
        [
            Types.STRING(),                      # print_id
            Types.INT(),                         # batch_id
            Types.INT(),                         # tile_id
            Types.INT(),                         # layer
            Types.PRIMITIVE_ARRAY(Types.BYTE())  # tif
        ]
    ))

    # keyed = filtered_ds.key_by(lambda row: row.tile_id, key_type=Types.INT())

    # def image_mode(row):
    #     image = Image.open(io.BytesIO(row[4]))
    #
    #     print(f"Image mode: {image.mode}")
    #
    #     np_image = np.array(image)
    #     print(f"Image shape: {np_image.shape}")
    #     print(f"Image dtype: {np_image.dtype}")
    #     print(f"Max pixel: {np_image.max()}, Min pixel: {np_image.min()}")
    #
    # obj_ds.map(image_mode)

    print("Starting job execution...")
    env.execute("Flink 2.0 - Kafka MsgPack Consumer")


if __name__ == '__main__':
    main()
