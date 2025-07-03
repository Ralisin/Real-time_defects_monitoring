import base64
import msgpack
import json
import os

from Q1 import *
from Q2 import *

from pyflink.common import WatermarkStrategy, Row
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaSink,
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema
)
from pyflink.datastream.functions import MapFunction
from pyflink.common.serialization import ByteArraySchema, SimpleStringSchema
from pyflink.common.typeinfo import Types

VERBOSE = os.getenv("VERBOSE", "false").lower() in ("1", "true", "yes")

OUTLIER_THRESH = 6000

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
            return Row(
                print_id=None,
                batch_id=None,
                tile_id=None,
                layer=None,
                tif=None
            )

def main():
    print("=== Starting Flink Kafka MsgPack Consumer ===")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(300000, CheckpointingMode.AT_LEAST_ONCE)

    print("Creating Kafka source with ByteArraySchema and Kafka Sinks...")

    # Usa ByteArraySchema per ricevere i dati come byte array
    source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:9092") \
        .set_topics("raw-batch") \
        .set_group_id("flink-msgpack-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(ByteArraySchema()) \
        .build()

    # kafka_sink_q1 = KafkaSink.builder() \
    #     .set_bootstrap_servers("kafka:9092") \
    #     .set_record_serializer(
    #         KafkaRecordSerializationSchema.builder()
    #         .set_topic("saturated-pixels")
    #         .set_value_serialization_schema(SimpleStringSchema())
    #         .build()
    #     ) \
    #     .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
    #     .set_property("linger.ms", "1") \
    #     .set_property("batch.size", "1") \
    #     .build()

    kafka_sink_q2 = KafkaSink.builder() \
        .set_bootstrap_servers("kafka:9092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("saturated-rank")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .set_property("linger.ms", "1") \
        .set_property("batch.size", "1") \
        .build()

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

    print("Detecting saturated pixels...")

    # Q1
    saturated_ds = obj_ds.map(DetectSaturatedPixels(), output_type=Types.STRING())
    # saturated_ds \
    #     .sink_to(kafka_sink_q1) \
    #     .name("kafka-sink-saturated-pixels") \
    #     .uid("sink-saturated-pixels")

    print("Filtering outliers from images... (this may take a while)")

    # Data filtered (all outranged points are set to 0)
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

    print("Windowing data and computing outliers... (this may take a while)")

    # Q2
    keyed_windowed_ds = filtered_ds.key_by(lambda row: row.tile_id, key_type=Types.INT()).count_window(3, 1)

    outlier_points_ds = keyed_windowed_ds.process(
        TemperatureDeviation(),
        output_type=Types.ROW_NAMED(
            ["print_id", "batch_id", "tile_id", "layers", "outlier_points"],
            [
                Types.STRING(),
                Types.INT(),
                Types.INT(),
                Types.LIST(Types.INT()),
                Types.LIST(
                    Types.TUPLE([Types.INT(), Types.INT(), Types.FLOAT()])
                )
            ]
        )
    )

    ranked_outliers_ds = outlier_points_ds.map(OutlierRanker(), output_type=Types.STRING())
    ranked_outliers_ds \
        .sink_to(kafka_sink_q2) \
        .name("kafka-sink-saturated-rank") \
        .uid("sink-saturated-rank")

    print("Starting job execution...")
    env.execute("Flink 2.0 - Kafka MsgPack Consumer")


if __name__ == '__main__':
    main()
