import base64
import msgpack
import redis

from Q1 import *
from Q2 import *
from Q3 import *

from pyflink.common import WatermarkStrategy, Row, Configuration, Duration
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.datastream.functions import MapFunction
from pyflink.common.serialization import ByteArraySchema, Encoder
from pyflink.common.typeinfo import Types

VERBOSE = os.getenv("VERBOSE", "false").lower() in ("1", "true", "yes")
REDIS = os.getenv("REDIS", "false").lower() in ("1", "true", "yes")

OUTLIER_THRESH = 6000

output_path_q1 = "file:///flink_output/q1_output"
output_path_q2 = "file:///flink_output/q2_output"
output_path_q3 = "file:///flink_output/q3_output"

class MsgpackBytesToRowMapper(MapFunction):
    def map(self, value):
        try:
            if VERBOSE:
                print(f"[MsgpackBytesToRowMapper] Received value: {repr(value)[:100]}")

            if isinstance(value, bytes):
                message_bytes = value
            elif isinstance(value, (list, tuple)):
                message_bytes = bytes(value)
            elif isinstance(value, str):
                message_bytes = value.encode('latin1')
            else:
                message_bytes = str(value).encode('utf-8')

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
                print(f"[MsgpackBytesToRowMapper] Successfully decoded msgpack: {json_obj[:100]}...")

            return Row(
                print_id=obj["print_id"],
                batch_id=obj["batch_id"],
                tile_id=obj["tile_id"],
                layer=obj["layer"],
                tif=obj["tif"]
            )

        except Exception as e:
            print(f"[MsgpackBytesToRowMapper] Error decoding msgpack: {e}")
            return Row(
                print_id=None,
                batch_id=None,
                tile_id=None,
                layer=None,
                tif=None
            )

class RedisPublishMapFunction(MapFunction):
    def __init__(self, channel_name, redis_host='redis', redis_port=6379, redis_db=0):
        self.channel_name = channel_name
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis_client = None

    def open(self, runtime_context):
        """Inizializza la connessione Redis quando la funzione viene aperta"""
        try:
            self.redis_client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                db=self.redis_db,
                decode_responses=True
            )
            # Test della connessione
            self.redis_client.ping()
            print(f"[RedisPublishMapFunction] Redis connection established for channel: {self.channel_name}")
        except Exception as e:
            print(f"[RedisPublishMapFunction] Error connecting to Redis: {e}")
            raise e

    def map(self, value):
        """Pubblica il messaggio sul canale Redis e ritorna il valore"""
        try:
            if self.redis_client is None:
                raise Exception("Redis client not initialized")

            # Pubblica il messaggio sul canale specificato
            result = self.redis_client.publish(self.channel_name, str(value))

            if VERBOSE:
                print(f"[RedisPublishMapFunction] Published message to {self.channel_name}: {str(value)[:100]}...")
                print(f"[RedisPublishMapFunction] Number of subscribers: {result}")

            return value

        except Exception as e:
            print(f"[RedisPublishMapFunction] Error publishing to Redis channel {self.channel_name}: {e}")
            # Tenta di riconnettersi
            try:
                self.redis_client = redis.Redis(
                    host=self.redis_host,
                    port=self.redis_port,
                    db=self.redis_db,
                    decode_responses=True
                )
                self.redis_client.publish(self.channel_name, str(value))
            except Exception as reconnect_error:
                print(f"[RedisPublishMapFunction] Failed to reconnect and publish: {reconnect_error}")

            return f"Error sending message to Redis: {str(value)[100]}"

    def close(self):
        """Chiude la connessione Redis"""
        if self.redis_client:
            self.redis_client.close()
            print(f"[RedisPublishMapFunction] Redis connection closed for channel: {self.channel_name}")

def main():
    print("=== Starting Flink Kafka MsgPack Consumer ===")

    env = StreamExecutionEnvironment.get_execution_environment()

    config = Configuration()
    config.set_string("state.checkpoint-storage", "filesystem")
    config.set_string("state.checkpoints.dir", "file:///tmp/flink-checkpoints")  # o HDFS
    env.configure(config)

    env.get_config().enable_object_reuse()

    # env.set_parallelism(1)
    env.enable_checkpointing(5 * 60 * 1000) # Every 5 minutes

    print("Creating Kafka source with ByteArraySchema and Kafka Sinks...")

    # Usa ByteArraySchema per ricevere i dati come byte array
    source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:9092") \
        .set_topics("raw-batch") \
        .set_group_id("flink-msgpack-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(ByteArraySchema()) \
        .build()

    redis_sink_q1 = RedisPublishMapFunction("saturated-pixels")
    redis_sink_q2 = RedisPublishMapFunction("saturated-rank")
    redis_sink_q3 = RedisPublishMapFunction("centroids")

    csv_sink_q1 = FileSink.for_row_format(
        output_path_q1,
        Encoder.simple_string_encoder()
    ).with_output_file_config(
        OutputFileConfig.builder()
        .with_part_prefix("part")
        .with_part_suffix(".csv")
        .build()
    ).with_rolling_policy(
        RollingPolicy.default_rolling_policy(
            15 * 60 * 1000,  # rollover every 15 minutes
            5 * 60 * 1000,  # if inactive for 5 minutes, close
            512 * 1024 * 1024  # max 512 MB per file
        )
    ).build()

    csv_sink_q2 = FileSink.for_row_format(
        output_path_q2,
        Encoder.simple_string_encoder()
    ).with_output_file_config(
        OutputFileConfig.builder()
        .with_part_prefix("part")
        .with_part_suffix(".csv")
        .build()
    ).with_rolling_policy(
        RollingPolicy.default_rolling_policy(
            15 * 60 * 1000,     # rollover every 15 minutes
            5 * 60 * 1000,      # if inactive for 5 minutes, close
            512 * 1024 * 1024   # max 512 MB per file
        )
    ).build()
    csv_sink_q3 = FileSink.for_row_format(
        output_path_q3,
        Encoder.simple_string_encoder()
    ).with_output_file_config(
        OutputFileConfig.builder()
        .with_part_prefix("part")
        .with_part_suffix(".csv")
        .build()
    ).with_rolling_policy(
        RollingPolicy.default_rolling_policy(
            15 * 60 * 1000,  # rollover every 15 minutes
            5 * 60 * 1000,  # if inactive for 5 minutes, close
            512 * 1024 * 1024  # max 512 MB per file
        )
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
    ).rebalance()

    print("Detecting saturated pixels...")

    # Q1
    # Data filtered (all outranged points are set to 0)
    filtered_ds = obj_ds.map(FilterPixels(), output_type=Types.ROW_NAMED(
        ["print_id", "batch_id", "tile_id", "layer", "saturated_count", "tif"],
        [
            Types.STRING(),                      # print_id
            Types.INT(),                         # batch_id
            Types.INT(),                         # tile_id
            Types.INT(),                         # layer
            Types.INT(),                         # saturated_count
            Types.PRIMITIVE_ARRAY(Types.BYTE())  # tif
        ]
    ))
    q1_csv_data = filtered_ds.map(ExtractCSVFieldsQ1(), output_type=Types.STRING())
    q1_csv_data.sink_to(csv_sink_q1).uid("csv-q1-sink")

    q1_csv_data.map(redis_sink_q1, output_type=Types.STRING())

    # Q2
    keyed_windowed_ds = filtered_ds.key_by(lambda row: row.tile_id, key_type=Types.INT()).count_window(3, 1)

    outlier_points_ds = keyed_windowed_ds.process(
        TemperatureDeviationConvolve(),
        output_type=Types.ROW_NAMED(
            ["print_id", "batch_id", "tile_id", "layers", "saturated_count", "outlier_points"],
            [
                Types.STRING(),             # print_id
                Types.INT(),                # batch_id
                Types.INT(),                # tile_id
                Types.LIST(Types.INT()),    # layers
                Types.INT(),                # saturated_count
                Types.LIST(                 # outlier_points
                    Types.TUPLE([Types.INT(), Types.INT(), Types.FLOAT()])
                )
            ]
        )
    )

    ranked_outliers_ds = outlier_points_ds.map(OutlierRanker(), output_type = Types.ROW_NAMED(
        [
            "seq_id", "print_id", "tile_id",
            "p1_point", "dp1",
            "p2_point", "dp2",
            "p3_point", "dp3",
            "p4_point", "dp4",
            "p5_point", "dp5"
        ],
        [
            Types.INT(), Types.STRING(), Types.INT(),
            Types.TUPLE([Types.INT(), Types.INT()]), Types.FLOAT(),
            Types.TUPLE([Types.INT(), Types.INT()]), Types.FLOAT(),
            Types.TUPLE([Types.INT(), Types.INT()]), Types.FLOAT(),
            Types.TUPLE([Types.INT(), Types.INT()]), Types.FLOAT(),
            Types.TUPLE([Types.INT(), Types.INT()]), Types.FLOAT(),
        ]
    ))
    q2_csv_data = ranked_outliers_ds.map(ExtractCSVFieldsQ2(), output_type=Types.STRING())
    q2_csv_data.sink_to(csv_sink_q2).uid("csv-q2-sink")

    q2_csv_data.map(redis_sink_q2, output_type=Types.STRING())

    # Q3
    eps_value = 20
    min_samples_value = 5

    clustered_ds = outlier_points_ds.map(
        OutlierClustering(eps=eps_value, min_samples=min_samples_value),
        output_type=Types.ROW_NAMED(
            ["seq_id", "print_id", "tile_id", "saturated", "centroids"],
            [
                Types.INT(),
                Types.STRING(),
                Types.INT(),
                Types.INT(),
                Types.LIST(Types.TUPLE([Types.FLOAT(), Types.FLOAT(), Types.INT()]))
            ]
        )
    )

    q3_csv_data = clustered_ds.map(ExtractCSVFieldsQ3(), output_type=Types.STRING())
    q3_csv_data.sink_to(csv_sink_q3).uid("csv-q3-sink")

    q3_csv_data.map(redis_sink_q3, output_type=Types.STRING())

    print("Starting job execution...")
    env.execute("Flink 2.0 - Kafka MsgPack Consumer")


if __name__ == '__main__':
    main()
