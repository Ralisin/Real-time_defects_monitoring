package ralisin.it;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.ByteArraySchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import ralisin.it.csvUtils.ExtractCSVFieldsQ1;
import ralisin.it.csvUtils.ExtractCSVFieldsQ2;
import ralisin.it.csvUtils.ExtractCSVFieldsQ3;
import ralisin.it.maps.*;
import ralisin.it.model.*;

public class Main {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka source
        KafkaSource<byte[]> source = KafkaSource.<byte[]>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("raw-batch")
                .setGroupId("flink-msgpack-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new ByteArraySchema())
                .build();

        DataStream<byte[]> byteStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "KafkaSource"
        );

        var redisSinkQ1 = new RedisPublishMapFunction<String>("saturated-pixels");
        var redisSinkQ2 = new RedisPublishMapFunction<String>("saturated-rank");
        var redisSinkQ3 = new RedisPublishMapFunction<String>("centroids");

        DataStream<PrintTile> tiles = byteStream.map(new MsgpackBytesToRowMapper());

        DataStream<PrintTileQ1> filteredQ1 = tiles
                .map(new FilterPixels())
                .name("filter-pixels");

        DataStream<String> q1CsvData = filteredQ1.map(new ExtractCSVFieldsQ1()).name("extract-csv-q1");
        q1CsvData.map(redisSinkQ1).name("redis-sink-q1");

        q1CsvData.print();

        var keyed = filteredQ1.keyBy(row -> row.tile_id);
        var windowed = keyed.countWindow(3, 1);

        SingleOutputStreamOperator<OutlierResult> outlierPoints = windowed
                .process(new TemperatureDeviation())
                .name("outlier-detection");

        SingleOutputStreamOperator<OutlierRank> rankedOutliers = outlierPoints
                .map(new OutlierRanker())
                .name("outlier-ranking");

        SingleOutputStreamOperator<String> q2CsvData = rankedOutliers
                .map(new ExtractCSVFieldsQ2())
                .name("extract-csv-fields-q2");
        q1CsvData.map(redisSinkQ2).name("redis-sink-q2");

        q2CsvData.print();

        SingleOutputStreamOperator<ClusteredResult> clusteredResults = outlierPoints
                .map(new OutlierClustering(20, 5))
                .name("outlier-clustering");

        SingleOutputStreamOperator<String> q3CsvData = clusteredResults
                .map(new ExtractCSVFieldsQ3())
                .name("extract-csv-fields-q3");

        q3CsvData
                .map(redisSinkQ3)
                .name("redis-sink-q3");

        q3CsvData.print();

        env.execute("Flink Kafka MsgPack Consumer");
    }
}