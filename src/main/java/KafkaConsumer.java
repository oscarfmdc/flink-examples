import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class KafkaConsumer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("flink")
                .setGroupId("flink")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // idUsuario, ubicación, intensidad
        DataStreamSource<String> text = env.fromSource(source, WatermarkStrategy
                .forBoundedOutOfOrderness(Duration.ofSeconds(1)), "Kafka");

        text.print();

        text
                .map(new MapFunction<String, Tuple3<Long, String, Long>>() {
                    @Override
                    public Tuple3<Long, String, Long> map(String s) {
                        String[] words = s.split(",");
                        return new Tuple3<Long, String, Long>(Long.parseLong(words[0]), words[1], Long.parseLong(words[2]));
                    }
                })
                .filter((new FilterFunction<Tuple3<Long, String, Long>>() {
                    @Override
                    public boolean filter(Tuple3 value) {
                        return (Long)value.f2 > 5;
                    }
                }))
                .keyBy(value -> value.f0 + "," + value.f1)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .aggregate(new AverageAggregate())
                .filter((new FilterFunction<Tuple4<Long, String, Long, Long>>() {
                    @Override
                    public boolean filter(Tuple4 value) {
                        return (Long)value.f3 > 1;
                    }
                }))
                .map(Tuple4::toString)
                .sinkTo(
                        new Elasticsearch7SinkBuilder<String>()
                                .setBulkFlushMaxActions(1) // Instructs the sink to emit after every element, otherwise they would be buffered
                                .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
                                .setEmitter(
                                        (element, context, indexer) ->
                                                indexer.add(createIndexRequest(element)))
                                .build());

        env.execute();

    }

    private static IndexRequest createIndexRequest(String element) {
        Map<String, Object> json = new HashMap<>();
        json.put("data", element);

        return Requests.indexRequest()
                .index("flink")
                .id(element)
                .source(json);
    }

    /**
     * The accumulator is used to keep a running sum and a count. The {@code getResult} method
     * computes the average.
     */
    private static class AverageAggregate
            implements AggregateFunction <Tuple3<Long, String, Long>, Tuple4<Long, String, Long, Long>, Tuple4<Long, String, Long, Long>> {
        @Override
        public Tuple4<Long, String, Long, Long> createAccumulator() {
            // sum, count
            return new Tuple4<Long, String, Long, Long>(0L, "", 0L, 0L);
        }

        @Override
        public Tuple4<Long, String, Long, Long> add(Tuple3<Long, String, Long> value, Tuple4<Long, String, Long, Long> accumulator) {
            return new Tuple4<Long, String, Long, Long>(value.f0, value.f1, accumulator.f2 + value.f2, accumulator.f3 + 1L);
        }

        @Override
        public Tuple4<Long, String, Long, Long> getResult(Tuple4<Long, String, Long, Long> accumulator) {
            return new Tuple4<Long, String, Long, Long>(accumulator.f0, accumulator.f1, accumulator.f2 / accumulator.f3, accumulator.f3);
        }

        @Override
        public Tuple4<Long, String, Long, Long> merge(Tuple4<Long, String, Long, Long> a, Tuple4<Long, String, Long, Long> b) {
            return new Tuple4<Long, String, Long, Long>(a.f0, b.f1, a.f2 + b.f2, a.f3 + b.f3);
        }
    }
}
