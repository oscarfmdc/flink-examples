import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Random;

public class ProcessingTime {

    private static class DataSource extends RichParallelSourceFunction<Tuple2<String, Integer>> {
        private volatile boolean isRunning = true;
        @Override
        public void run(SourceContext<Tuple2<String, Integer>> ctx) throws InterruptedException {
            Random random = new Random();
            while (isRunning) {
                Thread.sleep(2000);
                String key = "K" + random.nextInt(3);
                int value = random.nextInt(10) + 1;
                System.out.printf("Emits\t(%s, %d)%n", key, value);
                ctx.collect(new Tuple2<>(key, value));
            }
        }
        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        WatermarkStrategy<Tuple2<String, Integer>> watermarkStrat = WatermarkStrategy
                .forBoundedOutOfOrderness(Duration.ofSeconds(1));

        DataStream<Tuple2<String, Integer>> ds = env.addSource(new DataSource());
        ds.assignTimestampsAndWatermarks(watermarkStrat)
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1)
                .addSink(new SinkFunction<>() {
                    @Override
                    public void invoke(Tuple2<String, Integer> value, Context context) {
                        System.out.println(value.f1);
                    }
                });

        env.execute();
    }
}