import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import java.time.Duration;

/**
 * Reads a log file
 */
public class LogReader {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final FileSource<String> source =
                FileSource.forRecordStreamFormat(new TextLineInputFormat(),
                        new Path("/home/oscarfmdc/Downloads/Flink/"))
                        .monitorContinuously(Duration.ofSeconds(1L))
                        .build();



        final DataStream<String> stream = env.fromSource(source,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1)),
                "file-source");

        DataStream<Tuple2<Long, String>> tuplestream = stream
                .map((MapFunction<String, Tuple2<Long, String>>) value ->
                new Tuple2<Long, String>(Long.parseLong(value.split(" ")[0]), value.split(" ")[1]))
                .returns(Types.TUPLE(Types.LONG, Types.STRING));

        tuplestream
                .keyBy(tuple -> tuple.f1)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new MyProcessWindowFunction())
                .print();

        env.execute();

    }
}
