import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MyProcessWindowFunction
        extends ProcessWindowFunction<Tuple2<Long, String>, String, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<Tuple2<Long, String>> input, Collector<String> out) {
        long count = 0;
        for (Tuple2<Long, String> in: input) {
            count++;
        }
        out.collect("Window: " + context.window() + "count: " + count);
    }
}