import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.ArrayList;
import java.util.List;

public class Hello {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<String> listaObjetos = new ArrayList<String>();
        listaObjetos.add("uno");
        listaObjetos.add("dos");
        listaObjetos.add("tres");

        DataStream<String> stream = env.fromCollection(listaObjetos);

        stream.print();
        env.execute();
    }
}
