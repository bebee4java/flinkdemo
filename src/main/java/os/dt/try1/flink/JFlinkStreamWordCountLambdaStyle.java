package os.dt.try1.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;


/**
 * flink流wordcount java实现
 * Created by songgr on 2021/04/15.
 */
public class JFlinkStreamWordCountLambdaStyle {

    public static void main(String[] args) throws Exception {
        // 0.准备env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置执行模式
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        // 1.准备数据
        DataStream<String> lines = env.fromElements("hello flink", "flink streaming", "flink");
        // 2.切割
        DataStream<String> words = lines.flatMap((String word, Collector<String> out) ->
                Arrays.stream(word.split(" ")).forEach(out::collect)).returns(Types.STRING);

//        words.print();
        // 3.设置1
        DataStream<Tuple2<String, Integer>> wordKeyBy = words.map((String word) -> Tuple2.of(word, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        KeyedStream<Tuple2<String, Integer>, String> wordAndOne = wordKeyBy.keyBy(t -> t.f0);

//        map.print();

        DataStream<Tuple2<String, Integer>> result = wordAndOne.sum(1);

        result.print();

        result.writeAsText("hdfs://node1:9000/data/flink/wc/" + System.currentTimeMillis() )
        .setParallelism(1);

        env.execute();

    }
}
