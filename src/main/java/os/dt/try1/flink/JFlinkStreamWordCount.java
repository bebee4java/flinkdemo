package os.dt.try1.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * flink流wordcount java实现
 * Created by songgr on 2021/04/15.
 */
public class JFlinkStreamWordCount {

    public static void main(String[] args) throws Exception {
        // 0.准备env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置执行模式
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        // 1.准备数据
        DataStream<String> lines = env.fromElements("hello flink", "flink streaming", "flink");
        // 2.切割
        DataStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        });
//        words.print();
        // 3.设置1
        DataStream<Tuple2<String, Integer>> wordAndOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        });

//        map.print();


        KeyedStream<Tuple2<String, Integer>, String> wordKeyBy = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.getField(0);
            }
        });

        DataStream<Tuple2<String, Integer>> result = wordKeyBy.sum(1);

        result.print();

        env.execute();


    }
}
