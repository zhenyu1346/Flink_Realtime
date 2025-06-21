import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建模拟数据源
        DataStream<String> sourceStream = env.fromElements("hello world", "hello flink", "hello hadoop",
                "hello flink", "hello flink", "hello flink", "hello flink");

//        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = sourceStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//
//            @Override
//            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
//                String[] words = line.split(" ");
//                for (String word : words) {
//                    out.collect(new Tuple2<>(word, 1));
//                }
//            }
//        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        //数据转换,根据空格将内容拆分为(单词,1)
        DataStream<Tuple2<String,Integer>> flatMap = sourceStream.flatMap((String line,Collector<Tuple2<String,Integer>> out) -> {
            String[] words = line.split(" ");
            for(String word : words){
                out.collect(new Tuple2<>(word, 1));
            }

        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        //将flatMap按照键分组
        KeyedStream<Tuple2<String,Integer>,String> keyBy = flatMap.keyBy(value -> value.f0);

        // 对单词聚合计数
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyBy.sum(1);

        result.print();

        env.execute("WordCount");

    }
}
