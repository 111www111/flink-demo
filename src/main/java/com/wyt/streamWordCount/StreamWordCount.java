package com.wyt.streamWordCount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;


/**
 * @projectName: flink-demo
 * @package: com.wyt.streamWordCount
 * @className: StreamWordCount
 * @author: WangYiTone
 * @date: 2023/9/18 22:32
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        final String ip = "hadoop101";
        final int host = 7777;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> nc = env.socketTextStream(ip, host);

        SingleOutputStreamOperator<Tuple2<String, Long>> maps = nc.flatMap((String line, Collector<String> words) -> {
                    //给这一行的数据切成单个的放进去
                    Arrays.stream(line.split(" ")).forEach(words::collect);
                }).returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        KeyedStream<Tuple2<String, Long>, String> reduce = maps.keyBy(k -> k.f0);
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = reduce.sum(1);
        sum.print();
        env.execute();
    }
}
