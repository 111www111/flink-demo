package com.wyt.datastream;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @projectName: flink-demo
 * @package: com.wyt.datastream
 * @className: BoundedStreamWordCount
 * @author: WangYiTone
 * @date: 2023/9/18 22:20
 */
public class BoundedStreamWordCount {
    /**
     * 流处理
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        final String inputPath = "E:\\code\\java\\flink\\flink-demo\\test\\demo1-word-count\\input.txt";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.readTextFile(inputPath);
        SingleOutputStreamOperator<Tuple2<String, Long>> returns = lines.flatMap((String line, Collector<String> words) -> {
                    Arrays.stream(line.split(" ")).forEach(words::collect);
                }).returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        //分组求和
        KeyedStream<Tuple2<String, Long>, String> groupBy = returns.keyBy(t -> t.f0);
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = groupBy.sum(1);
        sum.print();
        env.execute();
    }
}
