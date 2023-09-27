package com.wyt.wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @projectName: flink-demo
 * @package: com.wyt.wordcount
 * @className: BatchWordCount
 * @author: WangYiTone
 * @date: 2023/9/18 22:10
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        final String inputPath = "E:\\code\\java\\flink\\flink-demo\\test\\demo1-word-count\\input.txt";
        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //读取为文件 line
        DataSource<String> lineDs = env.readTextFile(inputPath);
        //转换数据格式
        FlatMapOperator<String, Tuple2<String, Long>> map = lineDs
                .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] lineWords = line.split(" ");
                    for (String word : lineWords) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        //reduce
        UnsortedGrouping<Tuple2<String, Long>> reduce = map.groupBy(0);
        AggregateOperator<Tuple2<String, Long>> sum = reduce.sum(1);
        sum.print();
    }
}
