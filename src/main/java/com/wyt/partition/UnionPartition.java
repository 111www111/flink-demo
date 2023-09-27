package com.wyt.partition;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @projectName: flink-demo
 * @package: com.wyt.partition
 * @className: UnionPartition
 * @author: WangYiTone
 * @date: 2023/9/26 22:22
 */
public class UnionPartition {

    /**
     * @description: 合流
     * @author: W1T
     * @param: args
     * @return: void
     * @date: 2023/9/26
     **/
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> oddSource = env.fromElements(1, 3, 5, 7, 9);
        DataStreamSource<Integer> evenSource = env.fromElements(2, 4, 6, 8, 10);
        DataStreamSource<String> stringSource = env.fromElements("100", "200", "300");
        //合流
        oddSource.union(evenSource, stringSource.map(Integer::valueOf)).print();
        env.execute();
    }
}
