package com.wyt.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @projectName: flink-demo
 * @package: com.wyt.source
 * @className: ReadByCollection
 * @author: WangYiTone
 * @date: 2023/9/19 22:35
 */
public class ReadByCollection {
    /**
     * 从集合读取
     * @param args
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从集合读取数据
//        DataStreamSource<Integer> source = env.fromCollection(Arrays.asList(11, 22, 33));
        DataStreamSource<Integer> source = env.fromElements(11, 22, 33);

        source.print();

        env.execute();
    }
}
