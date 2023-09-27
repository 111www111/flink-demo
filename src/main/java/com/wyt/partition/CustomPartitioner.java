package com.wyt.partition;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.partitioner.CustomPartitionerWrapper;

/**
 * @projectName: flink-demo
 * @package: com.wyt.partition
 * @className: CustomPartitioner
 * @author: WangYiTone
 * @date: 2023/9/23 21:51
 */
public class CustomPartitioner {

    public static void main(String[] args) throws Exception {
        final String hosts = "hadoop101";
        final int port = 7777;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> socketSource = env.socketTextStream(hosts, port);

        //自定义分区
        socketSource.partitionCustom(new TestCustomPartitioner(), r -> r).print();

        env.execute();
    }


    /**
     * @description: 自定义分区
     * @author: W1T
     * @date: 2023/9/23
     **/
    static class TestCustomPartitioner implements Partitioner<String> {

        /**
         * @description: 取模分区
         * @author: W1T
         * @return: int
         * @date: 2023/9/23
         **/
        @Override
        public int partition(String s, int i) {
            return Integer.parseInt(s) % i;
        }
    }
}
