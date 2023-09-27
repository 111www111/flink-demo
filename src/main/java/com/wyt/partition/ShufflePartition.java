package com.wyt.partition;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @projectName: flink-demo
 * @package: com.wyt.partition
 * @className: ShufflePartition
 * @author: WangYiTone
 * @date: 2023/9/23 21:20
 */
public class ShufflePartition {

    /**
     * @description: 随机分区 shuffle
     * @author: W1T
     * @param: args
     * @return: void
     * @date: 2023/9/23
     **/
    public static void main(String[] args) throws Exception {
        final String hosts = "hadoop101";
        final int port = 7777;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(10);

        DataStreamSource<String> socketSource = env.socketTextStream(hosts, port);
        //随机
        //socketSource.shuffle().print();

        //rebalance 轮询 : nextChannelToSendTo = (nextChannelToSendTo + 1) % numberOfChannels;
//        socketSource.rebalance().print();

        //rescale 缩放 : 分组轮询,效率更高
        //socketSource.rescale().print();

        //broadcast 广播 :全都给
        //socketSource.broadcast().print();

        //global 全局 : 默认全部给0号,相当于吧下游的并行度强制设置为1
        socketSource.global().print();

        env.execute();
    }
}
