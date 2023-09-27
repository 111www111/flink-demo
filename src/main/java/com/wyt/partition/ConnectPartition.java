package com.wyt.partition;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @projectName: flink-demo
 * @package: com.wyt.partition
 * @className: ConnectPartition
 * @author: WangYiTone
 * @date: 2023/9/26 22:37
 */
public class ConnectPartition {

    /**
     * @description: 链接流
     * @author: W1T
     * @param: args
     * @return: void
     * @date: 2023/9/26
     **/
    public static void main(String[] args) throws Exception {
        final String host = "hadoop101";
        final int port = 7777;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //数字流
        DataStreamSource<Integer> integerSource = env.fromElements(1, 3, 5, 7, 9);
        //字符串流
        DataStreamSource<String> stringSource = env.fromElements("a", "b", "c");
        //我输啥是啥流
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream(host, port);

        /**
         * TODO 使用 connect 合流
         * 1、一次只能连接 2条流
         * 2、流的数据类型可以不一样
         * 3、 连接后可以调用 map、flatmap、process来处理，但是各处理各的
         */
        ConnectedStreams<Integer, String> connect = integerSource.connect(stringSource);
        SingleOutputStreamOperator<String> result = connect.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return "来源于数字流:" + value.toString();
            }

            @Override
            public String map2(String value) throws Exception {
                return "来源于字母流:" + value;
            }
        });
        ConnectedStreams<String, String> connect1 = result.connect(stringDataStreamSource);
        SingleOutputStreamOperator<String> map = connect1.map(new CoMapFunction<String, String, String>() {
            @Override
            public String map1(String value) throws Exception {
                return "来源于result链接流:" + value;
            }

            @Override
            public String map2(String value) throws Exception {
                return "来源于socket流:" + value;
            }
        });
        map.print();
        env.execute();
    }
}
