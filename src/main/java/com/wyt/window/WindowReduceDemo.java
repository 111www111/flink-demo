package com.wyt.window;

import com.wyt.entity.WaterSensor;
import com.wyt.mapfunc.WaterSensorMapFunctionImpl;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @projectName: flink-demo
 * @package: com.wyt.window
 * @className: WindowApiDemo
 * @author: WangYiTone
 * @date: 2023/9/30 14:49
 */
public class WindowReduceDemo {

    /**
     * @description: 窗口函数
     * @author: W1T
     * @param: args
     * @return: void
     * @date: 2023/9/30
     **/
    public static void main(String[] args) throws Exception {
        //服务器配置
        final String host = "hadoop101";
        final int port = 7777;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketStream = env.socketTextStream(host, port);

        SingleOutputStreamOperator<WaterSensor> reduce = socketStream
                .map(new WaterSensorMapFunctionImpl())
                .keyBy(WaterSensor::getId)
                .countWindow(5)
                .reduce((value1, value2) ->
                        new WaterSensor(value1.getId(), value2.getTs(), value1.getVc() + value2.getVc())
                );
        reduce.print();

        env.execute();
    }
}
