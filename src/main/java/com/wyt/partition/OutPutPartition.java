package com.wyt.partition;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @projectName: flink-demo
 * @package: com.wyt.partition
 * @className: OutPustPartition
 * @author: WangYiTone
 * @date: 2023/9/25 14:57
 */
public class OutPutPartition {

    /**
     * @description: 分流
     * @author: W1T
     * @param: args
     * @return: void
     * @date: 2023/9/25
     **/
    public static void main(String[] args) throws Exception {
        final String host = "hadoop101";
        final int port = 7777;

        final String odd = "奇数";
        final String even = "偶数";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> socketSource = env.socketTextStream(host, port);

        OutputTag<String> oddOutPut = new OutputTag<>(odd, Types.STRING);
        OutputTag<String> evenOutPut = new OutputTag<>(even, Types.STRING);

        //分流策略
        SingleOutputStreamOperator<String> process = socketSource.process(
                new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) {
                        try {
                            int valueInt = Integer.parseInt(value);
                            ctx.output(valueInt / 2 == 0 ? oddOutPut : evenOutPut, value);
                        } catch (Exception e) {
                            out.collect(value);
                        }
                    }
                }
        );
        process.print("主流，非odd,even的传感器");
        //获取流
        process.getSideOutput(oddOutPut).printToErr(odd);
        process.getSideOutput(evenOutPut).printToErr(even);

        env.execute();
    }
}
