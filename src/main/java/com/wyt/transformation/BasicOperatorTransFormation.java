package com.wyt.transformation;

import com.wyt.source.WaterSensor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.Objects;

/**
 * @projectName: flink-demo
 * @package: com.wyt.transformation
 * @className: BasicOperatorTransFormation
 * @author: WangYiTone
 * @date: 2023/9/20 22:16
 */
public class BasicOperatorTransFormation {

    /**
     * @description: 基本转换算子, map, filter, flatMap参考Java8 stream流
     * @author: W1T
     * @param: args
     * @return: void
     * @date: 2023/9/20
     **/
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> source = env.fromElements(
                new WaterSensor("S1", 1L, 1),
                new WaterSensor("S2", 2L, 2),
                new WaterSensor("S3", 3L, 3),
                new WaterSensor("S4", 4L, 4),
                new WaterSensor("S5", 5L, 1)
        );
        //map
//        source.map(WaterSensor::getId).print();
        //filter
//        source.filter(data -> Objects.equals(data.getVc(),1)).print();
        //flatMap
        source.flatMap((WaterSensor obj, Collector<String> out) -> {
            if (Objects.equals(obj.getId(), "S1")) {
                out.collect("S1 VC:" + obj.getVc());
            } else if (Objects.equals(obj.getId(), "S2")) {
                out.collect("S2 VC:" + obj.getVc());
                out.collect("S2 TS:" + obj.getTs());
            }
        }).returns(Types.STRING)
                .print();

        env.execute();
    }
}
