package com.wyt.aggreagte;

import com.wyt.source.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @projectName: flink-demo
 * @package: com.wyt.aggreagte
 * @className: ReduceDEMO
 * @author: WangYiTone
 * @date: 2023/9/23 10:05
 */
public class ReduceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        env.setParallelism(1);

        DataStreamSource<WaterSensor> source = env.fromElements(
                new WaterSensor("S1", 1L, 1),
                new WaterSensor("S2", 2L, 2),
                new WaterSensor("S3", 3L, 3),
                new WaterSensor("S1", 4L, 4),
                new WaterSensor("S3", 5L, 1)
        );

        source
                .keyBy(WaterSensor::getId)
                .reduce((value1, value2) -> {
                    return new WaterSensor(value1.getId(), value2.getTs(), value1.getVc() + value2.getVc());
                })
                .print();


        env.execute();
    }
}
