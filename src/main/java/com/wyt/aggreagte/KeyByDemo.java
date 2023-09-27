package com.wyt.aggreagte;

import com.wyt.entity.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @projectName: flink-demo
 * @package: com.wyt.aggreagte
 * @className: KeyByDemo
 * @author: WangYiTone
 * @date: 2023/9/20 23:12
 */
public class KeyByDemo {

    /**
     * @description: keyBy
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
                new WaterSensor("S1", 4L, 4),
                new WaterSensor("S3", 5L, 1)
        );

        source.keyBy(WaterSensor::getId).print();


        env.execute();
    }
}
