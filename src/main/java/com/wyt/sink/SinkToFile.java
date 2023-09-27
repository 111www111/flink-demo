package com.wyt.sink;

import com.wyt.source.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @projectName: flink-demo
 * @package: com.wyt.sink
 * @className: SinkToFile
 * @author: WangYiTone
 * @date: 2023/9/24 20:47
 */
public class SinkToFile {

    /**
     * @description: 算子输出到文件
     * @author: W1T
     * @param: args
     * @return: void
     * @date: 2023/9/24
     **/
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> source = env.fromElements(
                new WaterSensor("S1", 1L, 1),
                new WaterSensor("S2", 2L, 2),
                new WaterSensor("S3", 3L, 3),
                new WaterSensor("S1", 4L, 4),
                new WaterSensor("S3", 5L, 1)
        );



    }
}
