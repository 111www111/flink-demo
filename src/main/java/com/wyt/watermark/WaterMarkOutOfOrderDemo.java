package com.wyt.watermark;

import com.wyt.entity.WaterSensor;
import com.wyt.mapfunc.WaterSensorMapFunctionImpl;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @projectName: flink-demo
 * @package: com.wyt.watermark
 * @className: WaterMarkOutOfOrderDemo
 * @author: WangYiTone
 * @date: 2023/10/3 15:26
 */
public class WaterMarkOutOfOrderDemo {

    /**
     * @description: 乱序水位线设置
     * @author: W1T
     * @param: args
     * @return: void
     * @date: 2023/10/3
     **/
    public static void main(String[] args) throws Exception {
        //服务器配置
        final String host = "hadoop101";
        final int port = 7777;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> socketStream = env.socketTextStream(host, port);

        //侧输出流
        OutputTag<WaterSensor> lateStream = new OutputTag<>("lateStream", Types.POJO(WaterSensor.class));

        SingleOutputStreamOperator<String> process = socketStream
                .map(new WaterSensorMapFunctionImpl())
                .assignTimestampsAndWatermarks(
                        //设置时间分配器,以数据的事件时间为准处理
                        WatermarkStrategy
                                //最大乱序三秒
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        (WaterSensor waterSensor, long recordTimestamp) -> {
                                            System.out.println("数据:" + waterSensor + ",recordTimestamp:(" + recordTimestamp + ")");
                                            return waterSensor.getTs() * 1000L;
                                        }
                                )
                                .withIdleness(Duration.ofSeconds(3))
                )
                .keyBy(WaterSensor::getId)
                //使用事件事件窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                //推迟两秒关窗
                .allowedLateness(Time.seconds(2))
                //迟到数据 侧输出流
                .sideOutputLateData(lateStream)
                .process(
                        new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                            /**
                             * @description:
                             * @author: W1T
                             * @param: key: key
                             * @Param context: 上下文
                             * @Param elements: 存储的元素
                             * @Param out:  输出
                             * @return: void
                             * @date: 2023/9/30
                             **/
                            @Override
                            public void process(String key,
                                                ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context,
                                                Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                                //新版的上下文可以获得很多东西
                                //获得窗口,及其相关信息
                                TimeWindow windowType = context.window();
                                String startTime = DateFormatUtils.format(windowType.getStart(), "yyyy-MM-dd HH:mm:ss.SSS");
                                String endTime = DateFormatUtils.format(windowType.getEnd(), "yyyy-MM-dd HH:mm:ss.SSS");
                                //获得元素信息
                                //元素数量等等
                                long elementCount = elements.spliterator().estimateSize();

                                out.collect(String.format("key : %s , 窗口开始时间:%s - %s , 包含数据:%s",
                                        key, startTime, endTime, elementCount));
                            }
                        }
                );

        //主流打印
        process.print();
        //测流
        process.getSideOutput(lateStream).printToErr("关窗");

        env.execute();
    }

}
