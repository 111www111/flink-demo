package com.wyt.watermark;

import com.wyt.entity.WaterSensor;
import com.wyt.mapfunc.WaterSensorMapFunctionImpl;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.events.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @projectName: flink-demo
 * @package: com.wyt.watermark
 * @className: CustomWaterMarkDemo
 * @author: WangYiTone
 * @date: 2023/10/3 15:30
 */
public class CustomWaterMarkDemo {

    /**
     * @description: 自定义水位线
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
        //设置输出水位线的周期,一般不做更改 试下下看看就行了
        env.getConfig().setAutoWatermarkInterval(5000);


        env.socketTextStream(host, port)
                .map(new WaterSensorMapFunctionImpl())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                //自定义水位线
                                .forGenerator((ctx) -> new CustomWatermarkStrategy(5000))
                                .withTimestampAssigner((WaterSensor waterSensor, long recordTimestamp) -> waterSensor.getTs() * 1000L)
                )
                .keyBy(WaterSensor::getId)
                //使用事件事件窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
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
                })
                .print();

        env.execute();
    }


    public static class CustomWatermarkStrategy implements WatermarkGenerator<WaterSensor> {
        /**
         * 当前为止的最大事件时间
         */
        private long maxTs;
        /**
         * 接受的乱序时间
         */
        private final long delayTs;

        /**
         * @description: 构造函数
         * @author: W1T
         * @param: delayTs: 乱序时间
         * @return: CustomWatermarkStrategy
         * @date: 2023/10/3
         **/
        public CustomWatermarkStrategy(long delayTs) {
            this.delayTs = delayTs;
            this.maxTs = Long.MIN_VALUE + this.delayTs + 1;
        }

        /**
         * @description: 提取最大事件时间, 每个element都会触发
         * @author: W1T
         * @param: event
         * @Param eventTimestamp:
         * @Param output:
         * @return: void
         * @date: 2023/10/3
         **/
        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            this.maxTs = Math.max(event.getTs(), this.maxTs);
            System.out.println("调用onEvent,当前最大时间戳为:" + this.maxTs);
            //我们在数据更新最大的时候 更新水位线
            output.emitWatermark(new Watermark(this.maxTs - this.delayTs - 1));
            System.out.println("调用onPeriodicEmit,输出水位线" + (this.maxTs - this.delayTs - 1));
        }

        /**
         * @description: 输出水位线
         * @author: W1T
         * @param: output
         * @return: void
         * @date: 2023/10/3
         **/
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            //生成水位线,这个方法不管有没有数据都会调用,所以 我们在数据进入的时候处理就ok 了
//            output.emitWatermark(new Watermark(this.maxTs - this.delayTs - 1));
//            System.out.println("调用onPeriodicEmit,输出水位线" + (this.maxTs - this.delayTs - 1));
        }
    }
}
