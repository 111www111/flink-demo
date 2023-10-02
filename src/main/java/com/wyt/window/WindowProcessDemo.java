package com.wyt.window;

import com.wyt.entity.WaterSensor;
import com.wyt.mapfunc.WaterSensorMapFunctionImpl;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @projectName: flink-demo
 * @package: com.wyt.window
 * @className: WindowProcessDemo
 * @author: WangYiTone
 * @date: 2023/9/30 15:58
 */
public class WindowProcessDemo {
    /**
     * @description: 全窗口模式
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

        KeyedStream<WaterSensor, String> mapStream = socketStream
                .map(new WaterSensorMapFunctionImpl())
                .keyBy(WaterSensor::getId);

        WindowedStream<WaterSensor, String, TimeWindow> window = mapStream
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        //全局统计 旧版写法
        //WindowFunction<IN, OUT, KEY, W extends Window> 输入 输出 key 窗口 类型 对应四个泛型
//        window.apply(
//                (WindowFunction<WaterSensor, String, String, TimeWindow>) (key, dataWindow, input, out) -> {
//                    //....balabala
//                }
//        );

        //新版写法
        window.process(
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
        ).print();

        env.execute();
    }
}
