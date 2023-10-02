package com.wyt.window;

import com.wyt.entity.WaterSensor;
import com.wyt.mapfunc.WaterSensorMapFunctionImpl;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @projectName: flink-demo
 * @package: com.wyt.window
 * @className: WindowAggDemo
 * @author: WangYiTone
 * @date: 2023/9/30 15:37
 */
public class WindowAggDemo {
    /**
     * @description: agg
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

        SingleOutputStreamOperator<String> reduce = socketStream
                .map(new WaterSensorMapFunctionImpl())
                .keyBy(WaterSensor::getId)
                .countWindow(5)
                .aggregate(
                        new AggregateFunction<WaterSensor, Long, String>() {

                            /**
                             * @description: 创建累加器 ,初始化
                             * @author: W1T
                             * @return: java.lang.Long
                             * @date: 2023/9/30
                             **/
                            @Override
                            public Long createAccumulator() {
                                System.out.println("创建累加器");
                                return 0L;
                            }

                            /**
                             * @description: 聚合逻辑
                             * @author: W1T
                             * @param: value: 数据
                             * @Param accumulator: 累加器
                             * @return: java.lang.Long
                             * @date: 2023/9/30
                             **/
                            @Override
                            public Long add(WaterSensor value, Long accumulator) {
                                System.out.println("累加,当前值" + accumulator + "新增数据" + value);
                                return accumulator += value.getVc();
                            }

                            /**
                             * @description: 获得结果
                             * @author: W1T
                             * @param: accumulator
                             * @return: java.lang.String
                             * @date: 2023/9/30
                             **/
                            @Override
                            public String getResult(Long accumulator) {
                                System.out.println("窗口触发");
                                return accumulator + "";
                            }

                            /**
                             * @description: 合并 一般不用
                             * @author: W1T
                             * @param: a
                             * @Param b:
                             * @return: java.lang.Long
                             * @date: 2023/9/30
                             **/
                            @Override
                            public Long merge(Long a, Long b) {
                                System.out.println("用这个消息判断merge有没有触发,只有会话窗口会用到");
                                return 0L;
                            }
                        }
                );
        reduce.print();

        env.execute();
    }
}
