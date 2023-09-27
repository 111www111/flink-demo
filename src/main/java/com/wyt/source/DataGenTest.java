package com.wyt.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @projectName: flink-demo
 * @package: com.wyt.source
 * @className: DataGen
 * @author: WangYiTone
 * @date: 2023/9/20 20:37
 */
public class DataGenTest {
    /**
     * @description: 数据生成器, 需要依赖, 需要1.17版本
     * <dependency>
     * <groupId>org.apache.flink</groupId>
     * <artifactId>flink-connector-datagen</artifactId>
     * <version>${flink.version}</version>
     * </dependency>
     * @author: W1T
     * @param: args
     * @return: void
     * @date: 2023/9/20
     **/
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        //数据生成器
        DataGeneratorSource<String> dataGenSource = new DataGeneratorSource<>(
                //造数据
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {
                        return "Number:" + value;
                    }
                },
                //造多少
                Long.MAX_VALUE,
                //频率
                RateLimiterStrategy.perSecond(10),
                //返回类型
                Types.STRING
        );
        //用这个生成器数据源
        env.fromSource(dataGenSource, WatermarkStrategy.noWatermarks(), "data-generator")
                .print();
        env.execute();
    }
}
