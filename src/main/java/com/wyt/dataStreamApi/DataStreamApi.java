package com.wyt.dataStreamApi;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @projectName: flink-demo
 * @package: com.wyt.dataStreamApi
 * @className: DataStreamApi
 * @author: WangYiTone
 * @date: 2023/9/19 21:59
 */
public class DataStreamApi {
    public static void main(String[] args) throws Exception {
        final String ip = "hadoop101";
        final int host = 7777;
        //配置端口
        Configuration conf = new Configuration();
        conf.set(RestOptions.BIND_PORT, "8082");
        //自动识别是本地还是提交集群
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.socketTextStream(ip, host)
                .flatMap((String line, Collector<String> words) -> {
                    //给这一行的数据切成单个的放进去
                    Arrays.stream(line.split(" ")).forEach(words::collect);
                }).returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(k -> k.f0)
                .sum(1)
                .print();

        env.execute();

    }
}
