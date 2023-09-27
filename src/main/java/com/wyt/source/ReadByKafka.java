package com.wyt.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @projectName: flink-demo
 * @package: com.wyt.source
 * @className: ReadByKafka
 * @author: WangYiTone
 * @date: 2023/9/19 22:49
 */
public class ReadByKafka {
    /**
     * 从kafka读取数据,需要依赖
     * <dependency>
     * <groupId>org.apache.flink</groupId>
     * <artifactId>flink-connector-kafka_${scala.binary.version}</artifactId>
     * <version>${flink.version}</version>
     * </dependency>
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        final String kafkaServers = "hadoop101:9092,hadoop102:9092,hadoop103:9092";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                //指定服务
                .setBootstrapServers(kafkaServers)
                //分组ID
                .setGroupId("wyt")
                //队列
                .setTopics("kafkaTopics1")
                //序列化器
                .setValueOnlyDeserializer(new SimpleStringSchema())
                //偏移量
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();

        //创建从Kafka读取的算子
        env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "wyt-ReadByKafka"
        ).print();

        env.execute();
    }
}
