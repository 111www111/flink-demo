package com.wyt.sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;

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
     * Flink专门提供了一个流式文件系统的连接器：FileSink，为批处理和流处理提供了一个统一的Sink，它可以将分区文件写入Flink支持的文件系统。
     * FileSink支持行编码（Row-encoded）和批量编码（Bulk-encoded）格式。这两种不同的方式都有各自的构建器（builder），可以直接调用FileSink的静态方法：
     * 行编码： FileSink.forRowFormat（basePath，rowEncoder）。
     * 批量编码： FileSink.forBulkFormat（basePath，bulkWriterFactory）。
     * @author: W1T
     * @param: args
     * @return: void
     * @date: 2023/9/24
     **/
    public static void main(String[] args) throws Exception {
        final String host = "hadoop101";
        final int port = 7777;
        final String outPutPath = "E:\\zhuome\\flink_demo\\flink-demo\\test\\sink_output_path";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每个目录中，都有 并行度个数的 文件在写入
        env.setParallelism(1);
        // 必须开启checkpoint，否则一直都是 .inprogress
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        //监听
        DataStreamSource<String> socketTextStream = env.socketTextStream(host, port);

        //输出到文件
        FileSink<String> fileSink = FileSink.<String>forRowFormat(new Path(outPutPath), new SimpleStringEncoder<>("UTF-8"))
                .withOutputFileConfig(
                        //配置文件前后缀
                        OutputFileConfig.builder()
                                .withPartPrefix("w1t-")
                                .withPartSuffix(".log")
                                .build()
                )
                .withBucketAssigner(
                        //按照格式分桶,此处为 每个小时一个目录
                        new DateTimeBucketAssigner<>("yyyy-MM-dd HH",
                                ZoneId.systemDefault())
                )
                .withRollingPolicy(
                        //文件滚动策略,要么一分钟 要么满足大小
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(1))
                                .withMaxPartSize(new MemorySize(1024 * 1024))
                                .build()
                ).build();

        socketTextStream.sinkTo(fileSink);
        env.execute();
    }
}
