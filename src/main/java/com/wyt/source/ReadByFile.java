package com.wyt.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @projectName: flink-demo
 * @package: com.wyt.source
 * @className: ReadByFile
 * @author: WangYiTone
 * @date: 2023/9/19 22:38
 */
public class ReadByFile {

    /**
     * 从文件读取,需要依赖
     * <!-- flink 文件读取依赖-->
     * <dependency>
     * <groupId>org.apache.flink</groupId>
     * <artifactId>flink-connector-files</artifactId>
     * <version>${flink.version}</version>
     * </dependency>
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        final String filePath = "E:\\code\\java\\flink\\flink-demo\\test\\demo1-word-count\\input.txt";
        //环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //创建算子
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(
                new TextLineFormat(),
                new Path(filePath)
        ).build();

        //填充算子
        env.fromSource(
                fileSource,
                WatermarkStrategy.noWatermarks(),
                "wyt-ReadByFile"
        ).print();

        env.execute();
    }
}
