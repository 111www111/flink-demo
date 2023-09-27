package com.wyt.sink;


import com.wyt.entity.WaterSensor;
import com.wyt.mapfunc.WaterSensorMapFunctionImpl;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author WangYiTong
 * @ClassName SinkToMysql.java
 * @Description mysql输出算子
 * @createTime 2023-09-27 14:20
 **/
public class SinkToMysql {

    /**
     * @description: flink输出到mysql 算子
     * 表结构:
     * <div>
     *     CREATE TABLE `ws` (
     *   `id` varchar(100) NOT NULL,
     *   `ts` bigint(20) DEFAULT NULL,
     *   `vc` int(11) DEFAULT NULL,
     *   PRIMARY KEY (`id`)
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8
     * </div>
     * model 为 entity.WaterSensor
     * @author: WangYiTong
     * @param: args
     * @return: void
     * @date: 2023-9-27
     **/
    public static void main(String[] args) throws Exception {
        //服务器配置
        final String host = "hadoop101";
        final int port = 7777;
        //数据库配置
        final String jdbcUrl = "jdbc:mysql://localhost:3306/flink_demo?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8";
        final String username = "root";
        final String pwd = "123456";

        //先创建流监听
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //监听
        DataStreamSource<String> socketTextStream = env.socketTextStream(host, port);
        //将监听到的东西转成 实体
        SingleOutputStreamOperator<WaterSensor> waterSensorSource = socketTextStream.map(new WaterSensorMapFunctionImpl());

        /**
         * 写入mysql,我觉得这个文档在折磨我,如果 socket 一直来消息,那不是一直连接JDBC? 哪个数据库能一直让这么玩
         * 1、只能用老的sink写法： addsink
         * 2、JDBCSink的4个参数:
         *    第一个参数： 执行的sql，一般就是 insert into
         *    第二个参数： 预编译sql， 对占位符填充值
         *    第三个参数： 执行选项 ---》 攒批、重试
         *    第四个参数： 连接选项 ---》 url、用户名、密码
         */
        SinkFunction<WaterSensor> mysqlSink = JdbcSink.sink(
                "insert into ws values(?,?,?)",
                (PreparedStatement preparedStatement, WaterSensor waterSensor) -> {
                    preparedStatement.setString(1, waterSensor.getId());
                    preparedStatement.setLong(2, waterSensor.getTs());
                    preparedStatement.setInt(3, waterSensor.getVc());
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3) // 重试次数
                        .withBatchSize(100) // 批次的大小：条数
                        .withBatchIntervalMs(3000) // 批次的时间
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(jdbcUrl)
                        .withUsername(username)
                        .withPassword(pwd)
                        .withConnectionCheckTimeoutSeconds(60) // 重试的超时时间
                        .build()
        );
        waterSensorSource.addSink(mysqlSink);
        //执行
        env.execute();
    }
}
