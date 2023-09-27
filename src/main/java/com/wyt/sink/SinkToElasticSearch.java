package com.wyt.sink;

import com.wyt.entity.WaterSensor;
import com.wyt.mapfunc.WaterSensorMapFunctionImpl;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.HashMap;
import java.util.Map;

/**
 * @author WangYiTong
 * @ClassName SinkToElasticSearch.java
 * @Description es算子推送
 * @createTime 2023-09-27 14:57
 **/
public class SinkToElasticSearch {

    /**
     * @description: 推送至ES
     * @author: WangYiTong
     * @param: args
     * @return: void
     * @date: 2023-9-27
     **/
    public static void main(String[] args) throws Exception {
        //服务器配置
        final String host = "hadoop101";
        final int port = 7777;
        //es配置
        final String esHost = "localhost";
        final int esPort = 9200;
        final String esType = "http";
        final String esIndex = "flink_demo";

        //先创建流监听
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        DataStreamSource<String> socketTextStream = env.socketTextStream(host, port);
        //将监听到的东西转成 实体
        SingleOutputStreamOperator<WaterSensor> waterSensorSource = socketTextStream.map(new WaterSensorMapFunctionImpl());

        //elasticSearch 算子
        ElasticsearchSink<WaterSensor> elasticsearchSink = new Elasticsearch7SinkBuilder<WaterSensor>()
                //设置批量操作的最大数量,这里写1,指示接收器在每个元素之后触发，否则被缓冲
                .setBulkFlushMaxActions(1)
                .setHosts(new HttpHost(esHost, esPort, esType))
                .setEmitter((element, context, indexer) -> indexer.add(createIndexRequest(element, esIndex)))
                .build();
        //添加算子执行
        waterSensorSource.sinkTo(elasticsearchSink);
        env.execute();
    }

    /**
     * @description: 创建请求
     * @author: WangYiTong
     * @param: element: 数据元素
     * @Param esIndex: es index name
     * @return: org.elasticsearch.action.index.IndexRequest
     * @date: 2023-9-27
     **/
    private static IndexRequest createIndexRequest(WaterSensor element, String esIndexName) {
        Map<String, Object> json = new HashMap<>();
        json.put("data", element);

        return Requests.indexRequest()
                .index(esIndexName)
                .id(element.getId())
                .source(json);
    }
}
