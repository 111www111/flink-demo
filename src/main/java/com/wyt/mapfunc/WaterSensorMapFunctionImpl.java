package com.wyt.mapfunc;

import com.wyt.entity.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author WangYiTong
 * @ClassName WaterSensorFunctionImpl.java
 * @Description 根据entity中的实体 自定义map函数
 * @createTime 2023-09-27 14:30
 **/
public class WaterSensorMapFunctionImpl implements MapFunction<String, WaterSensor> {


    /**
     * @description: 简单写一下, 空什么的先不管
     * @author: WangYiTong
     * @param: value
     * @return: com.wyt.entity.WaterSensor
     * @date: 2023-9-27
     **/
    @Override
    public WaterSensor map(String value) throws Exception {
        String[] datas = value.split(",");
        //根据逗号分割后,new 一个 WaterSensor丢回去
        WaterSensor waterSensor = new WaterSensor();
        waterSensor.setId(datas[0]);
        waterSensor.setTs(Long.valueOf(datas[1]));
        waterSensor.setVc(Integer.valueOf(datas[2]));
        return waterSensor;
    }
}
