package com.wyt.entity;

import java.util.Objects;

/**
 * @projectName: flink-demo
 * @package: com.wyt.source
 * @className: Event
 * @author: WangYiTone
 * @date: 2023/9/19 22:30
 */
public class WaterSensor {

    /**
     * 水位传感器类型
     */
    private String id;

    /**
     * 传感器时间戳
     */
    private Long ts;

    /**
     * 水位记录
     */
    private Integer vc;

    /**
     * 必须提供空参构造
     */
    public WaterSensor() {
    }

    public WaterSensor(String id, Long ts, Integer vc) {
        this.id = id;
        this.ts = ts;
        this.vc = vc;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Integer getVc() {
        return vc;
    }

    public void setVc(Integer vc) {
        this.vc = vc;
    }

    @Override
    public String toString() {
        return "WaterSensor{" +
                "id='" + id + '\'' +
                ", ts=" + ts +
                ", vc=" + vc +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WaterSensor that = (WaterSensor) o;
        return Objects.equals(id, that.id) && Objects.equals(ts, that.ts) && Objects.equals(vc, that.vc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, ts, vc);
    }
}
