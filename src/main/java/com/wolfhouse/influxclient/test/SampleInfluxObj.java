package com.wolfhouse.influxclient.test;

import com.wolfhouse.influxclient.pojo.AbstractBaseInfluxObj;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 添加对象实体类实例
 *
 * @author Rylin Wolf
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class SampleInfluxObj extends AbstractBaseInfluxObj {
    private String sensorId;
    private String sensorType;
    private Double o2;
    private Double co2;
    private Double temperature;

}
