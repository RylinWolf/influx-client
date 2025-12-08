package com.wolfhouse.influxclient.test;

import com.wolfhouse.influxclient.pojo.AbstractActionInfluxObj;
import com.wolfhouse.influxclient.pojo.InfluxFields;
import com.wolfhouse.influxclient.pojo.InfluxTags;
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
public class SampleActionInfluxObj extends AbstractActionInfluxObj {
    {
        addFields(InfluxFields.from("temperature", null)
                              .add("o2", null)
                              .add("co2", null));

        addTags(InfluxTags.from("sensor_id", null)
                          .add("sensor_type", null));
    }

    @Override
    protected String tableName() {
        return "temperature";
    }
}
