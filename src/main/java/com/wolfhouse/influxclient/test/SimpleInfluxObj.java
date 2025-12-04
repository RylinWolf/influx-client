package com.wolfhouse.influxclient.test;

import com.wolfhouse.influxclient.core.AbstractInfluxObj;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * 添加对象实体类实例
 *
 * @author Rylin Wolf
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class SimpleInfluxObj extends AbstractInfluxObj {
    @Override
    protected String tableName() {
        return "temperature";
    }
}
