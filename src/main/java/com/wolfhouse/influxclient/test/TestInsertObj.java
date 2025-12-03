package com.wolfhouse.influxclient.test;

import com.wolfhouse.influxclient.core.AbstractInsertObj;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author Rylin Wolf
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class TestInsertObj extends AbstractInsertObj {
    public TestInsertObj() {
        super("temperature");
    }

    @Override
    public String getMeasurement() {
        return measurement;
    }
}
