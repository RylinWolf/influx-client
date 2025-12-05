package com.wolfhouse.influxclient.typehandler;

import com.wolfhouse.influxclient.utils.TimeStampUtils;

import java.time.Instant;

/**
 * @author Rylin Wolf
 */
public class InstantTypeHandler implements TypeHandler<Instant> {
    @Override
    public Instant getResult(Object result) {
        return TimeStampUtils.autoConvert(Long.parseLong(result.toString()));
    }
}
