package com.wolfhouse.influxclient.typehandler;

import com.wolfhouse.influxclient.utils.TimeStampUtils;

import java.time.Instant;

/**
 * 时间戳类型处理器
 *
 * @author Rylin Wolf
 */
public class InstantTypeHandler implements TypeHandler<Instant> {
    @Override
    public Instant getResult(Object result) {
        return TimeStampUtils.autoConvert(Long.parseLong(result.toString()));
    }
}
