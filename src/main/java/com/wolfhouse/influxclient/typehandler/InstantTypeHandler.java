package com.wolfhouse.influxclient.typehandler;

import com.wolfhouse.influxclient.utils.TimeStampUtils;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * 时间戳类型处理器
 *
 * @author Rylin Wolf
 */
public class InstantTypeHandler implements TypeHandler<Instant> {
    @Override
    public Instant getResult(Object result) {
        if (result == null) {
            return null;
        }
        // InfluxDB Java SDK 的时间列已经统一为纳秒，无需再次推断精度
        return TimeStampUtils.toInstant(Long.parseLong(result.toString()), ChronoUnit.NANOS);
    }
}
