package com.wolfhouse.influxclient;

/**
 * @author Rylin Wolf
 */
public interface InfluxClientConstant {
    /** 时间字段 */
    String   TIMESTAMP_FIELD   = "time";
    /** 最近查询结果中，标识时间的字段 */
    String   RECENT_TIME_FIELD = "recent_time";
    String[] BUILD_IN_FIELDS   = {TIMESTAMP_FIELD};
}
